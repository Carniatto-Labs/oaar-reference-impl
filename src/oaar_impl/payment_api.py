import json
import time
import urllib.error
import urllib.request

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind

from oaar_impl.otel_setup import configure_telemetry

configure_telemetry()

app = FastAPI(title="oaar-payment-api")

PRODUCT_API = "http://product-api:8082"
_gateway = {
    "latency_s": 0.15,
    "decline_codes": {"insufficient_limit": "51", "card_blocked": "57", "gateway_declined": "05", "out_of_stock": "29"},
}
BRAND_LIMITS = {"visa": 1500.0, "mastercard": 800.0, "amex": 2000.0, "elo": 500.0, "unknown": 300.0}

# Server-side idempotency store, keyed by the caller's idempotency key. A retried attempt
# returns the original decision instead of charging (or reserving inventory) a second time.
# Production deploys this as a unique index on the acquirer side; the semantic point is that
# the runtime-owned key — not the transport — is what makes the retry safe.
_IDEMPOTENT: dict[str, dict] = {}


def _remember(key: str, status_code: int, body: dict) -> None:
    if key:
        _IDEMPOTENT[key] = {"status_code": status_code, "body": body}


def _replay(entry: dict) -> JSONResponse:
    body = {**entry["body"], "deduplicated": True}
    if entry["status_code"] >= 400:
        # preserve FastAPI's error envelope: callers parse HTTPException.detail
        body = {"detail": body}
    return JSONResponse(status_code=entry["status_code"], content=body)


class _InventoryError(RuntimeError):
    def __init__(self, reason: str, stage: str, status_code: int):
        super().__init__(f"inventory: {reason} at {stage}")
        self.reason = reason
        self.stage = stage
        self.status_code = status_code


def _product_call(path: str, payload: dict) -> dict:
    headers = {"Content-Type": "application/json"}
    inject(headers)
    request = urllib.request.Request(
        f"{PRODUCT_API}{path}", data=json.dumps(payload).encode(), headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        detail = json.loads(exc.read()).get("detail", {})
        if isinstance(detail, dict):
            raise _InventoryError(
                detail.get("reason", "not_found"),
                detail.get("stage", "product_catalog"),
                exc.code,
            ) from None
        raise _InventoryError("not_found", "product_catalog", exc.code) from None


@app.get("/health")
def health():
    return {"status": "ok", "service": "payment-api", "gateway": "simulated-acquirer"}


@app.post("/payments")
async def create_payment(request: Request):
    body = await request.json()
    context = extract(dict(request.headers))
    order_id = body.get("order_id", "")
    amount = float(body.get("amount", 0.0))
    method = body.get("method", "")
    card_last4 = body.get("card_last4", "")
    card_brand = body.get("card_brand", "unknown")
    product_id = body.get("product_id")
    quantity = int(body.get("quantity", 1))
    idempotency_key = str(body.get("idempotency_key") or "")
    with trace.get_tracer("payment-api").start_as_current_span(
        "payment.process", context=context, kind=SpanKind.SERVER,
        attributes={
            "purchase.order.id": order_id,
            "purchase.amount": amount,
            "purchase.payment.method": method,
            "purchase.payment.last4": card_last4,
            "purchase.payment.brand": card_brand,
        },
    ) as span:
        if idempotency_key:
            span.set_attribute("payment.idempotency_key", idempotency_key)
        if idempotency_key and idempotency_key in _IDEMPOTENT:
            span.set_attribute("payment.deduplicated", True)
            return _replay(_IDEMPOTENT[idempotency_key])

        time.sleep(_gateway["latency_s"])

        reserved = False
        if product_id:
            try:
                _product_call(f"/products/{product_id}/reserve", {"quantity": quantity})
                reserved = True
                span.set_attribute("product.reserved", True)
            except _InventoryError as exc:
                span.set_attribute("payment.status", "declined")
                span.set_attribute("payment.failure.reason", exc.reason)
                span.set_attribute("payment.failure.stage", exc.stage)
                detail = {
                    "status": "declined",
                    "reason": exc.reason,
                    "stage": exc.stage,
                    "gateway_code": _gateway["decline_codes"].get(exc.reason, "XX"),
                }
                _remember(idempotency_key, 409, detail)
                raise HTTPException(status_code=409, detail=detail) from None

        def _auth_decline(reason: str):
            if reserved:
                _product_call(f"/products/{product_id}/release", {"quantity": quantity})
            span.set_attribute("payment.status", "declined")
            span.set_attribute("payment.failure.reason", reason)
            span.set_attribute("payment.failure.stage", "acquirer_authorization")
            span.set_attribute("payment.gateway.code", _gateway["decline_codes"].get(reason, "XX"))
            detail = {
                "status": "declined",
                "reason": reason,
                "stage": "acquirer_authorization",
                "gateway_code": _gateway["decline_codes"].get(reason, "XX"),
            }
            _remember(idempotency_key, 402, detail)
            raise HTTPException(status_code=402, detail=detail)

        if card_last4 == "0000":
            _auth_decline("card_blocked")
        limit = BRAND_LIMITS.get(card_brand, BRAND_LIMITS["unknown"])
        span.set_attribute("purchase.payment.limit", limit)
        if amount > limit:
            _auth_decline("insufficient_limit")

        payment_id = f"pay-{int(time.time() * 1000)}"
        span.set_attribute("payment.status", "approved")
        span.set_attribute("payment.id", payment_id)
        payload = {
            "status": "approved",
            "payment_id": payment_id,
            "auth_code": f"auth-{int(time.time() * 1000) % 100000}",
            "stage": "captured",
        }
        _remember(idempotency_key, 200, payload)
        return payload
