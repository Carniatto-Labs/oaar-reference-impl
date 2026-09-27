import time

from fastapi import FastAPI, HTTPException, Request
from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from oaar_impl.otel_setup import configure_telemetry

configure_telemetry()

app = FastAPI(title="oaar-payment-api")

_gateway = {"latency_s": 0.15, "decline_codes": {"gateway_declined": "05"}}


@app.get("/health")
def health():
    return {"status": "ok", "service": "payment-api", "gateway": "simulated-acquirer"}


@app.post("/payments")
async def create_payment(request: Request):
    body = await request.json()
    context = extract(dict(request.headers))
    with trace.get_tracer("payment-api").start_as_current_span(
        "payment.process", context=context, kind=SpanKind.SERVER,
        attributes={
            "purchase.order.id": body.get("order_id", ""),
            "purchase.amount": body.get("amount", 0.0),
            "purchase.payment.method": body.get("method", ""),
            "purchase.payment.last4": body.get("card_last4", ""),
        },
    ) as span:
        time.sleep(_gateway["latency_s"])
        if body.get("force") == "declined":
            reason = body.get("decline_reason", "gateway_declined")
            stage = "acquirer_authorization"
            span.set_attribute("payment.status", "declined")
            span.set_attribute("payment.failure.reason", reason)
            span.set_attribute("payment.failure.stage", stage)
            span.set_attribute("payment.gateway.code", _gateway["decline_codes"].get(reason, "XX"))
            raise HTTPException(
                status_code=402,
                detail={
                    "status": "declined",
                    "reason": reason,
                    "stage": stage,
                    "gateway_code": _gateway["decline_codes"].get(reason, "XX"),
                },
            )
        payment_id = f"pay-{int(time.time() * 1000)}"
        span.set_attribute("payment.status", "approved")
        span.set_attribute("payment.id", payment_id)
        return {
            "status": "approved",
            "payment_id": payment_id,
            "auth_code": f"auth-{int(time.time() * 1000) % 100000}",
            "stage": "captured",
        }
