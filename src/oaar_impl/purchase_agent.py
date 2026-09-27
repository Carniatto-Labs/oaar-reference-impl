import logging
import os
import re
import time
from collections.abc import Iterator

from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.checkpointer_otel import OtelCheckpointer
from oaar_impl.outcomes import emit_outcome
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import decision_span, effect_span, log_with_context, new_id, tool_span

WORKFLOW_NAME = "agent.purchase"
WORKFLOW_VERSION = "0.1.0"
PRODUCT_API = "http://product-api:8082"
PAYMENT_API = "http://payment-api:8083"

BRANDS = {
    "visa": re.compile(r"^4"),
    "mastercard": re.compile(r"^(5[1-5]|2[2-7])"),
    "amex": re.compile(r"^3[47]"),
    "elo": re.compile(r"^(636368|438935|504175|451416|636297|5067|4576|4011)"),
}

logger = logging.getLogger("oaar.agent")


class PaymentDeclined(RuntimeError):
    def __init__(self, reason: str, stage: str, gateway_code: str):
        super().__init__(f"payment declined: {reason} at {stage}")
        self.reason = reason
        self.stage = stage
        self.gateway_code = gateway_code


def detect_brand(pan: str) -> str:
    for brand, pattern in BRANDS.items():
        if pattern.match(pan):
            return brand
    return "unknown"


def _digits(text: str) -> str:
    candidates = re.findall(r"\d[\d\s]{11,22}\d", text)
    if not candidates:
        return ""
    return re.sub(r"\s", "", candidates[-1])


WORD_NUMBERS = {"one": 1, "a": 1, "um": 1, "uma": 1, "two": 2, "dois": 2, "duas": 2, "three": 3, "tres": 3, "quatro": 4, "four": 4, "five": 5, "cinco": 5}


def parse_intent(prompt: str) -> dict:
    pan = _digits(prompt)
    cleaned = re.sub(r"\d[\d\s.\-]{11,22}\d", " ", prompt)
    quantity = 1
    match = re.search(r"(\d+)\s*(?:x\s+|unidades?\s+(?:de\s+)?|units?\s+of\s+)", cleaned, re.IGNORECASE)
    if match:
        quantity = min(int(match.group(1)), 10)
        cleaned = cleaned[: match.start()] + " " + cleaned[match.end():]
    else:
        for word, value in WORD_NUMBERS.items():
            match = re.search(rf"\b{word}\b\s+(?:x\s+)?", cleaned, re.IGNORECASE)
            if match and re.search(rf"\b{word}\b", cleaned, re.IGNORECASE):
                quantity = value
                cleaned = cleaned[: match.start()] + " " + cleaned[match.end():]
                break
    cleaned = re.sub(
        r"\b(?:com|using|with|no|na|pelo|pela|cart[aã]o|card|credit|credito|cr[eé]dito|visa|mastercard|master|amex|elo)\b",
        " ", cleaned, flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(?:please\s+)?(?:i\s+(?:want|would\s+like)\s+(?:to\s+)?(?:buy|purchase)|can\s+you\s+buy|buy|purchase|get|order|quero|queria|gostaria\s+de\s+comprar|comprar|compre|compra|pe[çc]o|me\s+traz|adiciona)\s+",
        " ", cleaned.strip(), flags=re.IGNORECASE,
    )
    query = re.sub(r"\s+", " ", cleaned).strip(" ,.!?:;-\n")
    if not query:
        query = prompt.strip()
    return {
        "query": query,
        "quantity": quantity,
        "pan": pan,
        "last4": pan[-4:] if pan else "",
        "brand": detect_brand(pan) if pan else "",
    }


def _http_json(method: str, url: str, payload: dict | None = None) -> dict:
    import json as _json
    import urllib.error
    import urllib.request

    headers = {}
    inject(headers)
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = _json.dumps(payload).encode()
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            return _json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        detail = _json.loads(exc.read()).get("detail", {})
        if isinstance(detail, dict):
            raise PaymentDeclined(
                detail.get("reason", "unknown"),
                detail.get("stage", "unknown"),
                detail.get("gateway_code", "XX"),
            ) from None
        raise PaymentDeclined("unknown", "unknown", "XX") from None


def _psycopg_connect(dsn: str):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row)


def run_purchase_agent(prompt: str, thread_id: str) -> Iterator[dict]:
    """Interactive purchase agent: parses a free-form prompt, then runs the
    observe-act-iterate loop (search -> lookup -> charge) with full OAAR
    semantics. Yields UI events; the trace carries every decision, tool call
    and effect exactly as the RFC requires."""
    intent = parse_intent(prompt)
    order_id = f"order-{int(time.time() * 1000)}"
    dsn = os.environ.get("OAAR_POSTGRES_DSN", "postgresql://oaar:oaar@postgres:5432/oaar")
    conn = _psycopg_connect(dsn)
    saver = OtelCheckpointer(conn)
    saver.setup()
    outcome = "pending"

    def finish(value: str):
        emit_outcome(f"purchase:{order_id}", "payment.authorization", "1.0.0", value)
        m.record_purchase(value)

    try:
        with correlation.operation(f"purchase:{order_id}"):
            with durable_run(thread_id, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger) as run:
                trace_id = format(run.span_context.trace_id, "032x")
                yield {"type": "run", "run_id": run.run_id, "trace_id": trace_id, "order_id": order_id}

                yield {
                    "type": "decision", "kind": "plan", "iteration": 0,
                    "basis": f"intent: query='{intent['query']}' quantity={intent['quantity']} "
                             f"card={intent['brand'] or 'none'}****{intent['last4'] or '????'}",
                    "outcome": "parsed" if intent["pan"] else "missing_card",
                }
                m.record_decision("plan")

                if not intent["pan"]:
                    outcome = "declined"
                    run.logical_outcome = "declined"
                    summary = "I could not find a card number in your message. Please include the full card digits so I can charge it."
                    yield {"type": "final", "outcome": "declined", "reason": "missing_card", "stage": "intent_parsing", "order_id": order_id, "summary": summary}
                    log_with_context(logger, logging.INFO, f"purchase {order_id} declined: reason=missing_card stage=intent_parsing", run_id=run.run_id, thread_id=thread_id, category="purchase")
                    finish("declined")
                    return

                with tool_span(new_id("call"), "product.search", "read_only") as span:
                    span.set_attribute("purchase.product.query", intent["query"])
                    data = _http_json("GET", f"{PRODUCT_API}/products/search?q={intent['query'].replace(' ', '%20')}")
                    results = data.get("results", [])
                    span.set_attribute("purchase.product.results", len(results))
                    m.record_tool_call("product.search", "read_only", "not_attempted")
                yield {"type": "tool", "name": "product.search", "status": "ok",
                       "detail": f"{len(results)} candidate(s) for '{intent['query']}'"}

                if not results:
                    outcome = "declined"
                    run.logical_outcome = "declined"
                    summary = f"No product matching '{intent['query']}' exists in the catalog. Nothing was charged."
                    yield {"type": "final", "outcome": "declined", "reason": "not_found", "stage": "product_catalog", "order_id": order_id, "summary": summary}
                    log_with_context(logger, logging.INFO, f"purchase {order_id} declined: reason=not_found stage=product_catalog query={intent['query']}", run_id=run.run_id, thread_id=thread_id, category="purchase")
                    finish("declined")
                    return

                with tool_span(new_id("call"), "product.lookup", "read_only") as span:
                    chosen = results[0]
                    span.set_attribute("purchase.product.id", chosen["id"])
                    span.set_attribute("purchase.product.name", chosen["name"])
                    span.set_attribute("purchase.product.price", chosen["price"])
                    detail = _http_json("GET", f"{PRODUCT_API}/products/{chosen['id']}")["product"]
                    span.set_attribute("product.stock", detail["stock"])
                    m.record_tool_call("product.lookup", "read_only", "not_attempted")
                amount = round(detail["price"] * intent["quantity"], 2)
                yield {"type": "tool", "name": "product.lookup", "status": "ok",
                       "detail": f"{intent['quantity']}x {detail['name']} — ${detail['price']:.2f} each, total ${amount:.2f} (stock {detail['stock']})"}

                yield {
                    "type": "decision", "kind": "select_tool", "iteration": 1,
                    "basis": f"product:{detail['id']} price:{amount} method:credit_card brand:{intent['brand']} last4:{intent['last4']}",
                    "outcome": "charge",
                }
                m.record_decision("select_tool")

                with tool_span(new_id("call"), "payment.charge", "mutable_write") as span:
                    span.set_attribute("purchase.order.id", order_id)
                    span.set_attribute("purchase.amount", amount)
                    span.set_attribute("purchase.payment.method", "credit_card")
                    span.set_attribute("purchase.payment.last4", intent["last4"])
                    span.set_attribute("purchase.payment.brand", intent["brand"])
                    span.set_attribute("purchase.product.id", detail["id"])
                    try:
                        payment = _http_json("POST", f"{PAYMENT_API}/payments", {
                            "order_id": order_id,
                            "amount": amount,
                            "method": "credit_card",
                            "card_last4": intent["last4"],
                            "card_brand": intent["brand"],
                            "product_id": detail["id"],
                            "quantity": intent["quantity"],
                        })
                    except PaymentDeclined as exc:
                        span.set_attribute("purchase.payment.status", "declined")
                        span.set_attribute("payment.failure.reason", exc.reason)
                        span.set_attribute("payment.failure.stage", exc.stage)
                        span.record_exception(exc)
                        span.set_status(Status(StatusCode.ERROR, f"declined: {exc.reason}"))
                        m.record_tool_call("payment.charge", "mutable_write", "not_attempted")
                        outcome = "declined"
                        run.logical_outcome = "declined"
                        yield {"type": "tool", "name": "payment.charge", "status": "error",
                               "detail": f"declined by the gateway: {exc.reason} (stage: {exc.stage}, code {exc.gateway_code})"}
                        summary = (f"Order {order_id} was declined: {exc.reason.replace('_', ' ')} "
                                   f"at stage {exc.stage}. No charge was made and no effect was created.")
                        yield {"type": "final", "outcome": "declined", "reason": exc.reason, "stage": exc.stage,
                               "order_id": order_id, "summary": summary}
                        log_with_context(logger, logging.INFO,
                                         f"purchase {order_id} declined: product={detail['name']} method=credit_card last4={intent['last4']} reason={exc.reason} stage={exc.stage}",
                                         run_id=run.run_id, thread_id=thread_id, category="purchase")
                        finish("declined")
                        return
                    span.set_attribute("purchase.payment.status", "approved")
                    m.record_tool_call("payment.charge", "mutable_write", "not_attempted")
                yield {"type": "tool", "name": "payment.charge", "status": "ok",
                       "detail": f"approved: {payment['payment_id']} auth {payment['auth_code']}"}

                outcome = "accepted"
                run.logical_outcome = "accepted"
                effect_id = new_id("eff")
                with effect_span(effect_id, f"charge:{order_id}", "create",
                                 confirmation_status="confirmed", compensation_available=True):
                    pass
                yield {"type": "effect", "resource": f"charge:{order_id}", "status": "confirmed",
                       "detail": "mutable_write with compensation available (refund)"}
                yield {"type": "decision", "kind": "respond", "iteration": 2,
                       "basis": f"payment:{payment['payment_id']} amount:{amount}", "outcome": "confirmed"}
                m.record_decision("respond")
                summary = (f"Order {order_id} approved: {intent['quantity']}x {detail['name']} — "
                           f"${amount:.2f} charged to {intent['brand']} ****{intent['last4']} "
                           f"({payment['payment_id']}, auth {payment['auth_code']}).")
                yield {"type": "final", "outcome": "accepted", "order_id": order_id,
                       "payment_id": payment["payment_id"], "summary": summary}
                log_with_context(logger, logging.INFO,
                                 f"purchase {order_id} approved: product={detail['name']} amount={amount} method=credit_card last4={intent['last4']} payment={payment['payment_id']}",
                                 run_id=run.run_id, thread_id=thread_id, category="purchase")
                finish("accepted")
    finally:
        conn.close()
