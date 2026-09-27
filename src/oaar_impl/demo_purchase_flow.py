import json
import logging
import os
import time
import urllib.error
import urllib.request

from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.checkpointer_otel import OtelCheckpointer
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.outcomes import emit_outcome
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import (
    decision_span,
    effect_span,
    log_with_context,
    new_id,
    tool_span,
)

WORKFLOW_NAME = "scenario.purchase_flow"
WORKFLOW_VERSION = "0.1.0"
CONTEXT_PATH = "/tmp/oaar-purchase-context.json"
PRODUCT_API = "http://product-api:8082"
PAYMENT_API = "http://payment-api:8083"

PRODUCT_ID = "sku-1234"
PAYMENT_METHOD = "credit_card"
CARD_LAST4 = "4444"
# Full PAN must NEVER enter telemetry; only the tokenized last4 travels.
BAIT_PAN = "4111111111111111"

logger = logging.getLogger("oaar.purchase")


class PaymentDeclined(RuntimeError):
    def __init__(self, reason: str, stage: str, gateway_code: str):
        super().__init__(f"payment declined: {reason} at {stage}")
        self.reason = reason
        self.stage = stage
        self.gateway_code = gateway_code


def _get(url: str) -> dict:
    headers = {}
    inject(headers)
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=10) as resp:
        return json.loads(resp.read())


def _post(url: str, payload: dict) -> dict:
    headers = {"Content-Type": "application/json"}
    inject(headers)
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        detail = json.loads(exc.read()).get("detail", {})
        raise PaymentDeclined(
            detail.get("reason", "unknown"),
            detail.get("stage", "unknown"),
            detail.get("gateway_code", "XX"),
        ) from None


def _lookup_product(product_id: str) -> dict:
    with tool_span(new_id("call"), "product.lookup", "read_only") as span:
        span.set_attribute("purchase.product.api", "product-api:8082")
        data = _get(f"{PRODUCT_API}/products/{product_id}")
        m.record_tool_call("product.lookup", "read_only", "not_attempted")
        product = data["product"]
        span.set_attribute("purchase.product.id", product["id"])
        span.set_attribute("purchase.product.name", product["name"])
        span.set_attribute("purchase.product.price", product["price"])
        return product


def _charge(order_id: str, amount: float, force: str | None):
    with tool_span(
        new_id("call"),
        "payment.charge",
        "mutable_write",
    ) as span:
        span.set_attribute("purchase.order.id", order_id)
        span.set_attribute("purchase.amount", amount)
        span.set_attribute("purchase.payment.method", PAYMENT_METHOD)
        span.set_attribute("purchase.payment.last4", CARD_LAST4)
        try:
            result = _post(
                PAYMENT_API + "/payments",
                {
                    "order_id": order_id,
                    "amount": amount,
                    "method": PAYMENT_METHOD,
                    "card_last4": CARD_LAST4,
                    "force": force,
                },
            )
        except PaymentDeclined as exc:
            span.set_attribute("purchase.payment.status", "declined")
            span.set_attribute("payment.failure.reason", exc.reason)
            span.set_attribute("payment.failure.stage", exc.stage)
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, f"declined: {exc.reason}"))
            m.record_tool_call("payment.charge", "mutable_write", "not_attempted")
            raise
        span.set_attribute("purchase.payment.status", "approved")
        m.record_tool_call("payment.charge", "mutable_write", "not_attempted")
        return result


def _purchase(checkpointer, order_id: str, force: str | None) -> dict:
    dsn = os.environ.get("OAAR_POSTGRES_DSN", "postgresql://oaar:oaar@postgres:5432/oaar")
    conn = psycopg_connect(dsn)
    saver = OtelCheckpointer(conn)
    saver.setup()
    thread_id = f"thread-buy-{order_id}"
    config = {"configurable": {"thread_id": thread_id}}
    result = {"order_id": order_id}

    with correlation.operation(f"purchase:{order_id}"):
        with durable_run(thread_id, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger) as run:
            result["run_id"] = run.run_id
            result["trace"] = format(run.span_context.trace_id, "032x")

            product = _lookup_product(PRODUCT_ID)
            result["product"] = product
            amount = product["price"]

            with decision_span(
                new_id("dec"),
                "select_tool",
                f"product:{PRODUCT_ID} price:{amount} method:{PAYMENT_METHOD}",
                "proceed",
                1,
            ):
                m.record_decision("select_tool")

            try:
                payment = _charge(order_id, amount, force)
            except PaymentDeclined as exc:
                run.logical_outcome = "declined"
                result["declined"] = {"reason": exc.reason, "stage": exc.stage}
                _finish(run, "declined", order_id)
                log_with_context(
                    logger, logging.INFO,
                    f"purchase {order_id} declined: product={product['name']} method={PAYMENT_METHOD} last4={CARD_LAST4} reason={exc.reason} stage={exc.stage}",
                    run_id=run.run_id, thread_id=thread_id, category="purchase",
                )
                conn.close()
                return result

            result["payment"] = payment
            run.logical_outcome = "accepted"
            effect_id = new_id("eff")
            with effect_span(
                effect_id, f"charge:{order_id}", "create",
                confirmation_status="confirmed",
                compensation_available=True,
            ):
                pass
            result["effect_id"] = effect_id
            _finish(run, "accepted", order_id)
            log_with_context(
                logger, logging.INFO,
                f"purchase {order_id} approved: product={product['name']} amount={amount} method={PAYMENT_METHOD} last4={CARD_LAST4} payment={payment['payment_id']}",
                run_id=run.run_id, thread_id=thread_id, category="purchase",
            )
    conn.close()
    return result


def _finish(run, value: str, order_id: str):
    emit_outcome(f"purchase:{order_id}", "payment.authorization", "1.0.0", value)
    m.record_purchase(value)


def prom_runs_by_outcome(workflow_name: str) -> dict:
    import urllib.parse

    query = urllib.parse.quote(
        f'sum by (agent_run_outcome) (oaar_runs_total{{agent_runtime_workflow_name="{workflow_name}"}})'
    )
    request = urllib.request.Request(
        f"http://prometheus:9090/api/v1/query?query={query}"
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            data = json.loads(resp.read()).get("data", {}).get("result", [])
    except Exception:
        return {}
    return {
        r["metric"]["agent_run_outcome"]: float(r["value"][1]) for r in data
    }


def psycopg_connect(dsn):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row)


def main() -> int:
    configure_telemetry()
    ts = int(time.time())
    baseline = prom_runs_by_outcome(WORKFLOW_NAME)

    success = _purchase(None, f"order-{ts}-s", force=None)
    declined = _purchase(None, f"order-{ts}-d", force="declined")

    context = {
        "workflow_name": WORKFLOW_NAME,
        "baseline": baseline,
        "product": success["product"],
        "payment_method": PAYMENT_METHOD,
        "card_last4": CARD_LAST4,
        "bait_pan": BAIT_PAN,
        "success": success,
        "declined": declined,
    }
    with open(CONTEXT_PATH, "w") as f:
        json.dump(context, f, indent=2)
    print(f"purchase context written to {CONTEXT_PATH}")
    print(json.dumps(context, indent=2, default=str))
    shutdown_telemetry()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
