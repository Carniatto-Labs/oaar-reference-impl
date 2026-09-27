import json
import sys
import urllib.parse

from oaar_impl.spans import (
    ATTR_EFFECT_CONFIRMATION,
    ATTR_LINK_TYPE,
    ATTR_RUN_ID,
    EFFECT_SPAN_NAME,
    TOOL_SPAN_NAME,
)

from oaar_impl.verify_langgraph import attr_value, fetch_trace, iter_events, iter_spans
from oaar_impl.verify_pipeline import http_get, poll

PROMETHEUS = "http://prometheus:9090"
RUN_SPAN = "agent.run"


def spans_by_service(doc) -> dict:
    """Map service.name -> spans, using each batch's resource attributes."""
    out = {}
    for batch in doc.get("batches", []):
        svc = attr_value(
            batch.get("resource", {}).get("attributes", []), "service.name"
        )
        for scope in batch.get("scopeSpans", []):
            for span in scope.get("spans", []):
                out.setdefault(svc, []).append(span)
    return out


def _span(doc, name, **attr_filters):
    for span in iter_spans(doc):
        if span.get("name") != name:
            continue
        attrs = span.get("attributes", [])
        if all(attr_value(attrs, k) == v for k, v in attr_filters.items()):
            return span
    return None


def _events(doc, name):
    return [
        event
        for span in iter_spans(doc)
        for event in iter_events(span)
        if event.get("name") == name
    ]


def prom_total(metric, label_key, label_value):
    query = urllib.parse.quote(f'sum({metric}{{{label_key}="{label_value}"}})')
    status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
    if status != 200:
        return 0.0
    results = json.loads(body).get("data", {}).get("result", [])
    return float(results[0]["value"][1]) if results else 0.0


def check_success_flow(ctx):
    def fn():
        doc = fetch_trace(ctx["success"]["trace"])
        if doc is None:
            return False, "success trace not found yet"
        if not _events(doc, "agent.run.completed"):
            return False, "success run did not complete"
        lookup = _span(doc, TOOL_SPAN_NAME)
        lookups = [
            s for s in iter_spans(doc)
            if s.get("name") == TOOL_SPAN_NAME
            and attr_value(s.get("attributes", []), "gen_ai.tool.name") == "product.lookup"
        ]
        if not lookups:
            return False, "product.lookup span missing"
        attrs = lookups[0].get("attributes", [])
        if attr_value(attrs, "purchase.product.id") != ctx["product"]["id"]:
            return False, "product id not attached to the lookup span"
        if attr_value(attrs, "purchase.product.name") != ctx["product"]["name"]:
            return False, "product name missing"
        charge = _span(
            doc, TOOL_SPAN_NAME, **{"gen_ai.tool.name": "payment.charge",
            "purchase.payment.status": "approved"}
        )
        if charge is None:
            return False, "approved payment.charge span missing"
        cattrs = charge.get("attributes", [])
        if attr_value(cattrs, "purchase.payment.method") != ctx["payment_method"]:
            return False, "payment method missing on charge span"
        if attr_value(cattrs, "purchase.payment.last4") != ctx["card_last4"]:
            return False, "card last4 missing"
        effect = _span(doc, EFFECT_SPAN_NAME, **{ATTR_EFFECT_CONFIRMATION: "confirmed"})
        if effect is None:
            return False, "confirmed charge effect span missing"
        return True, (
            f"success: product={ctx['product']['name']} "
            f"method={ctx['payment_method']} ****{ctx['card_last4']} effect confirmed"
        )

    return fn


def check_full_topology(ctx):
    def fn():
        for key in ("success", "declined"):
            doc = fetch_trace(ctx[key]["trace"])
            if doc is None:
                return False, f"{key} trace not found yet"
            services = set(spans_by_service(doc))
            if not {"oaar-impl", "product-api", "payment-api"}.issubset(services):
                return False, f"{key} trace services: {sorted(services)}"
        return True, "both traces span the whole chain: agent + product-api + payment-api (W3C propagation)"

    return fn


def check_declined_flow(ctx):
    def fn():
        doc = fetch_trace(ctx["declined"]["trace"])
        if doc is None:
            return False, "declined trace not found yet"
        lookups = [
            s for s in iter_spans(doc)
            if s.get("name") == TOOL_SPAN_NAME
            and attr_value(s.get("attributes", []), "gen_ai.tool.name") == "product.lookup"
        ]
        if not lookups:
            return False, "product context lost in declined flow"
        charge = _span(
            doc, TOOL_SPAN_NAME, **{"gen_ai.tool.name": "payment.charge",
            "purchase.payment.status": "declined"}
        )
        if charge is None:
            return False, "declined payment.charge span missing"
        cattrs = charge.get("attributes", [])
        if attr_value(cattrs, "payment.failure.reason") != ctx["declined"]["declined"]["reason"]:
            return False, "failure reason missing"
        if attr_value(cattrs, "payment.failure.stage") != ctx["declined"]["declined"]["stage"]:
            return False, "failure stage missing"
        if charge.get("status", {}).get("code") not in (2, "2", "STATUS_CODE_ERROR"):
            return False, "declined attempt not marked as error"
        exceptions = [
            e for e in iter_events(charge) if e.get("name") == "exception"
        ]
        if not exceptions:
            return False, "declined attempt has no exception event"
        if _span(doc, EFFECT_SPAN_NAME) is not None:
            return False, "declined payment must not produce an effect span"
        return True, (
            f"declined: failure mapped to stage='{ctx['declined']['declined']['stage']}' "
            f"reason='{ctx['declined']['declined']['reason']}', no charge effect created"
        )

    return fn


def check_declined_is_business_not_failure(ctx):
    def fn():
        doc = fetch_trace(ctx["declined"]["trace"])
        if doc is None:
            return False, "declined trace not found yet"
        if not _events(doc, "agent.run.completed"):
            return False, "declined run did not complete (agent handled the answer)"
        observed = [e for e in _events(doc, "agent.outcome.observed")]
        values = [
            attr_value(e.get("attributes", []), "agent.outcome.value") for e in observed
        ]
        if "declined" not in values:
            return False, f"outcome.observed values: {values}"
        return True, "decline recorded as business outcome (accepted/declined), not an execution failure"

    return fn


def check_logs_privacy(ctx):
    def fn():
        import time as _time

        end_ns = _time.time_ns()
        params = urllib.parse.urlencode(
            {
                "query": '{service_name="oaar-impl"} |= "purchase "',
                "start": str(end_ns - 15 * 60 * 1_000_000_000),
                "end": str(end_ns),
                "limit": "50",
            }
        )
        status, body = http_get(f"http://loki:3100/loki/api/v1/query_range?{params}")
        if status != 200:
            return False, f"loki returned {status}"
        streams = json.loads(body).get("data", {}).get("result", [])
        lines = [line for stream in streams for _, line in stream.get("values", [])]
        joined = "\n".join(lines)
        if ctx["bait_pan"] in joined:
            return False, "SENSITIVE LEAK: full PAN found in logs"
        if ctx["success"]["order_id"] not in joined:
            return False, "no purchase log line correlated with success order id"
        if "method=credit_card" not in joined:
            return False, "payment method missing from purchase logs"
        return True, f"{len(lines)} purchase log lines: order, product, method ****{ctx['card_last4']}, status; no full PAN"

    return fn


def check_metrics(ctx):
    def fn():
        approved = prom_total("oaar_purchases_total", "agent_purchase_outcome", "accepted")
        declined = prom_total("oaar_purchases_total", "agent_purchase_outcome", "declined")
        if approved < 1 or declined < 1:
            return False, f"accepted={approved} declined={declined}"
        baseline = ctx.get("baseline", {})
        query = urllib.parse.quote(
            f'sum by (agent_run_outcome) (oaar_runs_total{{agent_runtime_workflow_name="{ctx["workflow_name"]}"}})'
        )
        status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
        now = {}
        if status == 200:
            for r in json.loads(body).get("data", {}).get("result", []):
                now[r["metric"]["agent_run_outcome"]] = float(r["value"][1])
        failed_before = baseline.get("failed", 0.0)
        failed_now = now.get("failed", 0.0)
        if failed_now > failed_before:
            return False, f"declined purchase counted as failed execution: {failed_before} -> {failed_now}"
        completed_before = baseline.get("completed", 0.0)
        completed_now = now.get("completed", 0.0)
        if completed_now < completed_before + 2:
            return False, f"expected both purchase runs completed: {completed_before} -> {completed_now}"
        return True, (
            f"purchase metrics by business outcome: accepted={approved}, declined={declined}; "
            f"executions completed {completed_before}->{completed_now}, failed {failed_before}->{failed_now} (decline is not a failure)"
        )

    return fn


def main() -> int:
    with open("/tmp/oaar-purchase-context.json") as f:
        ctx = json.load(f)
    checks = [
        ("success flow: product + payment visible", check_success_flow(ctx)),
        ("full topology in one trace (3 services)", check_full_topology(ctx)),
        ("declined flow: failure mapped to stage/reason", check_declined_flow(ctx)),
        ("decline is business outcome, not failure", check_declined_is_business_not_failure(ctx)),
        ("purchase logs + PAN privacy", check_logs_privacy(ctx)),
        ("purchase metrics by outcome", check_metrics(ctx)),
    ]
    failures = 0
    for name, check in checks:
        ok, detail = poll(check)
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not ok:
            failures += 1
    print(f"\n{'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
