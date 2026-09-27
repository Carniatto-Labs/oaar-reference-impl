import json
import sys
import urllib.parse

from oaar_impl.correlation import OPERATION_ID_ATTR
from oaar_impl.spans import (
    ATTR_COMPENSATION_OUTCOME,
    ATTR_EFFECT_CONFIRMATION,
    ATTR_EFFECT_ID,
    ATTR_LINK_TYPE,
    ATTR_RUN_ID,
)

from oaar_impl.demo_compensation_saga import WORKFLOW_NAME
from oaar_impl.verify_langgraph import attr_value, b64_hex, fetch_trace, iter_events, iter_spans
from oaar_impl.verify_pipeline import http_get, poll

PROMETHEUS = "http://prometheus:9090"
EFFECT_SPAN = "agent.effect"
COMPENSATE_SPAN = "agent.compensate"
RUN_SPAN = "agent.run"


def prom_total(metric: str, label_key: str, label_value: str) -> float:
    query = urllib.parse.quote(f'sum({metric}{{{label_key}="{label_value}"}})')
    status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
    if status != 200:
        return 0.0
    results = json.loads(body).get("data", {}).get("result", [])
    return float(results[0]["value"][1]) if results else 0.0


def _events(doc, name):
    return [
        (span, event)
        for span in iter_spans(doc)
        for event in iter_events(span)
        if event.get("name") == name
    ]


def check_run_failed_mid_saga(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_run"])
        if doc is None:
            return False, "run trace not found yet"
        if not _events(doc, "agent.run.failed"):
            return False, "run did not fail"
        effects = [s for s in iter_spans(doc) if s.get("name") == EFFECT_SPAN]
        flight = [
            s
            for s in effects
            if attr_value(s.get("attributes"), ATTR_EFFECT_ID) == ctx["flight_effect_id"]
            and attr_value(s.get("attributes"), ATTR_EFFECT_CONFIRMATION) == "confirmed"
        ]
        if len(flight) != 1:
            return False, "confirmed flight effect missing"
        if len(effects) != 1:
            return False, f"expected only the flight effect, found {len(effects)}"
        run_span = next(
            (s for s in iter_spans(doc) if s.get("name") == RUN_SPAN and attr_value(s.get("attributes"), ATTR_RUN_ID) == ctx["run"]),
            None,
        )
        if run_span is None or attr_value(run_span.get("attributes"), OPERATION_ID_ATTR) != ctx["operation_id"]:
            return False, "run span missing logical operation correlation"
        return True, "run failed after the confirmed flight effect; operation correlated"

    return fn


def check_compensation(ctx):
    def fn():
        doc = fetch_trace(ctx["saga_trace"])
        if doc is None:
            return False, "saga trace not found yet"
        comps = [s for s in iter_spans(doc) if s.get("name") == COMPENSATE_SPAN]
        if not comps:
            return False, "compensate span missing"
        comp = comps[0]
        if attr_value(comp.get("attributes"), ATTR_COMPENSATION_OUTCOME) != "partial":
            return False, "compensation outcome should be partial"
        if attr_value(comp.get("attributes"), "agent.effect.id") != ctx["flight_effect_id"]:
            return False, "compensation does not reference the effect id"
        if not any(
            attr_value(link_.get("attributes", []), ATTR_LINK_TYPE) == "compensates"
            and b64_hex(link_.get("traceId", "")) == ctx["trace_run"]
            for link_ in comp.get("links", [])
        ):
            return False, "compensates link to the origin run missing"
        return True, "compensate span: partial outcome, effect id reference, compensates link"

    return fn


def check_metrics(ctx):
    def fn():
        compensations = prom_total("oaar_compensations_total", "agent_compensation_outcome", "partial")
        if compensations < 1:
            return False, "partial compensation counter missing"
        query = urllib.parse.quote(
            f'oaar_runs_total{{agent_runtime_workflow_name="{WORKFLOW_NAME}",agent_run_outcome="failed"}}'
        )
        status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
        results = json.loads(body).get("data", {}).get("result", []) if status == 200 else []
        failed = float(results[0]["value"][1]) if results else 0.0
        if failed < 1:
            return False, "saga run failure not counted"
        return True, f"compensation rate fed (partial={compensations}); failed run counted ({failed})"

    return fn


def main() -> int:
    with open("/tmp/oaar-saga-context.json") as f:
        ctx = json.load(f)
    checks = [
        ("run failed after confirmed effect", check_run_failed_mid_saga(ctx)),
        ("compensation partial + linked", check_compensation(ctx)),
        ("compensation metrics", check_metrics(ctx)),
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
