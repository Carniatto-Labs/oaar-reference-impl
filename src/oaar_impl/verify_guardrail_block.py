import json
import sys
import time
import urllib.parse

from oaar_impl.spans import (
    ATTR_GUARDRAIL_OUTCOME,
    ATTR_GUARDRAIL_POLICY_ID,
    ATTR_GUARDRAIL_POLICY_VERSION,
    ATTR_GUARDRAIL_RISK_CATEGORY,
    GUARDRAIL_SPAN_NAME,
)

from oaar_impl.verify_langgraph import attr_value, fetch_trace, iter_events, iter_spans
from oaar_impl.verify_pipeline import LOKI, http_get, poll

RUN_SPAN = "agent.run"


def prom_blocked() -> float:
    query = urllib.parse.quote('sum(oaar_guardrail_evaluations_total{agent_guardrail_outcome="blocked"})')
    status, body = http_get(f"http://prometheus:9090/api/v1/query?query={query}")
    if status != 200:
        return 0.0
    results = json.loads(body).get("data", {}).get("result", [])
    return float(results[0]["value"][1]) if results else 0.0


def _events(doc, name):
    return [
        event
        for span in iter_spans(doc)
        for event in iter_events(span)
        if event.get("name") == name
    ]


def check_guardrail_span(ctx):
    def fn():
        doc = fetch_trace(ctx["trace"])
        if doc is None:
            return False, "trace not found yet"
        spans = [s for s in iter_spans(doc) if s.get("name") == GUARDRAIL_SPAN_NAME]
        if not spans:
            return False, "guardrail span missing"
        attrs = spans[0].get("attributes", [])
        if attr_value(attrs, ATTR_GUARDRAIL_POLICY_ID) != ctx["policy_id"]:
            return False, "policy id missing"
        if attr_value(attrs, ATTR_GUARDRAIL_POLICY_VERSION) != ctx["policy_version"]:
            return False, "policy version missing"
        if attr_value(attrs, ATTR_GUARDRAIL_OUTCOME) != "blocked":
            return False, "guardrail outcome should be blocked"
        if attr_value(attrs, ATTR_GUARDRAIL_RISK_CATEGORY) != "pii":
            return False, "risk category missing"
        return True, f"guardrail.evaluate: {ctx['policy_id']}@{ctx['policy_version']} blocked pii"

    return fn


def check_no_sensitive_content(ctx):
    def fn():
        doc = fetch_trace(ctx["trace"])
        if doc is None:
            return False, "trace not found yet"
        text = json.dumps(doc)
        if ctx["bait"] in text or ctx["bait"][:20] in text:
            return False, "SENSITIVE LEAK: bait content found in trace"
        return True, "no sensitive source content in telemetry"

    return fn


def check_cancelled_not_failed(ctx):
    def fn():
        doc = fetch_trace(ctx["trace"])
        if doc is None:
            return False, "trace not found yet"
        if not _events(doc, "agent.run.cancelled"):
            return False, "cancelled event missing"
        if _events(doc, "agent.run.completed") or _events(doc, "agent.run.failed"):
            return False, "blocked run must be cancelled, not completed/failed"
        return True, "run cancelled by guardrail (not a failure)"

    return fn


def check_metrics_and_loki(ctx):
    def fn():
        blocked = prom_blocked()
        if blocked < 1:
            return False, "guardrail blocked counter missing"
        end_ns = time.time_ns()
        params = urllib.parse.urlencode(
            {
                "query": '{service_name="oaar-impl"}',
                "start": str(end_ns - 15 * 60 * 1_000_000_000),
                "end": str(end_ns),
                "limit": "300",
            }
        )
        status, body = http_get(f"{LOKI}/loki/api/v1/query_range?{params}")
        if status != 200:
            return False, f"loki returned {status}"
        streams = json.loads(body).get("data", {}).get("result", [])
        lines = [line for stream in streams for _, line in stream.get("values", [])]
        if not any(ctx["run"] in line for line in lines):
            return False, "no log line correlated with run id"
        if any(ctx["bait"] in line for line in lines):
            return False, "SENSITIVE LEAK: bait content found in logs"
        return True, f"blocked={blocked}; logs correlated with run, no sensitive content"

    return fn


def main() -> int:
    with open("/tmp/oaar-guardrail-context.json") as f:
        ctx = json.load(f)
    checks = [
        ("guardrail evaluate span", check_guardrail_span(ctx)),
        ("no sensitive content in trace", check_no_sensitive_content(ctx)),
        ("blocked => cancelled, not failed", check_cancelled_not_failed(ctx)),
        ("metrics + loki privacy", check_metrics_and_loki(ctx)),
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
