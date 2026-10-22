import json
import sys
import time
import urllib.parse
import urllib.request

from oaar_impl import metrics as m
from oaar_impl.spans import ATTR_RUN_ID, SemanticViolation, link

TEMPO = "http://tempo:3200"
PROMETHEUS = "http://prometheus:9090"
LOKI = "http://loki:3100"

CONTEXT_PATH = "/tmp/oaar-demo-context.json"
BAIT_KEY = "sk-demo1234567890abcdef"
BAIT_EMAIL = "ops@example.com"

UNIQUE_LABELS = {
    "agent_runtime_run_id",
    "agent_runtime_thread_id",
    "agent_runtime_session_id",
    "agent_runtime_parent_run_id",
    "agent_checkpoint_id",
    "agent_tool_call_id",
    "agent_effect_id",
    "agent_outcome_subject",
    "agent_operation_id",
    "run_id",
    "thread_id",
}


def http_get(url: str, headers: dict | None = None):
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            return resp.status, resp.read()
    except Exception as exc:
        return getattr(exc, "code", 0), str(exc).encode()


def poll(check, attempts=20, delay=3.0):
    detail = "not run"
    for _ in range(attempts):
        ok, detail = check()
        if ok:
            return True, detail
        time.sleep(delay)
    return False, detail


def check_tempo_trace(ctx):
    def fn():
        status, body = http_get(f"{TEMPO}/api/traces/{ctx['trace_id']}", {"Accept": "application/json"})
        if status != 200:
            return False, f"tempo returned {status}"
        text = body.decode("utf-8", "replace")
        if BAIT_KEY in text or BAIT_EMAIL in text:
            return False, "REDACTION LEAK: bait values found in stored trace"
        if "***REDACTED***" not in text:
            return False, "trace stored but redaction marker missing"
        return True, "trace found, sensitive values redacted"

    return fn


def check_prometheus_counter(ctx):
    def fn():
        query = urllib.parse.quote('oaar_runs_total{agent_run_outcome="completed"}')
        status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
        if status != 200:
            return False, f"prometheus returned {status}"
        data = json.loads(body)
        results = data.get("data", {}).get("result", [])
        if not results:
            return False, "oaar_runs_total{completed} series not present yet"
        value = float(results[0]["value"][1])
        if value < 1:
            return False, f"counter value {value} unexpected"
        return True, f"oaar_runs_total{{completed}} = {value}"

    return fn


def check_prometheus_cardinality(ctx):
    def fn():
        matcher = urllib.parse.quote('{__name__=~"oaar_.*"}')
        status, body = http_get(f"{PROMETHEUS}/api/v1/series?match%5B%5D={matcher}")
        if status != 200:
            return False, f"prometheus returned {status}"
        series = json.loads(body).get("data", [])
        if not series:
            return False, "no oaar_ series yet"
        leaked = UNIQUE_LABELS.intersection({k for item in series for k in item})
        if leaked:
            return False, f"UNIQUE ID LEAKED AS METRIC LABEL: {sorted(leaked)}"
        return True, f"{len(series)} series, none carries unique-id labels"

    return fn


def check_loki_log(ctx):
    def fn():
        end_ns = time.time_ns()
        start_ns = end_ns - 15 * 60 * 1_000_000_000
        params = urllib.parse.urlencode(
            {
                "query": '{service_name="oaar-impl"}',
                "start": str(start_ns),
                "end": str(end_ns),
                "limit": "200",
            }
        )
        status, body = http_get(f"{LOKI}/loki/api/v1/query_range?{params}")
        if status != 200:
            return False, f"loki returned {status}"
        streams = json.loads(body).get("data", {}).get("result", [])
        lines = [line for stream in streams for _, line in stream.get("values", [])]
        if not lines:
            return False, "no logs in loki yet"
        if not any(ctx["run_id"] in line for line in lines):
            return False, "logs exist but run_id not found in any line"
        return True, f"log line correlated with run_id={ctx['run_id']}"

    return fn


def check_enforcement(ctx):
    def fn():
        try:
            m.enforce_metric_attributes({ATTR_RUN_ID: "run-should-fail"})
            return False, "enforcement accepted a unique id as metric dimension"
        except m.MetricCardinalityError:
            pass
        try:
            m.enforce_metric_attributes({"some.random.attr": "v"})
            return False, "enforcement accepted a non-allowlisted metric attribute"
        except m.MetricCardinalityError:
            pass
        try:
            link(None, "bogus_relationship")
            return False, "link type validation accepted an unknown relationship"
        except SemanticViolation:
            pass
        return True, "metric allowlist and link-type validation both reject violations"

    return fn


def main() -> int:
    with open(CONTEXT_PATH) as f:
        ctx = json.load(f)
    checks = [
        ("tempo-trace-redacted", check_tempo_trace(ctx)),
        ("prometheus-counter", check_prometheus_counter(ctx)),
        ("prometheus-cardinality", check_prometheus_cardinality(ctx)),
        ("loki-correlated-log", check_loki_log(ctx)),
        ("rules-enforcement", check_enforcement(ctx)),
    ]
    width = max(len(name) for name, _ in checks)
    failed = 0
    for name, fn in checks:
        ok, detail = poll(fn)
        if not ok:
            failed += 1
        print(f"{name.ljust(width)}  {'PASS' if ok else 'FAIL'}  {detail}")
    total = len(checks)
    print(f"\n{total - failed}/{total} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
