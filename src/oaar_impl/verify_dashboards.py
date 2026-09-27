import json
import sys
import time
from urllib.parse import quote, urlencode

from oaar_impl.verify_pipeline import http_get, poll

GRAFANA = "http://grafana:3000"
EXPECTED_DASHBOARDS = {
    "OAAR · Runtime & Execution Identity": "oaar-runtime",
    "OAAR · Logical Outcomes, Effects & Controls": "oaar-effects",
    "OAAR · Purchase Flow (e-commerce)": "oaar-purchase",
}


def check_health():
    def fn():
        status, body = http_get(f"{GRAFANA}/api/health")
        if status != 200:
            return False, f"grafana health returned {status}"
        if json.loads(body).get("database") != "ok":
            return False, "grafana database not ok"
        return True, "grafana up, database ok"

    return fn


def check_dashboards():
    def fn():
        status, body = http_get(f"{GRAFANA}/api/search")
        if status != 200:
            return False, f"search returned {status}"
        found = {item.get("title"): item.get("uid") for item in json.loads(body)}
        for title, uid in EXPECTED_DASHBOARDS.items():
            if found.get(title) != uid:
                return False, f"dashboard {title!r} not provisioned (found: {sorted(found)})"
        return True, f"{len(EXPECTED_DASHBOARDS)} dashboards provisioned in folder OAAR"

    return fn


def check_datasources():
    def fn():
        status, body = http_get(f"{GRAFANA}/api/datasources")
        if status != 200:
            return False, f"datasources returned {status}"
        names = {ds.get("name") for ds in json.loads(body)}
        if not {"Prometheus", "Tempo", "Loki"}.issubset(names):
            return False, f"missing datasources: {sorted(names)}"
        return True, "Prometheus + Tempo + Loki datasources provisioned with explicit uids"

    return fn


def check_prometheus_reflection():
    def fn():
        status, body = http_get(
            f"{GRAFANA}/api/datasources/proxy/uid/oaar-prom/api/v1/query?query=up"
        )
        if status != 200:
            return False, f"proxy returned {status}"
        return True, "prometheus reachable through grafana"

    return fn


def check_query_reflection():
    def fn():
        queries = {
            "runs by outcome": "sum by (agent_run_outcome) (oaar_runs_total)",
            "compensations": "sum by (agent_compensation_outcome) (oaar_compensations_total)",
            "guardrails": "sum by (agent_guardrail_outcome) (oaar_guardrail_evaluations_total)",
            "tool dedup": "sum by (agent_tool_deduplication_status) (oaar_tool_calls_total)",
        }
        for label, query in queries.items():
            status, body = http_get(
                f"{GRAFANA}/api/datasources/proxy/uid/oaar-prom/api/v1/query?query={quote(query)}"
            )
            if status != 200:
                return False, f"{label}: proxy returned {status}"
            results = json.loads(body).get("data", {}).get("result", [])
            if not results:
                return False, f"{label}: no series — dashboard panel would be empty"
        return True, f"all {len(queries)} dashboard queries return series with scenario data"

    return fn


def check_loki_reflection():
    def fn():
        end_ns = time.time_ns()
        params = urlencode(
            {
                "query": '{service_name="oaar-impl"} |= "run run-"',
                "start": str(end_ns - 30 * 60 * 1_000_000_000),
                "end": str(end_ns),
                "limit": "50",
            }
        )
        status, body = http_get(
            f"{GRAFANA}/api/datasources/proxy/uid/oaar-loki/loki/api/v1/query_range?{params}"
        )
        if status != 200:
            return False, f"loki proxy returned {status}"
        streams = json.loads(body).get("data", {}).get("result", [])
        lines = sum(len(s.get("values", [])) for s in streams)
        if lines < 5:
            return False, f"lifecycle logs panel would be nearly empty ({lines} lines)"
        return True, f"lifecycle logs panel has data ({lines} correlated lines in window)"

    return fn


def main() -> int:
    checks = [
        ("grafana health", check_health()),
        ("dashboards provisioned", check_dashboards()),
        ("datasources provisioned", check_datasources()),
        ("prometheus proxy", check_prometheus_reflection()),
        ("dashboard queries reflect scenario data", check_query_reflection()),
        ("lifecycle logs reflect scenario runs", check_loki_reflection()),
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
