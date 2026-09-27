import json
import sys
import urllib.parse

from oaar_impl.spans import (
    ATTR_LINK_TYPE,
    ATTR_PARENT_RUN_ID,
    ATTR_RUN_ID,
)

from oaar_impl.verify_langgraph import attr_value, b64_hex, fetch_trace, iter_events, iter_spans
from oaar_impl.verify_pipeline import http_get, poll

PROMETHEUS = "http://prometheus:9090"
RUN_SPAN = "agent.run"


def prom_completed(workflow_name: str) -> float:
    query = urllib.parse.quote(
        f'oaar_runs_total{{agent_runtime_workflow_name="{workflow_name}",agent_run_outcome="completed"}}'
    )
    status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
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


def check_children_reference_parent(ctx):
    def fn():
        for run_id, trace_id in zip(ctx["child_runs"], ctx["child_traces"]):
            doc = fetch_trace(trace_id)
            if doc is None:
                return False, f"child trace {trace_id[:12]} not found yet"
            span = next(
                (s for s in iter_spans(doc) if s.get("name") == RUN_SPAN and attr_value(s.get("attributes"), ATTR_RUN_ID) == run_id),
                None,
            )
            if span is None:
                return False, f"child run span {run_id} not found"
            if attr_value(span.get("attributes"), ATTR_PARENT_RUN_ID) != ctx["parent_run_id"]:
                return False, f"child {run_id} missing parent_run.id"
        return True, f"{len(ctx['child_runs'])} children carry parent_run.id = {ctx['parent_run_id']}"

    return fn


def check_parent_links(ctx):
    def fn():
        doc = fetch_trace(ctx["parent_trace"])
        if doc is None:
            return False, "parent trace not found yet"
        span = next(
            (s for s in iter_spans(doc) if s.get("name") == RUN_SPAN and attr_value(s.get("attributes"), ATTR_RUN_ID) == ctx["parent_run_id"]),
            None,
        )
        if span is None:
            return False, "parent run span not found"
        links = span.get("links", [])
        for rel in ("delegates_to", "aggregates"):
            matched = {
                b64_hex(link_.get("traceId", ""))
                for link_ in links
                if attr_value(link_.get("attributes", []), ATTR_LINK_TYPE) == rel
            }
            if not set(ctx["child_traces"]).issubset(matched):
                return False, f"parent missing {rel} links to all children"
        return True, "parent carries delegates_to + aggregates links to both children"

    return fn


def check_all_completed(ctx):
    def fn():
        for trace_id in [ctx["parent_trace"]] + ctx["child_traces"]:
            doc = fetch_trace(trace_id)
            if doc is None:
                return False, "traces not found yet"
            if not _events(doc, "agent.run.completed"):
                return False, "missing completed event"
        return True, "parent and both children completed"

    return fn


def check_metrics(ctx):
    def fn():
        children = prom_completed("scenario.delegation_child")
        parent = prom_completed("scenario.delegation_parent")
        if children < 2 or parent < 1:
            return False, f"parent={parent} children={children}"
        return True, f"execution counters: parent completed={parent}, children completed={children}"

    return fn


def main() -> int:
    with open("/tmp/oaar-fanout-context.json") as f:
        ctx = json.load(f)
    checks = [
        ("children reference parent run", check_children_reference_parent(ctx)),
        ("parent delegates_to + aggregates links", check_parent_links(ctx)),
        ("parent and children completed", check_all_completed(ctx)),
        ("execution metrics", check_metrics(ctx)),
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
