import json
import sys
import urllib.parse

from oaar_impl.spans import (
    ATTR_CHECKPOINT_ID,
    ATTR_LINK_TYPE,
    ATTR_RUN_ID,
    ATTR_TRANSITION_ACTOR,
    ATTR_TRANSITION_REASON,
)

from oaar_impl.demo_hitl_resume import WORKFLOW_NAME
from oaar_impl.verify_langgraph import attr_value, b64_hex, fetch_trace, iter_events, iter_spans
from oaar_impl.verify_pipeline import http_get, poll

PROMETHEUS = "http://prometheus:9090"
RESTORE_SPAN = "agent.checkpoint.restore"
SAVE_SPAN = "agent.checkpoint.save"
RUN_SPAN = "agent.run"


def prom_runs_by_outcome(workflow_name: str) -> dict:
    query = urllib.parse.quote(
        f'sum by (agent_run_outcome) (oaar_runs_total{{agent_runtime_workflow_name="{workflow_name}"}})'
    )
    status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
    if status != 200:
        return {}
    out = {}
    for result in json.loads(body).get("data", {}).get("result", []):
        outcome = result.get("metric", {}).get("agent_run_outcome")
        if outcome:
            out[outcome] = float(result["value"][1])
    return out


def _events(doc, name):
    return [
        (span, event)
        for span in iter_spans(doc)
        for event in iter_events(span)
        if event.get("name") == name
    ]


def _run_span(doc, run_id):
    for span in iter_spans(doc):
        if span.get("name") == RUN_SPAN and attr_value(span.get("attributes"), ATTR_RUN_ID) == run_id:
            return span
    return None


def check_waiting_event(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_wait"])
        if doc is None:
            return False, "wait trace not found yet"
        for span, event in _events(doc, "agent.run.waiting"):
            attrs = event.get("attributes", [])
            if attr_value(attrs, ATTR_TRANSITION_REASON) == "human approval required" and attr_value(
                attrs, ATTR_TRANSITION_ACTOR
            ) == "human_gate":
                return True, "agent.run.waiting emitted with human_gate actor"
        return False, "waiting event missing or has wrong reason/actor"

    return fn


def check_resume_restore(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_resume"])
        if doc is None:
            return False, "resume trace not found yet"
        restores = [s for s in iter_spans(doc) if s.get("name") == RESTORE_SPAN]
        if not restores:
            return False, "no restore span in resume trace"
        cid = attr_value(restores[0].get("attributes"), ATTR_CHECKPOINT_ID)
        if cid != ctx["last_checkpoint_wait"]:
            return False, f"restore references {cid}, expected {ctx['last_checkpoint_wait']}"
        run_span = _run_span(doc, ctx["run_resume"])
        if run_span is None:
            return False, "resume run span not found"
        if not any(
            attr_value(link_.get("attributes", []), ATTR_LINK_TYPE) == "resume"
            and b64_hex(link_.get("traceId", "")) == ctx["trace_wait"]
            for link_ in run_span.get("links", [])
        ):
            return False, "resume link missing"
        if not _events(doc, "agent.run.resumed"):
            return False, "resumed event missing"
        return True, "resume restored the waiting checkpoint, linked, resumed event present"

    return fn


def check_wait_duration(ctx):
    def fn():
        doc_wait = fetch_trace(ctx["trace_wait"])
        doc_resume = fetch_trace(ctx["trace_resume"])
        if doc_wait is None or doc_resume is None:
            return False, "traces not found yet"
        saves = [s for s in iter_spans(doc_wait) if s.get("name") == SAVE_SPAN]
        restores = [s for s in iter_spans(doc_resume) if s.get("name") == RESTORE_SPAN]
        if not saves or not restores:
            return False, "save/restore spans missing"
        last_save_end = max(int(s["endTimeUnixNano"]) for s in saves)
        restore_start = min(int(s["startTimeUnixNano"]) for s in restores)
        waited_seconds = (restore_start - last_save_end) / 1e9
        if waited_seconds < 1.5:
            return False, f"waited only {waited_seconds:.2f}s"
        return True, f"human gate held {waited_seconds:.2f}s (checkpoint age at recovery)"

    return fn


def check_resume_completed(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_resume"])
        if doc is None:
            return False, "resume trace not found yet"
        if not _events(doc, "agent.run.completed"):
            return False, "resume run did not complete"
        saves = [s for s in iter_spans(doc) if s.get("name") == SAVE_SPAN]
        if len(saves) < 2:
            return False, f"expected post-resume supersteps (saves={len(saves)})"
        return True, f"resume completed with {len(saves)} checkpoint saves"

    return fn


def check_metrics(ctx):
    def fn():
        after = prom_runs_by_outcome(ctx["workflow_name"])
        if after.get("completed", 0) < 2 or after.get("failed", 0) > 0:
            return False, f"waiting must not count as failure: {after}"
        return True, f"execution counters show completed={after.get('completed', 0)}, failed=0 (waiting != failure)"

    return fn


def main() -> int:
    with open("/tmp/oaar-hitl-context.json") as f:
        ctx = json.load(f)
    checks = [
        ("waiting lifecycle event", check_waiting_event(ctx)),
        ("resume restores waiting checkpoint", check_resume_restore(ctx)),
        ("wait duration / checkpoint age", check_wait_duration(ctx)),
        ("resume completes workflow", check_resume_completed(ctx)),
        ("waiting is not a failure", check_metrics(ctx)),
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
