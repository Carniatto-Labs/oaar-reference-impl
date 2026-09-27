import base64
import json
import sys

from oaar_impl.spans import (
    ATTR_CHECKPOINT_ID,
    ATTR_LINK_TYPE,
    ATTR_RUN_ID,
    ATTR_STATE_VERSION,
)

from oaar_impl.verify_pipeline import http_get, poll

TEMPO = "http://tempo:3200"
CONTEXT_PATH = "/tmp/oaar-langgraph-context.json"

SAVE_SPAN = "agent.checkpoint.save"
RESTORE_SPAN = "agent.checkpoint.restore"
RUN_SPAN = "agent.run"
TRANSITION_EVENT = "agent.state.transition"
RESUMED_EVENT = "agent.run.resumed"
RUN_LINK_TYPE_ATTR = "agent.runtime.link.type"


def b64_hex(value: str) -> str:
    return base64.b64decode(value + "=" * (-len(value) % 4)).hex()


def fetch_trace(trace_id):
    status, body = http_get(f"{TEMPO}/api/traces/{trace_id}", {"Accept": "application/json"})
    if status != 200:
        return None
    return json.loads(body)


def iter_spans(doc):
    for batch in doc.get("batches", []):
        for scope in batch.get("scopeSpans", []):
            for span in scope.get("spans", []):
                yield span


def attr_value(attrs, key):
    for attr in attrs or []:
        if attr.get("key") == key:
            value = attr.get("value", {})
            for field in ("intValue", "doubleValue", "boolValue", "stringValue"):
                if field in value:
                    raw = value[field]
                    return int(raw) if field == "intValue" else raw
    return None


def iter_events(span):
    for event in span.get("events", []):
        yield event


def check_run_a_saves(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_a"])
        if doc is None:
            return False, "trace A not found in Tempo yet"
        saves = [s for s in iter_spans(doc) if s.get("name") == SAVE_SPAN]
        if len(saves) < 2:
            return False, f"expected >=2 {SAVE_SPAN} spans in run A, found {len(saves)}"
        return True, f"{len(saves)} {SAVE_SPAN} spans in run A"

    return fn


def check_run_a_transitions(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_a"])
        if doc is None:
            return False, "trace A not found in Tempo yet"
        versions = []
        for span in iter_spans(doc):
            for event in iter_events(span):
                if event.get("name") == TRANSITION_EVENT:
                    version = attr_value(event.get("attributes"), ATTR_STATE_VERSION)
                    if version is not None:
                        versions.append(version)
        if not versions:
            return False, f"no {TRANSITION_EVENT} events in run A"
        return True, f"run A transitions with state versions {sorted(versions)}"

    return fn


def check_run_b_restore(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_b"])
        if doc is None:
            return False, "trace B not found in Tempo yet"
        for span in iter_spans(doc):
            if span.get("name") == RESTORE_SPAN:
                cid = attr_value(span.get("attributes"), ATTR_CHECKPOINT_ID)
                if cid == ctx["last_checkpoint_a"]:
                    return True, f"restore span references {cid}"
                return False, f"restore references {cid}, expected {ctx['last_checkpoint_a']}"
        return False, f"no {RESTORE_SPAN} span in run B"

    return fn


def check_run_b_resume_link(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_b"])
        if doc is None:
            return False, "trace B not found in Tempo yet"
        for span in iter_spans(doc):
            if span.get("name") == RUN_SPAN and attr_value(span.get("attributes"), ATTR_RUN_ID) == ctx["run_b"]:
                for link_ in span.get("links", []):
                    link_attrs = link_.get("attributes", [])
                    if attr_value(link_attrs, RUN_LINK_TYPE_ATTR) == "resume" and b64_hex(link_.get("traceId", "")) == ctx["trace_a"]:
                        return True, f"resume link on run B -> trace A ({ctx['trace_a'][:12]}...)"
                return False, "run B span found but no resume link to trace A"
        return False, "agent.run span for run B not found"

    return fn


def check_run_b_resumed(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_b"])
        if doc is None:
            return False, "trace B not found in Tempo yet"
        for span in iter_spans(doc):
            for event in iter_events(span):
                if event.get("name") == RESUMED_EVENT:
                    return True, f"{RESUMED_EVENT} event present in run B"
        return False, f"{RESUMED_EVENT} event not found yet"

    return fn


def check_versions_monotonic(ctx):
    def fn():
        doc_a = fetch_trace(ctx["trace_a"])
        doc_b = fetch_trace(ctx["trace_b"])
        if doc_a is None or doc_b is None:
            return False, "traces not found in Tempo yet"

        def versions(doc):
            out = []
            for span in iter_spans(doc):
                for event in iter_events(span):
                    if event.get("name") == TRANSITION_EVENT:
                        version = attr_value(event.get("attributes"), ATTR_STATE_VERSION)
                        if version is not None:
                            out.append(version)
            return out

        versions_a, versions_b = versions(doc_a), versions(doc_b)
        if not versions_a or not versions_b:
            return False, "missing transition events for comparison"
        max_a, min_b, max_b = max(versions_a), min(versions_b), max(versions_b)
        if max_a > min_b:
            return False, f"state version regressed: run A max {max_a} > run B min {min_b}"
        if max_b <= max_a:
            return False, f"no state progress: run B max {max_b} <= run A max {max_a}"
        return True, f"versions monotonic across runs: A<={max_a} < B[{min_b}..{max_b}]"

    return fn


def main() -> int:
    with open(CONTEXT_PATH) as f:
        ctx = json.load(f)
    checks = [
        ("run A checkpoint saves", check_run_a_saves(ctx)),
        ("run A state transitions", check_run_a_transitions(ctx)),
        ("run B restore references last checkpoint of A", check_run_b_restore(ctx)),
        ("run B resume link -> trace A", check_run_b_resume_link(ctx)),
        ("run B resumed lifecycle event", check_run_b_resumed(ctx)),
        ("state version monotonic across runs", check_versions_monotonic(ctx)),
    ]
    failures = 0
    for name, check in checks:
        ok, detail = poll(check)
        marker = "PASS" if ok else "FAIL"
        print(f"[{marker}] {name}: {detail}")
        if not ok:
            failures += 1
    print(f"\n{'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
