import json
import sys

from oaar_impl.demo_outcome_dedup import prom_counter
from oaar_impl.outcomes import OUTCOME_EVENT
from oaar_impl.spans import (
    ATTR_RUN_ID,
    SemanticViolation,
    set_artifact_outcome,
)
from oaar_impl.verify_langgraph import attr_value, fetch_trace, iter_events, iter_spans
from oaar_impl.verify_pipeline import poll

CONTEXT_PATH = "/tmp/oaar-outcome-dedup-context.json"

ATTR_SUBJECT = "agent.outcome.subject"
ATTR_DEFINITION = "agent.outcome.definition"
ATTR_DEFINITION_VERSION = "agent.outcome.definition.version"
ATTR_OUTCOME_VALUE = "agent.outcome.value"
ATTR_OBSERVED_AT = "agent.outcome.observed_at"
ATTR_RECORDED = "agent.outcome.recorded"
ATTR_PRIOR = "agent.outcome.prior_value"


def _run_span(doc, run_id):
    for span in iter_spans(doc):
        if span.get("name") == "agent.run" and attr_value(span.get("attributes"), ATTR_RUN_ID) == run_id:
            return span
    return None


def _recorded(attrs) -> bool:
    """Absent boolean attribute reads as false (OTLP JSON drops default values)."""
    return attr_value(attrs, ATTR_RECORDED) is True


def _outcome_events(doc, run_id):
    span = _run_span(doc, run_id)
    if span is None:
        return None
    return [
        event
        for event in iter_events(span)
        if event.get("name") == OUTCOME_EVENT
    ]


def check_purchase_counter(ctx):
    def fn():
        baseline = ctx["baseline"]["purchases_accepted"]
        after = prom_counter("oaar_purchases_total", agent_purchase_outcome="accepted")
        delta = after - baseline
        if delta != 3:
            return False, (
                f"expected +3 business outcomes (one per operation), got +{delta}: "
                "attempts leaked into the business denominator"
            )
        return True, "3 operations / 4 runs / 6 observations -> +3 accepted, attempts did not inflate the rate"

    return fn


def check_duplicates(ctx):
    def fn():
        baseline = ctx["baseline"]["duplicates"]
        delta = prom_counter("oaar_outcome_duplicates_total") - baseline
        if delta != 1:
            return False, f"expected +1 suppressed duplicate, got +{delta}"
        return True, "repeated terminal outcome suppressed once, kept on the event stream"

    return fn


def check_conflicts(ctx):
    def fn():
        baseline = ctx["baseline"]["conflicts"]
        delta = prom_counter("oaar_outcome_conflicts_total") - baseline
        if delta != 1:
            return False, f"expected +1 flagged conflict, got +{delta}"
        return True, "late conflicting outcome flagged, recorded value unchanged"

    return fn


def check_duplicate_event(ctx):
    def fn():
        events = _outcome_events(fetch_trace(ctx["trace_run_2"]) or {}, ctx["run_ids"]["run_2"])
        if events is None:
            return False, "trace of the restored run not found in Tempo yet"
        if not events:
            return False, "no agent.outcome.observed event on the restored run"
        event = events[0]
        if _recorded(event.get("attributes")):
            return False, "duplicate observation was marked as recorded"
        if attr_value(event.get("attributes"), ATTR_PRIOR) != "accepted":
            return False, "duplicate observation does not expose the prior recorded value"
        return True, "restored run kept as history with recorded=false and prior_value=accepted"

    return fn


def check_pending_not_counted(ctx):
    def fn():
        events = _outcome_events(fetch_trace(ctx["trace_run_3"]) or {}, ctx["run_ids"]["run_3"])
        if not events:
            return False, "no outcome events on the pending-then-accepted run"
        by_value = {}
        for event in events:
            value = attr_value(event.get("attributes"), ATTR_OUTCOME_VALUE)
            by_value[value] = _recorded(event.get("attributes"))
        if by_value.get("pending") is True:
            return False, "pending observation was recorded as a business outcome"
        if by_value.get("accepted") is not True:
            return False, "terminal accepted value was not recorded"
        return True, "pending stays unrecorded, terminal accepted recorded once"

    return fn


def check_semantics(ctx):
    def fn():
        # Level boundary: business vocabulary must not reach the artifact attribute.
        try:
            set_artifact_outcome(object(), "accepted")
            return False, "artifact outcome accepted a business value (accepted)"
        except SemanticViolation:
            pass
        from oaar_impl.outcomes import emit_outcome

        try:
            emit_outcome("subject:x", "d", "v", "approved")
            return False, "outcome vocabulary accepted an unknown value (approved)"
        except SemanticViolation:
            pass
        # Event completeness: subject, definition, version and observation time are present.
        events = _outcome_events(fetch_trace(ctx["trace_run_1"]) or {}, ctx["run_ids"]["run_1"])
        if not events:
            return False, "no outcome event on run 1 to check completeness"
        attrs = events[0].get("attributes")
        for key in (ATTR_SUBJECT, ATTR_DEFINITION, ATTR_DEFINITION_VERSION, ATTR_OBSERVED_AT):
            if attr_value(attrs, key) in (None, ""):
                return False, f"outcome event missing {key}"
        return True, "artifact and business vocabularies are disjoint; events carry subject/definition/version/observed_at"

    return fn


def main() -> int:
    with open(CONTEXT_PATH) as f:
        ctx = json.load(f)
    checks = [
        ("business counter: one per operation", check_purchase_counter(ctx)),
        ("duplicates suppressed", check_duplicates(ctx)),
        ("conflicts flagged", check_conflicts(ctx)),
        ("restored run kept as history", check_duplicate_event(ctx)),
        ("pending is not an outcome", check_pending_not_counted(ctx)),
        ("outcome vocabularies + event completeness", check_semantics(ctx)),
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
