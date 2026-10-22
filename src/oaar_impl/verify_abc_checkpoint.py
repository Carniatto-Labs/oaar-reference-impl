import json
import os
import sys

from oaar_impl.correlation import OPERATION_ID_ATTR
from oaar_impl.metrics import MetricCardinalityError, enforce_metric_attributes
from oaar_impl.outcomes import (
    ATTR_OUTCOME_DEFINITION,
    ATTR_OUTCOME_DEFINITION_VERSION,
    ATTR_OUTCOME_VALUE,
)
from oaar_impl.spans import (
    ATTR_CHECKPOINT_ID,
    ATTR_DECISION_BASIS,
    ATTR_DECISION_OUTCOME,
    ATTR_EFFECT_CONFIRMATION,
    ATTR_EFFECT_RESOURCE,
    ATTR_LINK_TYPE,
    ATTR_ARTIFACT_OUTCOME,
    ATTR_RUN_ID,
)

from oaar_impl.demo_abc_checkpoint import (
    ACCEPTANCE_DEFINITION,
    OPERATION_P,
    prom_runs_by_outcome,
)
from oaar_impl.verify_langgraph import attr_value, b64_hex, fetch_trace, iter_events, iter_spans
from oaar_impl.verify_pipeline import poll

RUN_SPAN = "agent.run"
DECISION_SPAN = "agent.decision"
EFFECT_SPAN = "agent.effect"
SAVE_SPAN = "agent.checkpoint.save"
RESTORE_SPAN = "agent.checkpoint.restore"
RESUMED_EVENT = "agent.run.resumed"
OUTCOME_EVENT = "agent.outcome.observed"
FAILED_EVENT = "agent.run.failed"
COMPLETED_EVENT = "agent.run.completed"


def _events(doc, name):
    out = []
    for span in iter_spans(doc):
        for event in iter_events(span):
            if event.get("name") == name:
                out.append((span, event))
    return out


def _run_span(doc, run_id):
    for span in iter_spans(doc):
        if span.get("name") == RUN_SPAN and attr_value(span.get("attributes"), ATTR_RUN_ID) == run_id:
            return span
    return None


def check_execution_denominator(ctx):
    def fn():
        after = prom_runs_by_outcome(ctx["workflow_name"])
        baseline = ctx.get("baseline") or {}
        failed = after.get("failed", 0) - baseline.get("failed", 0)
        completed = after.get("completed", 0) - baseline.get("completed", 0)
        cancelled = after.get("cancelled", 0) - baseline.get("cancelled", 0)
        if failed != 1 or completed != 2 or cancelled != 0:
            return False, f"deltas failed={failed} completed={completed} cancelled={cancelled}"
        return True, f"execution completion = 2/3 (failed={failed}, completed={completed})"

    return fn


def check_run_a_failed_before_artifact(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_a"])
        if doc is None:
            return False, "trace A not found in Tempo yet"
        run_span = _run_span(doc, ctx["run_a"])
        if run_span is None:
            return False, "agent.run span for A not found"
        if attr_value(run_span.get("attributes"), OPERATION_ID_ATTR) != ctx["operation_p"]:
            return False, "run A missing logical operation correlation"
        if attr_value(run_span.get("attributes"), ATTR_ARTIFACT_OUTCOME) != "pending":
            return False, "run A artifact outcome should be pending at its end"
        if not _events(doc, FAILED_EVENT):
            return False, "agent.run.failed event missing in trace A"
        saves = [s for s in iter_spans(doc) if s.get("name") == SAVE_SPAN]
        if len(saves) < 2:
            return False, f"expected >=2 checkpoint saves before failure, found {len(saves)}"
        effects = [s for s in iter_spans(doc) if s.get("name") == EFFECT_SPAN]
        if effects:
            return False, "run A must not produce artifacts, but effect spans exist"
        return True, f"A checkpointed ({len(saves)} saves), failed before any artifact, artifact outcome pending"

    return fn


def check_run_b_restores_and_completes(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_b"])
        if doc is None:
            return False, "trace B not found in Tempo yet"
        restores = [s for s in iter_spans(doc) if s.get("name") == RESTORE_SPAN]
        if not restores:
            return False, "no restore span in trace B"
        cid = attr_value(restores[0].get("attributes"), ATTR_CHECKPOINT_ID)
        if cid != ctx["last_checkpoint_a"]:
            return False, f"restore references {cid}, expected {ctx['last_checkpoint_a']}"
        run_span = _run_span(doc, ctx["run_b"])
        if run_span is None:
            return False, "agent.run span for B not found"
        link_ok = any(
            attr_value(link_.get("attributes", []), ATTR_LINK_TYPE) == "resume"
            and b64_hex(link_.get("traceId", "")) == ctx["trace_a"]
            for link_ in run_span.get("links", [])
        )
        if not link_ok:
            return False, "resume link B -> A missing"
        if not _events(doc, COMPLETED_EVENT):
            return False, "agent.run.completed missing in trace B"
        if attr_value(run_span.get("attributes"), ATTR_ARTIFACT_OUTCOME) != "produced":
            return False, "run B artifact outcome should be produced"
        produced = [
            s
            for s in iter_spans(doc)
            if s.get("name") == EFFECT_SPAN
            and attr_value(s.get("attributes"), ATTR_EFFECT_RESOURCE) == ctx["operation_p"]
            and attr_value(s.get("attributes"), ATTR_EFFECT_CONFIRMATION) == "confirmed"
        ]
        if len(produced) != 1:
            return False, f"expected exactly 1 confirmed effect for P in B, found {len(produced)}"
        return True, "B restored A's last checkpoint (resume link), completed, produced P"

    return fn


def check_run_c_fresh_thread(ctx):
    def fn():
        doc = fetch_trace(ctx["trace_c"])
        if doc is None:
            return False, "trace C not found in Tempo yet"
        if [s for s in iter_spans(doc) if s.get("name") == RESTORE_SPAN]:
            return False, "run C must not restore (fresh thread)"
        if _events(doc, FAILED_EVENT):
            return False, "run C must not contain failure events"
        if not _events(doc, COMPLETED_EVENT):
            return False, "agent.run.completed missing in trace C"
        produced = [
            s
            for s in iter_spans(doc)
            if s.get("name") == EFFECT_SPAN
            and attr_value(s.get("attributes"), ATTR_EFFECT_RESOURCE) == ctx["operation_q"]
            and attr_value(s.get("attributes"), ATTR_EFFECT_CONFIRMATION) == "confirmed"
        ]
        if len(produced) != 1:
            return False, f"expected exactly 1 confirmed effect for Q in C, found {len(produced)}"
        return True, "C completed on a fresh thread and produced Q"

    return fn


def check_artifact_rate(ctx):
    def fn():
        total = 0
        for trace_id, op in ((ctx["trace_b"], ctx["operation_p"]), (ctx["trace_c"], ctx["operation_q"])):
            doc = fetch_trace(trace_id)
            if doc is None:
                return False, "artifact traces not found yet"
            total += len(
                [
                    s
                    for s in iter_spans(doc)
                    if s.get("name") == EFFECT_SPAN
                    and attr_value(s.get("attributes"), ATTR_EFFECT_RESOURCE) == op
                ]
            )
        if total != 2:
            return False, f"artifact production = {total}/2"
        return True, "artifact production = 2/2 (P by B, Q by C)"

    return fn


def _resolution_check(resolution_trace, expected_value, linked_trace):
    doc = fetch_trace(resolution_trace)
    if doc is None:
        return None, "resolution trace not found yet"
    for span in iter_spans(doc):
        if span.get("name") == DECISION_SPAN and attr_value(span.get("attributes"), ATTR_DECISION_BASIS) == "customer_response":
            outcome = attr_value(span.get("attributes"), ATTR_DECISION_OUTCOME)
            if outcome != expected_value:
                return False, f"resolution decision outcome {outcome!r}, expected {expected_value!r}"
            observed = None
            for _, event in _events(doc, OUTCOME_EVENT):
                if attr_value(event.get("attributes"), ATTR_OUTCOME_VALUE) == expected_value:
                    observed = event
            if observed is None:
                return False, f"outcome event with value {expected_value!r} missing"
            attrs = observed.get("attributes", [])
            if attr_value(attrs, ATTR_OUTCOME_DEFINITION) != ACCEPTANCE_DEFINITION:
                return False, "outcome definition missing"
            if not attr_value(attrs, ATTR_OUTCOME_DEFINITION_VERSION):
                return False, "outcome definition version missing"
            linked = any(
                attr_value(link_.get("attributes", []), ATTR_LINK_TYPE) == "triggered_by"
                and b64_hex(link_.get("traceId", "")) == linked_trace
                for link_ in span.get("links", [])
            )
            if not linked:
                return False, "resolution not linked (triggered_by) to the producing run"
            return True, f"resolution outcome {expected_value!r} recorded post-run, linked to producer"
    return None, "customer_response decision span not found"


def check_acceptance_rate(ctx):
    def fn():
        for resolution_trace, expected, linked, label in (
            (ctx["resolution_p_trace"], "declined", ctx["trace_b"], "P"),
            (ctx["resolution_q_trace"], ctx["q_outcome"], ctx["trace_c"], "Q"),
        ):
            ok, detail = _resolution_check(resolution_trace, expected, linked)
            if ok is not True:
                return False, f"{label}: {detail}"
        expected_rate = "1/2 (declined + unknown)" if ctx["q_outcome"] == "unknown" else "1/2 (declined + accepted)"
        return True, f"customer acceptance = {expected_rate}, observed after runs ended"

    return fn


def check_unknown_not_failure(ctx):
    def fn():
        if ctx["q_outcome"] != "unknown":
            return True, "n/a for accepted variant"
        doc = fetch_trace(ctx["trace_c"])
        if doc is None:
            return False, "trace C not found yet"
        if _events(doc, FAILED_EVENT):
            return False, "unknown acceptance must not surface as run failure in C"
        ok, detail = _resolution_check(ctx["resolution_q_trace"], "unknown", ctx["trace_c"])
        if ok is not True:
            return False, f"Q: {detail}"
        after = prom_runs_by_outcome(ctx["workflow_name"])
        baseline = ctx.get("baseline") or {}
        failed = after.get("failed", 0) - baseline.get("failed", 0)
        if failed != 1:
            return False, f"unknown must not inflate failures: failed delta={failed}"
        return True, "pending/unknown is not counted as failure (Q-unknown variant)"

    return fn


def check_correlation_boundary(ctx):
    def fn():
        try:
            enforce_metric_attributes({OPERATION_ID_ATTR: "proposal:prop-001"})
        except MetricCardinalityError:
            pass
        else:
            return False, "logical operation id must be rejected as a metric dimension"
        doc = fetch_trace(ctx["trace_a"])
        if doc is None:
            return False, "trace A not found yet"
        for _, event in _events(doc, "agent.run.checkpointed"):
            if attr_value(event.get("attributes"), OPERATION_ID_ATTR) == ctx["operation_p"]:
                return True, "operation id present on events, rejected by metric allowlist"
        return False, "no lifecycle event carries the logical operation id"

    return fn


def main() -> int:
    context_path = os.environ.get("OAAR_ABC_CONTEXT_PATH", "/tmp/oaar-abc-context.json")
    with open(context_path) as f:
        ctx = json.load(f)
    checks = [
        ("execution denominator 2/3", check_execution_denominator(ctx)),
        ("run A: checkpointed then failed, no artifact", check_run_a_failed_before_artifact(ctx)),
        ("run B: restored, completed, produced P", check_run_b_restores_and_completes(ctx)),
        ("run C: fresh thread, completed, produced Q", check_run_c_fresh_thread(ctx)),
        ("artifact production 2/2", check_artifact_rate(ctx)),
        ("customer acceptance 1/2 (post-run outcomes)", check_acceptance_rate(ctx)),
        ("unknown is not failure (Q-unknown variant)", check_unknown_not_failure(ctx)),
        ("logical operation: on events, never in metrics", check_correlation_boundary(ctx)),
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
