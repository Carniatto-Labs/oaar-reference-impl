from opentelemetry import metrics

from oaar_impl.spans import (
    ATTR_CHECKPOINT_ID,
    ATTR_COMPENSATION_STATE,
    ATTR_EFFECT_CONFIRMATION,
    ATTR_DECISION_ID,
    ATTR_EFFECT_ID,
    ATTR_PARENT_RUN_ID,
    ATTR_RUN_ID,
    ATTR_SESSION_ID,
    ATTR_THREAD_ID,
    ATTR_TOOL_CALL_ID,
    ATTR_TOOL_IDEMPOTENCY_KEY,
)

ATTR_OUTCOME = "agent.run.outcome"
ATTR_WORKFLOW_NAME = "agent.runtime.workflow.name"
ATTR_WORKFLOW_VERSION = "agent.runtime.workflow.version"
ATTR_DECISION_KIND = "agent.decision.kind"
ATTR_TOOL_NAME = "gen_ai.tool.name"
ATTR_TOOL_EFFECT_CLASS = "agent.tool.effect.class"
ATTR_TOOL_DEDUP_STATUS = "agent.tool.deduplication.status"
ATTR_COMPENSATION_OUTCOME = "agent.compensation.outcome"
ATTR_GUARDRAIL_OUTCOME = "agent.guardrail.outcome"
ATTR_PURCHASE_OUTCOME = "agent.purchase.outcome"
ATTR_OUTCOME_DEFINITION = "agent.outcome.definition"
ATTR_EFFECT_STATUS = ATTR_EFFECT_CONFIRMATION

ALLOWED_METRIC_ATTRIBUTE_KEYS = frozenset(
    {
        ATTR_OUTCOME,
        ATTR_WORKFLOW_NAME,
        ATTR_WORKFLOW_VERSION,
        ATTR_DECISION_KIND,
        ATTR_TOOL_NAME,
        ATTR_TOOL_EFFECT_CLASS,
        ATTR_TOOL_DEDUP_STATUS,
        ATTR_COMPENSATION_OUTCOME,
        ATTR_GUARDRAIL_OUTCOME,
        ATTR_PURCHASE_OUTCOME,
        ATTR_OUTCOME_DEFINITION,
        ATTR_EFFECT_STATUS,
        ATTR_COMPENSATION_STATE,
    }
)

FORBIDDEN_METRIC_ATTRIBUTE_KEYS = frozenset(
    {
        ATTR_RUN_ID,
        ATTR_THREAD_ID,
        ATTR_SESSION_ID,
        ATTR_PARENT_RUN_ID,
        ATTR_CHECKPOINT_ID,
        ATTR_DECISION_ID,
        ATTR_TOOL_CALL_ID,
        ATTR_TOOL_IDEMPOTENCY_KEY,
        ATTR_EFFECT_ID,
    }
)


class MetricCardinalityError(ValueError):
    pass


def enforce_metric_attributes(attributes: dict) -> None:
    for key in dict(attributes or {}):
        if key in FORBIDDEN_METRIC_ATTRIBUTE_KEYS:
            raise MetricCardinalityError(
                f"unique identifier {key!r} MUST NOT be a metric dimension (RFC section 11)"
            )
        if key not in ALLOWED_METRIC_ATTRIBUTE_KEYS:
            raise MetricCardinalityError(
                f"metric attribute {key!r} is not in the bounded allowlist "
                f"{sorted(ALLOWED_METRIC_ATTRIBUTE_KEYS)}"
            )


_meter = None
_cached_instruments = None


def _get_instruments():
    global _meter, _instruments
    global _meter, _cached_instruments
    if _cached_instruments is None:
        _meter = metrics.get_meter("oaar.impl")
        _cached_instruments = {
            "runs": _meter.create_counter(
                "oaar.runs", description="Run outcomes, execution level (RFC section 11)"
            ),
            "run_duration": _meter.create_histogram(
                "oaar.run.duration", unit="ms", description="Run wall-clock duration"
            ),
            "decisions": _meter.create_counter(
                "oaar.decisions", description="Decision cycles by kind"
            ),
            "tool_calls": _meter.create_counter(
                "oaar.tool.calls", description="Tool executions by classification"
            ),
            "compensations": _meter.create_counter(
                "oaar.compensations", description="Compensation attempts by outcome"
            ),
            "guardrail_evaluations": _meter.create_counter(
                "oaar.guardrail.evaluations", description="Guardrail evaluations by outcome"
            ),
            "purchases": _meter.create_counter(
                "oaar.purchases", description="Purchase orders by business outcome"
            ),
            "outcome_duplicates": _meter.create_counter(
                "oaar.outcome.duplicates",
                description="Terminal outcomes suppressed as duplicates of a recorded operation outcome",
            ),
            "effect_resolutions": _meter.create_counter(
                "oaar.effect.resolutions",
                description="Logical effects by confirmation status at the reporting cut",
            ),
            "outcome_conflicts": _meter.create_counter(
                "oaar.outcome.conflicts",
                description="Conflicting terminal outcomes offered for an already-resolved operation",
            ),
        }
    return _cached_instruments


def record_run(outcome: str, workflow_name: str, workflow_version: str, duration_ms: float):
    attrs = {
        ATTR_OUTCOME: outcome,
        ATTR_WORKFLOW_NAME: workflow_name,
        ATTR_WORKFLOW_VERSION: workflow_version,
    }
    enforce_metric_attributes(attrs)
    instruments = _get_instruments()
    instruments["runs"].add(1, attrs)
    instruments["run_duration"].record(duration_ms, attrs)


def record_decision(kind: str):
    attrs = {ATTR_DECISION_KIND: kind}
    enforce_metric_attributes(attrs)
    _get_instruments()["decisions"].add(1, attrs)


def record_tool_call(tool_name: str, effect_class: str, deduplication_status: str):
    attrs = {
        ATTR_TOOL_NAME: tool_name,
        ATTR_TOOL_EFFECT_CLASS: effect_class,
        ATTR_TOOL_DEDUP_STATUS: deduplication_status,
    }
    enforce_metric_attributes(attrs)
    _get_instruments()["tool_calls"].add(1, attrs)


def record_compensation(outcome: str | None, state: str = "verified"):
    """One compensation per terminal state.

    A compensation that was only requested, or whose readback never arrived, is recorded as
    ``state=unverified`` and carries no outcome — requesting is not evidence that it happened.
    """
    attrs = {ATTR_COMPENSATION_STATE: state}
    if outcome:
        attrs[ATTR_COMPENSATION_OUTCOME] = outcome
    enforce_metric_attributes(attrs)
    _get_instruments()["compensations"].add(1, attrs)


def record_effect_resolution(status: str):
    attrs = {ATTR_EFFECT_STATUS: status}
    enforce_metric_attributes(attrs)
    _get_instruments()["effect_resolutions"].add(1, attrs)


def record_guardrail(outcome: str):
    attrs = {ATTR_GUARDRAIL_OUTCOME: outcome}
    enforce_metric_attributes(attrs)
    _get_instruments()["guardrail_evaluations"].add(1, attrs)


def record_purchase(outcome: str):
    attrs = {ATTR_PURCHASE_OUTCOME: outcome}
    enforce_metric_attributes(attrs)
    _get_instruments()["purchases"].add(1, attrs)


def record_outcome_duplicate(definition: str):
    attrs = {ATTR_OUTCOME_DEFINITION: definition}
    enforce_metric_attributes(attrs)
    _get_instruments()["outcome_duplicates"].add(1, attrs)


def record_outcome_conflict(definition: str):
    attrs = {ATTR_OUTCOME_DEFINITION: definition}
    enforce_metric_attributes(attrs)
    _get_instruments()["outcome_conflicts"].add(1, attrs)
