from datetime import datetime, timezone

from opentelemetry import trace

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.outcome_registry import OutcomeRegistry, OutcomeResult, default_registry
from oaar_impl.spans import SemanticViolation

OUTCOME_EVENT = "agent.outcome.observed"

ATTR_OUTCOME_SUBJECT = "agent.outcome.subject"
ATTR_OUTCOME_DEFINITION = "agent.outcome.definition"
ATTR_OUTCOME_DEFINITION_VERSION = "agent.outcome.definition.version"
ATTR_OUTCOME_VALUE = "agent.outcome.value"
ATTR_OUTCOME_OBSERVED_AT = "agent.outcome.observed_at"
ATTR_OUTCOME_RECORDED = "agent.outcome.recorded"
ATTR_OUTCOME_PRIOR_VALUE = "agent.outcome.prior_value"

OUTCOME_VALUES = frozenset({"accepted", "declined", "pending", "unknown"})


def emit_outcome(
    subject: str,
    definition: str,
    definition_version: str,
    value: str,
    observed_at: str | None = None,
    *,
    registry: OutcomeRegistry | None = None,
    count_metric: bool = False,
) -> OutcomeResult:
    """Record an application outcome observation.

    The observation is always kept as history: every attempt that reports an outcome emits
    an event. Only the first terminal value for a (subject, definition, version) is
    *recorded* for counting purposes, so retries, restores and replays cannot inflate a
    business rate. Callers that own a business counter pass ``count_metric=True``.
    """
    if value not in OUTCOME_VALUES:
        raise SemanticViolation(
            f"outcome value {value!r} invalid; must be one of {sorted(OUTCOME_VALUES)}"
        )
    span = trace.get_current_span()
    if not span or not span.get_span_context().is_valid:
        raise SemanticViolation("outcome events require an active span")

    result = (registry or default_registry()).resolve(
        subject, definition, definition_version, value
    )

    attributes = {
        ATTR_OUTCOME_SUBJECT: subject,
        ATTR_OUTCOME_DEFINITION: definition,
        ATTR_OUTCOME_DEFINITION_VERSION: definition_version,
        ATTR_OUTCOME_VALUE: value,
        ATTR_OUTCOME_OBSERVED_AT: observed_at or datetime.now(timezone.utc).isoformat(),
        ATTR_OUTCOME_RECORDED: result.recorded,
    }
    if result.prior_value is not None:
        attributes[ATTR_OUTCOME_PRIOR_VALUE] = result.prior_value
    operation_id = correlation.current_operation()
    if operation_id:
        attributes[correlation.OPERATION_ID_ATTR] = operation_id
    span.add_event(OUTCOME_EVENT, attributes=attributes)

    if count_metric and result.recorded:
        m.record_purchase(value)
    if result.duplicate:
        m.record_outcome_duplicate(definition)
    if result.conflict:
        m.record_outcome_conflict(definition)

    return result
