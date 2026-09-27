from datetime import datetime, timezone

from opentelemetry import trace

from oaar_impl.spans import SemanticViolation

OUTCOME_EVENT = "agent.outcome.observed"

ATTR_OUTCOME_SUBJECT = "agent.outcome.subject"
ATTR_OUTCOME_DEFINITION = "agent.outcome.definition"
ATTR_OUTCOME_DEFINITION_VERSION = "agent.outcome.definition.version"
ATTR_OUTCOME_VALUE = "agent.outcome.value"
ATTR_OUTCOME_OBSERVED_AT = "agent.outcome.observed_at"

OUTCOME_VALUES = frozenset({"accepted", "declined", "pending", "unknown"})


def emit_outcome(
    subject: str,
    definition: str,
    definition_version: str,
    value: str,
    observed_at: str | None = None,
):
    if value not in OUTCOME_VALUES:
        raise SemanticViolation(
            f"outcome value {value!r} invalid; must be one of {sorted(OUTCOME_VALUES)}"
        )
    span = trace.get_current_span()
    if not span or not span.get_span_context().is_valid:
        raise SemanticViolation("outcome events require an active span")
    span.add_event(
        OUTCOME_EVENT,
        attributes={
            ATTR_OUTCOME_SUBJECT: subject,
            ATTR_OUTCOME_DEFINITION: definition,
            ATTR_OUTCOME_DEFINITION_VERSION: definition_version,
            ATTR_OUTCOME_VALUE: value,
            ATTR_OUTCOME_OBSERVED_AT: observed_at
            or datetime.now(timezone.utc).isoformat(),
        },
    )
