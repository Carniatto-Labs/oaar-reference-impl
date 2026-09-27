import hashlib
import json
import logging
import uuid
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.trace import Link, SpanKind

from oaar_impl import correlation

RUN_SPAN_NAME = "agent.run"
DECISION_SPAN_NAME = "agent.decision"
CHECKPOINT_SAVE_SPAN_NAME = "agent.checkpoint.save"
CHECKPOINT_RESTORE_SPAN_NAME = "agent.checkpoint.restore"
EFFECT_SPAN_NAME = "agent.effect"
COMPENSATE_SPAN_NAME = "agent.compensate"
GUARDRAIL_SPAN_NAME = "agent.guardrail.evaluate"
TOOL_SPAN_NAME = "gen_ai.execute_tool"

ATTR_RUN_ID = "agent.runtime.run.id"
ATTR_THREAD_ID = "agent.runtime.thread.id"
ATTR_SESSION_ID = "agent.runtime.session.id"
ATTR_WORKFLOW_NAME = "agent.runtime.workflow.name"
ATTR_WORKFLOW_VERSION = "agent.runtime.workflow.version"
ATTR_PARENT_RUN_ID = "agent.runtime.parent_run.id"
ATTR_RESUME_REASON = "agent.runtime.resume.reason"
ATTR_USER_ID = "enduser.id"

ATTR_CHECKPOINT_ID = "agent.checkpoint.id"
ATTR_CHECKPOINT_BACKEND = "agent.checkpoint.backend"
ATTR_CHECKPOINT_OUTCOME = "agent.checkpoint.outcome"
ATTR_CHECKPOINT_DURATION_MS = "agent.checkpoint.duration.ms"

ATTR_STATE_VERSION = "agent.state.version"
ATTR_STATE_FROM = "agent.state.from"
ATTR_STATE_TO = "agent.state.to"
ATTR_STATE_CHANGED_KEYS = "agent.state.changed_keys"
ATTR_STATE_DIFF_HASH = "agent.state.diff_hash"
ATTR_TRANSITION_REASON = "agent.state.transition.reason"
ATTR_TRANSITION_ACTOR = "agent.state.transition.actor"

ATTR_DECISION_ID = "agent.decision.id"
ATTR_DECISION_KIND = "agent.decision.kind"
ATTR_DECISION_BASIS = "agent.decision.basis"
ATTR_DECISION_OUTCOME = "agent.decision.outcome"
ATTR_ITERATION = "agent.decision.iteration"

ATTR_TOOL_CALL_ID = "agent.tool.call.id"
ATTR_TOOL_NAME = "gen_ai.tool.name"
ATTR_TOOL_EFFECT_CLASS = "agent.tool.effect.class"
ATTR_TOOL_RETRY_COUNT = "agent.tool.retry.count"
ATTR_TOOL_IDEMPOTENCY_KEY = "agent.tool.idempotency_key"
ATTR_TOOL_DEDUP_STATUS = "agent.tool.deduplication.status"
ATTR_TOOL_MCP_SERVER = "agent.tool.mcp.server"

ATTR_EFFECT_ID = "agent.effect.id"
ATTR_EFFECT_RESOURCE = "agent.effect.resource"
ATTR_EFFECT_MUTATION_TYPE = "agent.effect.mutation.type"
ATTR_EFFECT_CONFIRMATION = "agent.effect.confirmation.status"
ATTR_COMPENSATION_OUTCOME = "agent.compensation.outcome"

ATTR_GUARDRAIL_POLICY_ID = "agent.guardrail.policy.id"
ATTR_GUARDRAIL_POLICY_VERSION = "agent.guardrail.policy.version"
ATTR_GUARDRAIL_OUTCOME = "agent.guardrail.outcome"
ATTR_GUARDRAIL_RISK_CATEGORY = "agent.guardrail.risk.category"

ATTR_LINK_TYPE = "agent.runtime.link.type"
ATTR_LOGICAL_OUTCOME = "agent.runtime.logical.outcome"
ATTR_LOG_CATEGORY = "agent.log.category"
ATTR_LOG_CLASSIFICATION = "agent.log.classification"

LINK_TYPES = frozenset(
    {"resume", "delegates_to", "triggered_by", "compensates", "retries", "aggregates"}
)
EFFECT_CLASSES = frozenset({"read_only", "idempotent_write", "mutable_write"})
DECISION_KINDS = frozenset(
    {"plan", "select_tool", "delegate", "respond", "compensate", "terminate"}
)
COMPENSATION_OUTCOMES = frozenset({"succeeded", "failed", "partial", "not_applicable"})

LIFECYCLE_EVENTS = frozenset(
    {
        "agent.run.created",
        "agent.run.started",
        "agent.run.waiting",
        "agent.run.checkpointed",
        "agent.run.resumed",
        "agent.run.completed",
        "agent.run.failed",
        "agent.run.cancelled",
        "agent.compensation.started",
        "agent.compensation.completed",
    }
)

TRACER_NAME = "oaar.impl"

_unique_id_attrs = frozenset(
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
        correlation.OPERATION_ID_ATTR,
    }
)


class SemanticViolation(ValueError):
    pass


def tracer():
    return trace.get_tracer(TRACER_NAME)


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def diff_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()[:16]


def link(previous_context, relationship: str) -> Link:
    if relationship not in LINK_TYPES:
        raise SemanticViolation(
            f"link type {relationship!r} invalid; must be one of {sorted(LINK_TYPES)}"
        )
    return Link(context=previous_context, attributes={ATTR_LINK_TYPE: relationship})


@contextmanager
def run_span(
    run_id: str,
    thread_id: str,
    workflow_name: str,
    workflow_version: str,
    *,
    parent_run_id: str | None = None,
    resume_reason: str | None = None,
    session_id: str | None = None,
    user_id: str | None = None,
    links: list | None = None,
    extra_attributes: dict | None = None,
):
    attributes = {
        ATTR_RUN_ID: run_id,
        ATTR_THREAD_ID: thread_id,
        ATTR_WORKFLOW_NAME: workflow_name,
        ATTR_WORKFLOW_VERSION: workflow_version,
    }
    if parent_run_id:
        attributes[ATTR_PARENT_RUN_ID] = parent_run_id
    if resume_reason:
        attributes[ATTR_RESUME_REASON] = resume_reason
    if session_id:
        attributes[ATTR_SESSION_ID] = session_id
    if user_id:
        attributes[ATTR_USER_ID] = user_id
    if extra_attributes:
        attributes.update(extra_attributes)
    with tracer().start_as_current_span(
        RUN_SPAN_NAME, kind=SpanKind.CONSUMER, attributes=attributes, links=links or []
    ) as span:
        yield span


@contextmanager
def decision_span(
    decision_id: str, kind: str, basis: str, outcome: str, iteration: int, *, links: list | None = None
):
    if kind not in DECISION_KINDS:
        raise SemanticViolation(
            f"decision kind {kind!r} invalid; must be one of {sorted(DECISION_KINDS)}"
        )
    with tracer().start_as_current_span(
        DECISION_SPAN_NAME,
        attributes={
            ATTR_DECISION_ID: decision_id,
            ATTR_DECISION_KIND: kind,
            ATTR_DECISION_BASIS: basis,
            ATTR_DECISION_OUTCOME: outcome,
            ATTR_ITERATION: iteration,
        },
        links=links or [],
    ) as span:
        yield span


@contextmanager
def checkpoint_span(
    direction: str,
    checkpoint_id: str,
    thread_id: str,
    state_version: int,
    backend: str,
):
    if direction == "save":
        name = CHECKPOINT_SAVE_SPAN_NAME
    elif direction == "restore":
        name = CHECKPOINT_RESTORE_SPAN_NAME
    else:
        raise SemanticViolation("checkpoint direction must be 'save' or 'restore'")
    with tracer().start_as_current_span(
        name,
        attributes={
            ATTR_CHECKPOINT_ID: checkpoint_id,
            ATTR_THREAD_ID: thread_id,
            ATTR_STATE_VERSION: state_version,
            ATTR_CHECKPOINT_BACKEND: backend,
        },
    ) as span:
        yield span


@contextmanager
def tool_span(
    call_id: str,
    name: str,
    effect_class: str,
    *,
    mcp_server: str | None = None,
    idempotency_key: str | None = None,
    deduplication_status: str | None = None,
    retry_count: int = 0,
):
    if effect_class not in EFFECT_CLASSES:
        raise SemanticViolation(
            f"tool effect class {effect_class!r} invalid; must be one of {sorted(EFFECT_CLASSES)}"
        )
    attributes = {
        ATTR_TOOL_CALL_ID: call_id,
        ATTR_TOOL_NAME: name,
        ATTR_TOOL_EFFECT_CLASS: effect_class,
        ATTR_TOOL_RETRY_COUNT: retry_count,
    }
    if mcp_server:
        attributes[ATTR_TOOL_MCP_SERVER] = mcp_server
    if idempotency_key:
        attributes[ATTR_TOOL_IDEMPOTENCY_KEY] = idempotency_key
    if deduplication_status:
        attributes[ATTR_TOOL_DEDUP_STATUS] = deduplication_status
    with tracer().start_as_current_span(
        TOOL_SPAN_NAME, kind=SpanKind.CLIENT, attributes=attributes
    ) as span:
        yield span


@contextmanager
def effect_span(
    effect_id: str,
    resource: str,
    mutation_type: str,
    *,
    confirmation_status: str,
    decision_id: str | None = None,
    tool_call_id: str | None = None,
    compensation_available: bool,
):
    attributes = {
        ATTR_EFFECT_ID: effect_id,
        ATTR_EFFECT_RESOURCE: resource,
        ATTR_EFFECT_MUTATION_TYPE: mutation_type,
        ATTR_EFFECT_CONFIRMATION: confirmation_status,
        "agent.effect.compensation.available": compensation_available,
    }
    if decision_id:
        attributes[ATTR_DECISION_ID] = decision_id
    if tool_call_id:
        attributes[ATTR_TOOL_CALL_ID] = tool_call_id
    with tracer().start_as_current_span(
        EFFECT_SPAN_NAME, kind=SpanKind.CLIENT, attributes=attributes
    ) as span:
        yield span


@contextmanager
def compensate_span(effect_id: str, effect_context, outcome: str):
    if outcome not in COMPENSATION_OUTCOMES:
        raise SemanticViolation(
            f"compensation outcome {outcome!r} invalid; must be one of {sorted(COMPENSATION_OUTCOMES)}"
        )
    with tracer().start_as_current_span(
        COMPENSATE_SPAN_NAME,
        kind=SpanKind.CLIENT,
        attributes={ATTR_EFFECT_ID: effect_id, ATTR_COMPENSATION_OUTCOME: outcome},
        links=[link(effect_context, "compensates")],
    ) as span:
        yield span


@contextmanager
def guardrail_span(
    policy_id: str, policy_version: str, outcome: str, risk_category: str
):
    with tracer().start_as_current_span(
        GUARDRAIL_SPAN_NAME,
        attributes={
            ATTR_GUARDRAIL_POLICY_ID: policy_id,
            ATTR_GUARDRAIL_POLICY_VERSION: policy_version,
            ATTR_GUARDRAIL_OUTCOME: outcome,
            ATTR_GUARDRAIL_RISK_CATEGORY: risk_category,
        },
    ) as span:
        yield span


def _current_span():
    span = trace.get_current_span()
    if not span or not span.get_span_context().is_valid:
        raise SemanticViolation("no active run span; lifecycle events must occur inside run_span")
    return span


def emit_lifecycle(event: str, *, reason: str, actor: str, state_version: int):
    if event not in LIFECYCLE_EVENTS:
        raise SemanticViolation(
            f"lifecycle event {event!r} invalid; must be one of {sorted(LIFECYCLE_EVENTS)}"
        )
    attributes = {
        ATTR_TRANSITION_REASON: reason,
        ATTR_TRANSITION_ACTOR: actor,
        ATTR_STATE_VERSION: state_version,
    }
    op = correlation.current_operation()
    if op:
        attributes[correlation.OPERATION_ID_ATTR] = op
    _current_span().add_event(event, attributes=attributes)


def emit_state_transition(
    previous: str,
    nxt: str,
    changed_keys: list,
    diff: dict,
    reason: str,
    actor: str,
    state_version: int,
):
    attributes = {
        ATTR_STATE_VERSION: state_version,
        ATTR_STATE_FROM: previous,
        ATTR_STATE_TO: nxt,
        ATTR_STATE_CHANGED_KEYS: ",".join(changed_keys),
        ATTR_STATE_DIFF_HASH: diff_hash(diff),
        ATTR_TRANSITION_REASON: reason,
        ATTR_TRANSITION_ACTOR: actor,
    }
    op = correlation.current_operation()
    if op:
        attributes[correlation.OPERATION_ID_ATTR] = op
    _current_span().add_event("agent.state.transition", attributes=attributes)


def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    *,
    run_id: str,
    thread_id: str,
    category: str,
    classification: str = "operational",
):
    logger.log(
        level,
        message,
        extra={
            ATTR_RUN_ID: run_id,
            ATTR_THREAD_ID: thread_id,
            ATTR_LOG_CATEGORY: category,
            ATTR_LOG_CLASSIFICATION: classification,
        },
    )
