import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass

from opentelemetry.trace import SpanContext

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.spans import (
    ATTR_LOGICAL_OUTCOME,
    emit_lifecycle,
    link,
    log_with_context,
    new_id,
    run_span,
)


@dataclass
class RunHandle:
    run_id: str
    thread_id: str
    span_context: SpanContext
    workflow_name: str = ""
    workflow_version: str = ""
    outcome: str | None = None
    logical_outcome: str | None = None

    def resume_link(self, relationship: str = "resume"):
        return link(self.span_context, relationship)


def complete_run(handle: RunHandle, reason: str = "work finished"):
    handle.outcome = "completed"
    handle.outcome_reason = reason


def fail_run(handle: RunHandle, reason: str = "execution failed"):
    handle.outcome = "failed"
    handle.outcome_reason = reason


def cancel_run(handle: RunHandle, reason: str = "cancelled"):
    handle.outcome = "cancelled"
    handle.outcome_reason = reason


@contextmanager
def durable_run(
    thread_id: str,
    workflow_name: str,
    workflow_version: str,
    *,
    run_id: str | None = None,
    links: list | None = None,
    resume_from: RunHandle | None = None,
    resume_reason: str | None = None,
    parent: RunHandle | None = None,
    logger: logging.Logger | None = None,
):
    logger = logger or logging.getLogger("oaar.run")
    run_id = run_id or new_id("run")
    links = list(links or [])
    kwargs = {}
    cause = "invocation"
    if resume_from is not None:
        links.append(resume_from.resume_link("resume"))
        kwargs["resume_reason"] = resume_reason or "checkpoint_restore"
        cause = kwargs["resume_reason"]
    if parent is not None:
        kwargs["parent_run_id"] = parent.run_id
    started = time.monotonic()
    op = correlation.current_operation()
    extra_attributes = {correlation.OPERATION_ID_ATTR: op} if op else None
    with run_span(
        run_id,
        thread_id,
        workflow_name,
        workflow_version,
        links=links,
        extra_attributes=extra_attributes,
        **kwargs,
    ) as span:
        handle = RunHandle(
            run_id, thread_id, span.get_span_context(), workflow_name, workflow_version
        )
        emit_lifecycle(
            "agent.run.created", reason=cause, actor="runtime", state_version=0
        )
        emit_lifecycle(
            "agent.run.started", reason=cause, actor="runtime", state_version=0
        )
        log_with_context(
            logger,
            logging.INFO,
            f"run {run_id} started thread={thread_id}",
            run_id=run_id,
            thread_id=thread_id,
            category="lifecycle",
        )
        try:
            yield handle
        except Exception as exc:
            handle.outcome = "failed"
            duration_ms = (time.monotonic() - started) * 1000
            if handle.logical_outcome is None:
                handle.logical_outcome = "pending"
            span.set_attribute(ATTR_LOGICAL_OUTCOME, handle.logical_outcome)
            emit_lifecycle(
                "agent.run.failed",
                reason=str(exc)[:200] or "error",
                actor="runtime",
                state_version=0,
            )
            m.record_run("failed", workflow_name, workflow_version, duration_ms)
            log_with_context(
                logger,
                logging.ERROR,
                f"run {run_id} failed: {exc}",
                run_id=run_id,
                thread_id=thread_id,
                category="lifecycle",
            )
            raise
        else:
            if handle.outcome is None:
                handle.outcome = "completed"
            duration_ms = (time.monotonic() - started) * 1000
            if handle.logical_outcome:
                span.set_attribute(ATTR_LOGICAL_OUTCOME, handle.logical_outcome)
            emit_lifecycle(
                f"agent.run.{handle.outcome}",
                reason=getattr(handle, "outcome_reason", "run finished"),
                actor="runtime",
                state_version=0,
            )
            m.record_run(handle.outcome, workflow_name, workflow_version, duration_ms)
            log_with_context(
                logger,
                logging.INFO,
                f"run {run_id} {handle.outcome}",
                run_id=run_id,
                thread_id=thread_id,
                category="lifecycle",
            )
