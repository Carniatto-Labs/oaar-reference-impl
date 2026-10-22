import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass

from opentelemetry.trace import SpanContext

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.spans import (
    emit_lifecycle,
    link,
    log_with_context,
    new_id,
    run_span,
    set_artifact_outcome,
    validate_artifact_outcome,
)


@dataclass
class RunHandle:
    run_id: str
    thread_id: str
    span_context: SpanContext
    workflow_name: str = ""
    workflow_version: str = ""
    outcome: str | None = None
    _artifact_outcome: str | None = None

    @property
    def artifact_outcome(self) -> str | None:
        return self._artifact_outcome

    @artifact_outcome.setter
    def artifact_outcome(self, value: str) -> None:
        # Fail at assignment, not at span close: a business value ("accepted") must never
        # reach the artifact-level attribute, and a caller that tries it should learn
        # immediately, before the run outcome is recorded.
        validate_artifact_outcome(value)
        self._artifact_outcome = value

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
            if handle.artifact_outcome is None:
                # the run ended without producing its artifact and without deciding that it
                # never will: at the artifact level the operation is still pending
                handle.artifact_outcome = "pending"
            set_artifact_outcome(span, handle.artifact_outcome)
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
            if handle.artifact_outcome:
                set_artifact_outcome(span, handle.artifact_outcome)
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
