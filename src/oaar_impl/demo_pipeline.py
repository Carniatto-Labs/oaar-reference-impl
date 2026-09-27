import json
import logging
import time
import uuid

from opentelemetry import trace

from oaar_impl import metrics as m
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.spans import (
    decision_span,
    emit_lifecycle,
    emit_state_transition,
    log_with_context,
    run_span,
    tool_span,
)

CONTEXT_PATH = "/tmp/oaar-demo-context.json"
REDACTION_BAIT = "contact ops@example.com or rotate key sk-demo1234567890abcdef"


def main() -> int:
    configure_telemetry()
    logger = logging.getLogger("oaar.demo")
    run_id = f"run-{uuid.uuid4().hex[:12]}"
    thread_id = "thread-demo-0001"
    workflow_name = "demo.pipeline"
    workflow_version = "0.1.0"
    started = time.monotonic()
    outcome = "completed"

    with run_span(
        run_id,
        thread_id,
        workflow_name,
        workflow_version,
        extra_attributes={"debug.note": REDACTION_BAIT},
    ) as span:
        trace_id = format(span.get_span_context().trace_id, "032x")
        emit_lifecycle("agent.run.created", reason="pipeline smoke", actor="runtime", state_version=0)
        emit_lifecycle("agent.run.started", reason="pipeline smoke", actor="runtime", state_version=0)
        log_with_context(
            logger,
            logging.INFO,
            f"run {run_id} started",
            run_id=run_id,
            thread_id=thread_id,
            category="lifecycle",
        )
        emit_state_transition(
            "initialized", "planning", ["stage"], {"stage": "planning"}, "advance to planning", "runtime", 1
        )
        with decision_span(
            "dec-demo-0001",
            "select_tool",
            "echo tool selected for pipeline validation",
            "tool_selected",
            iteration=1,
        ):
            with tool_span(
                "call-demo-0001",
                "echo",
                "read_only",
                mcp_server="oaar-mcp",
                deduplication_status="not_attempted",
            ):
                time.sleep(0.05)
            m.record_tool_call("echo", "read_only", "not_attempted")
            m.record_decision("select_tool")
        emit_state_transition(
            "planning", "completed", ["stage"], {"stage": "completed"}, "work finished", "runtime", 2
        )
        emit_lifecycle("agent.run.completed", reason="pipeline smoke", actor="runtime", state_version=2)
        log_with_context(
            logger,
            logging.INFO,
            f"run {run_id} completed",
            run_id=run_id,
            thread_id=thread_id,
            category="lifecycle",
        )

    duration_ms = (time.monotonic() - started) * 1000
    m.record_run(outcome, workflow_name, workflow_version, duration_ms)
    shutdown_telemetry()

    with open(CONTEXT_PATH, "w") as f:
        json.dump({"trace_id": trace_id, "run_id": run_id, "thread_id": thread_id}, f)
    print(f"demo trace_id={trace_id} run_id={run_id} thread_id={thread_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
