import json
import logging
import time
import urllib.request

from opentelemetry.propagate import inject
from opentelemetry.trace import StatusCode, Status

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import link, new_id, tool_span

WORKFLOW_NAME = "scenario.mcp_retry_dedup"
WORKFLOW_VERSION = "0.1.0"
CONTEXT_PATH = "/tmp/oaar-mcp-context.json"
MCP_URL = "http://mcp-server:8081/execute"
TOOL_NAME = "mcp.echo"
IDEMPOTENCY_KEY_PREFIX = "idem-mcp"


def call_mcp(payload: str, idempotency_key: str, fail: bool = False) -> dict:
    headers = {"Content-Type": "application/json"}
    inject(headers)
    body = json.dumps(
        {
            "payload": payload,
            "idempotency_key": idempotency_key,
            "fail": fail,
        }
    ).encode()
    request = urllib.request.Request(MCP_URL, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=10) as resp:
        return json.loads(resp.read())


def main() -> int:
    configure_telemetry()
    logger = logging.getLogger("oaar.mcp")
    thread_id = f"thread-mcp-{int(time.time())}"
    idempotency_key = f"{IDEMPOTENCY_KEY_PREFIX}-{int(time.time())}"
    result = {}

    with correlation.operation("tool:mcp-demo"):
        with durable_run(
            thread_id, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
        ) as handle:
            # Attempt 1: transient failure at the MCP server
            with tool_span(
                new_id("call"),
                TOOL_NAME,
                "read_only",
                mcp_server="mcp-server:8081",
                idempotency_key=idempotency_key,
                retry_count=0,
            ) as first:
                first_context = first.get_span_context()
                try:
                    call_mcp("hello", idempotency_key, fail=True)
                except Exception as exc:
                    first.record_exception(exc)
                    first.set_status(Status(StatusCode.ERROR, "transient failure"))
                    result["attempt1_failed"] = True

            # Attempt 2: retry succeeds; carries a retries link to attempt 1
            with tool_span(
                new_id("call"),
                TOOL_NAME,
                "read_only",
                mcp_server="mcp-server:8081",
                idempotency_key=idempotency_key,
                retry_count=1,
                links=[link(first_context, "retries")],
            ) as retry:
                result["attempt2"] = call_mcp("hello", idempotency_key)
                retry_trace_id = format(retry.get_span_context().trace_id, "032x")
                m.record_tool_call(TOOL_NAME, "read_only", "executed")

            # Attempt 3: same idempotency key, server deduplicates
            with tool_span(
                new_id("call"),
                TOOL_NAME,
                "read_only",
                mcp_server="mcp-server:8081",
                idempotency_key=idempotency_key,
                deduplication_status="duplicate",
                retry_count=0,
            ):
                result["attempt3"] = call_mcp("hello", idempotency_key)
                m.record_tool_call(TOOL_NAME, "read_only", "duplicate")

    context = {
        "workflow_name": WORKFLOW_NAME,
        "run": handle.run_id,
        "trace": format(handle.span_context.trace_id, "032x"),
        "retry_trace_id": retry_trace_id,
        "idempotency_key": idempotency_key,
        "traceparent_echo": result["attempt2"]["traceparent"],
        "deduplicated": result["attempt3"]["deduplicated"],
        "attempt1_failed": result.get("attempt1_failed", False),
    }
    with open(CONTEXT_PATH, "w") as f:
        json.dump(context, f, indent=2)
    log_with_context_safe(
        logger,
        f"mcp scenario done run={handle.run_id} dedup={context['deduplicated']}",
        run_id=handle.run_id,
        thread_id=thread_id,
    )
    print(f"mcp context written to {CONTEXT_PATH}")
    print(json.dumps(context, indent=2))
    shutdown_telemetry()
    return 0


def log_with_context_safe(logger, message, run_id, thread_id):
    from oaar_impl.spans import log_with_context

    log_with_context(
        logger,
        logging.INFO,
        message,
        run_id=run_id,
        thread_id=thread_id,
        category="demo",
    )


if __name__ == "__main__":
    raise SystemExit(main())
