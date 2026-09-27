import json
import sys
import urllib.parse

from oaar_impl.spans import (
    ATTR_LINK_TYPE,
    ATTR_TOOL_DEDUP_STATUS,
    ATTR_TOOL_IDEMPOTENCY_KEY,
    ATTR_TOOL_MCP_SERVER,
    ATTR_TOOL_NAME,
    ATTR_TOOL_RETRY_COUNT,
    TOOL_SPAN_NAME,
)

from oaar_impl.verify_langgraph import attr_value, b64_hex, fetch_trace, iter_spans
from oaar_impl.verify_pipeline import http_get, poll

PROMETHEUS = "http://prometheus:9090"
RUN_SPAN = "agent.run"


def prom_tool_calls(tool_name: str, dedup: str) -> float:
    query = urllib.parse.quote(
        f'oaar_tool_calls_total{{gen_ai_tool_name="{tool_name}",agent_tool_deduplication_status="{dedup}"}}'
    )
    status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
    if status != 200:
        return 0.0
    results = json.loads(body).get("data", {}).get("result", [])
    return float(results[0]["value"][1]) if results else 0.0


def _tool_spans(doc):
    return [s for s in iter_spans(doc) if s.get("name") == TOOL_SPAN_NAME]


def check_retry_link(ctx):
    def fn():
        doc = fetch_trace(ctx["trace"])
        if doc is None:
            return False, "trace not found yet"
        tools = _tool_spans(doc)
        if len(tools) != 3:
            return False, f"expected 3 tool spans (attempts), found {len(tools)}"
        retries = [s for s in tools if attr_value(s.get("attributes"), ATTR_TOOL_RETRY_COUNT) == 1]
        if not retries:
            return False, "retry attempt span (retry_count=1) missing"
        if not any(
            attr_value(link_.get("attributes", []), ATTR_LINK_TYPE) == "retries"
            and b64_hex(link_.get("traceId", "")) == ctx["trace"]
            for link_ in retries[0].get("links", [])
        ):
            return False, "retries link to failed attempt missing"
        if not any(
            attr_value(s.get("attributes"), ATTR_TOOL_DEDUP_STATUS) == "duplicate"
            for s in tools
        ):
            return False, "no span flagged deduplication_status=duplicate"
        keys = {attr_value(s.get("attributes"), ATTR_TOOL_IDEMPOTENCY_KEY) for s in tools}
        if keys != {ctx["idempotency_key"]}:
            return False, f"idempotency keys inconsistent: {keys}"
        return True, "attempts correlated: retry_count=1 with retries link, dedup flagged, same idempotency key"

    return fn


def _is_error(span) -> bool:
    code = span.get("status", {}).get("code")
    return code in (2, "2", "STATUS_CODE_ERROR")


def check_error_status(ctx):
    def fn():
        doc = fetch_trace(ctx["trace"])
        if doc is None:
            return False, "trace not found yet"
        tools = _tool_spans(doc)
        errors = [s for s in tools if _is_error(s)]
        if len(errors) != 1:
            return False, f"expected exactly 1 error-status tool span, found {len(errors)}"
        if attr_value(errors[0].get("attributes"), ATTR_TOOL_RETRY_COUNT) != 0:
            return False, "error span should be the first attempt"
        return True, "failed attempt carries error status; retried attempt did not"

    return fn


def check_w3c_traceparent(ctx):
    def fn():
        parts = ctx["traceparent_echo"].split("-")
        if len(parts) != 4:
            return False, f"malformed traceparent: {ctx['traceparent_echo']}"
        version, trace_id, span_id, flags = parts
        if version != "00" or len(flags) != 2:
            return False, "unexpected version/flags"
        if trace_id != ctx["retry_trace_id"] or trace_id != ctx["trace"]:
            return False, "traceparent trace id does not match the tool span trace"
        if len(span_id) != 16:
            return False, "span id malformed"
        return True, f"MCP server propagated the caller trace context (trace {trace_id[:12]}...)"

    return fn


def check_dedup(ctx):
    def fn():
        doc = fetch_trace(ctx["trace"])
        if doc is None:
            return False, "trace not found yet"
        tools = _tool_spans(doc)
        dup = [
            s
            for s in tools
            if attr_value(s.get("attributes"), ATTR_TOOL_DEDUP_STATUS) == "duplicate"
        ]
        if not dup:
            return False, "duplicate attempt span missing"
        if attr_value(dup[0].get("attributes"), ATTR_TOOL_MCP_SERVER) != "mcp-server:8081":
            return False, "mcp server attr missing"
        if not ctx["deduplicated"]:
            return False, "server did not deduplicate the third call"
        if not ctx["attempt1_failed"]:
            return False, "attempt 1 was expected to fail"
        return True, "third call served from idempotency store; deduplication visible in telemetry"

    return fn


def check_metrics(ctx):
    def fn():
        executed = prom_tool_calls("mcp.echo", "executed")
        duplicate = prom_tool_calls("mcp.echo", "duplicate")
        if executed < 1 or duplicate < 1:
            return False, f"executed={executed} duplicate={duplicate}"
        return True, f"tool metrics split by outcome: executed={executed}, duplicate={duplicate}"

    return fn


def main() -> int:
    with open("/tmp/oaar-mcp-context.json") as f:
        ctx = json.load(f)
    checks = [
        ("retry correlation + idempotency attrs", check_retry_link(ctx)),
        ("failed attempt error status", check_error_status(ctx)),
        ("W3C trace propagation to MCP server", check_w3c_traceparent(ctx)),
        ("deduplication observable", check_dedup(ctx)),
        ("tool call metrics", check_metrics(ctx)),
    ]
    failures = 0
    for name, check in checks:
        ok, detail = poll(check)
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not ok:
            failures += 1
    print(f"\n{'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
