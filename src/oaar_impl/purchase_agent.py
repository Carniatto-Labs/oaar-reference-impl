import contextvars
import json
import logging
import os
import re
import time
from collections.abc import Iterator
from typing import Annotated, Any, TypedDict
from urllib.parse import quote

from langchain_core.messages import AnyMessage, ToolMessage
from langchain_core.tools import tool
from langchain_ollama import ChatOllama
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.checkpointer_otel import OtelCheckpointer
from oaar_impl.outcome_registry import OutcomeRegistry
from oaar_impl.outcomes import emit_outcome
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import decision_span, effect_span, log_with_context, new_id, tool_span

WORKFLOW_NAME = "agent.purchase"
WORKFLOW_VERSION = "0.2.0"
PRODUCT_API = "http://product-api:8082"
PAYMENT_API = "http://payment-api:8083"
MODEL_NAME = os.environ.get("OAAR_MODEL", "gemma4:12b-mlx")
OLLAMA_BASE_URL = os.environ.get("OAAR_OLLAMA_BASE_URL", "http://host.docker.internal:11434")
MAX_STEPS = 20

logger = logging.getLogger("oaar.agent")

# Durable-outcome store boundary: one terminal business outcome per logical operation, no
# matter how many runs/attempts report it. In-process here; a production runtime backs the
# same interface with a unique index on (subject, definition, definition_version).
OUTCOME_REGISTRY = OutcomeRegistry()

# Artifact level of the three-level outcome model: did this run produce its artifact?
_ARTIFACT_OUTCOME = {
    "accepted": "produced",
    "declined": "not_applicable",
    "pending": "pending",
}


_current_order: contextvars.ContextVar[str] = contextvars.ContextVar("oaar_purchase_order", default="")

_BRAND_PATTERNS = [
    ("amex", re.compile(r"^3[47]")),
    ("elo", re.compile(r"^(636368|438935|504175|451416|636297|5067|4576|4011)")),
    ("mastercard", re.compile(r"^(5[1-5]|2[2-7])")),
    ("visa", re.compile(r"^4")),
]

SYSTEM_PROMPT = (
    "You are a purchase assistant for an online store. You complete purchases by calling tools, step by step.\n"
    "Rules:\n"
    "- ALWAYS call search_products first to find what the user wants; then charge_card to complete the purchase.\n"
    "- quantity defaults to 1 when the user does not specify an amount.\n"
    "- The card number must come from the user's message. If no card number is present, ask the user for it and call no tools.\n"
    "- Never invent product ids: use ids returned by search_products.\n"
    "- If charge_card returns status=declined, tell the user the reason and the stage (no retry).\n"
    "- Final answer: one short sentence stating what was bought, the total, the card brand with last 4 digits, and the outcome (approved or declined)."
)


def _detect_brand(pan: str) -> str:
    for brand, pattern in _BRAND_PATTERNS:
        if pattern.match(pan):
            return brand
    return "unknown"


def _sanitize(text: str) -> str:
    return re.sub(
        r"\d[\d\s.\-]{11,22}\d",
        lambda mo: "****" + re.sub(r"\D", "", mo.group(0))[-4:],
        text,
    )


def _api(method: str, url: str, payload: dict | None = None) -> dict:
    import urllib.error
    import urllib.request

    headers: dict = {}
    inject(headers)
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode()
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read()).get("detail", {})
        except Exception:
            detail = {}
        if isinstance(detail, dict) and detail.get("reason"):
            return {"status": "declined", **detail}
        return {"status": "error", "reason": f"http {exc.code}", "stage": "unknown"}


@tool
def search_products(query: str) -> str:
    """Search the store catalog by free text. Returns matching products with id, name, price and stock."""
    return json.dumps(_api("GET", f"{PRODUCT_API}/products/search?q={quote(query)}").get("results", []))


@tool
def charge_card(product_id: str, quantity: int, card_number: str) -> str:
    """Charge the customer's card for quantity units of product_id. card_number is the full card digits. Returns the payment result."""
    pan = re.sub(r"\D", "", card_number)
    last4 = pan[-4:]
    brand = _detect_brand(pan)
    order_id = _current_order.get("") or f"order-{int(time.time() * 1000)}"
    product = (_api("GET", f"{PRODUCT_API}/products/{product_id}").get("product") or {})
    if not product:
        result = {"status": "declined", "reason": "not_found", "stage": "product_catalog", "product": product_id}
    else:
        amount = round(float(product["price"]) * max(int(quantity), 1), 2)
        response = _api("POST", f"{PAYMENT_API}/payments", {
            "order_id": order_id,
            "amount": amount,
            "method": "credit_card",
            "card_last4": last4,
            "card_brand": brand,
            "product_id": product_id,
            "quantity": max(int(quantity), 1),
            "idempotency_key": f"charge:{order_id}",
        })
        result = {**response, "product": product.get("name"), "amount": amount,
                  "card_brand": brand, "card_last4": last4, "order_id": order_id}
    return json.dumps(result)


TOOLS = [search_products, charge_card]
_TOOL_EFFECT = {"search_products": "read_only", "charge_card": "mutable_write"}
_TOOL_LABEL = {"search_products": "product.search", "charge_card": "payment.charge"}


class AgentState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    charges: dict


def _model() -> ChatOllama:
    return ChatOllama(model=MODEL_NAME, base_url=OLLAMA_BASE_URL, temperature=0)


def _agent_node(state: AgentState) -> dict:
    messages = state["messages"]
    model = _model()
    tracer = trace.get_tracer("oaar.agent")
    with tracer.start_as_current_span(
        f"chat {MODEL_NAME}",
        attributes={
            "gen_ai.system": "ollama",
            "gen_ai.request.model": MODEL_NAME,
            "gen_ai.request.temperature": 0,
        },
    ) as inference:
        response = model.bind_tools(TOOLS).invoke(messages)
        usage = getattr(response, "usage_metadata", None) or {}
        if usage.get("input_tokens"):
            inference.set_attribute("gen_ai.usage.input_tokens", usage["input_tokens"])
        if usage.get("output_tokens"):
            inference.set_attribute("gen_ai.usage.output_tokens", usage["output_tokens"])

    calls = getattr(response, "tool_calls", None) or []
    kind = "select_tool" if calls else "respond"
    basis = _sanitize(_content_preview(response, calls))
    iteration = sum(1 for msg in messages if getattr(msg, "tool_calls", None) is not None and msg.type == "ai")
    with decision_span(new_id("dec"), kind, basis, "tool_call" if calls else "final_answer", iteration):
        pass
    m.record_decision(kind)
    return {"messages": [response]}


def _content_preview(message: AnyMessage, calls: list) -> str:
    if calls:
        parts = []
        for c in calls:
            args = ", ".join(f"{k}={v}" for k, v in (c.get("args") or {}).items())
            parts.append(f"{c['name']}({args})")
        return "next: " + "; ".join(parts)
    content = getattr(message, "content", "") or ""
    if isinstance(content, list):
        content = " ".join(str(part) for part in content)
    return f"answer: {content}"


def _tools_node(state: AgentState) -> dict:
    last = state["messages"][-1]
    charges = dict(state.get("charges") or {})
    outputs = []
    for call in getattr(last, "tool_calls", None) or []:
        name = call["name"]
        label = _TOOL_LABEL.get(name, name)
        effect = _TOOL_EFFECT.get(name, "read_only")
        idempotency_key = f"charge:{_current_order.get('') or 'unassigned'}" if name == "charge_card" else None
        with tool_span(new_id("call"), label, effect, idempotency_key=idempotency_key) as span:
            span.set_attribute("gen_ai.tool.name", name)
            _set_tool_attrs(span, name, call.get("args", {}))
            try:
                tool_impl = next(t for t in TOOLS if t.name == name)
                raw = tool_impl.invoke(call)
                if isinstance(raw, ToolMessage):
                    raw = raw.content
                payload = json.loads(raw if isinstance(raw, str) else json.dumps(raw))
            except Exception as exc:
                payload = {"status": "error", "reason": str(exc), "stage": "tool_execution"}
            _apply_tool_outcome(span, name, payload)
            if isinstance(payload, dict) and payload.get("deduplicated"):
                dedup_status = "duplicate"
            elif isinstance(payload, dict) and payload.get("status") in ("declined", "error"):
                dedup_status = "not_attempted"
            else:
                dedup_status = "unique"
            span.set_attribute("agent.tool.deduplication.status", dedup_status)
            m.record_tool_call(label, effect, dedup_status)
            if name == "charge_card" and isinstance(payload, dict):
                charges[payload.get("status", "error")] = payload
                if payload.get("status") == "approved":
                    with effect_span(new_id("eff"), f"charge:{payload.get('order_id', '')}", "create",
                                     confirmation_status="confirmed", compensation_available=True):
                        pass
            outputs.append(ToolMessage(content=json.dumps(payload), tool_call_id=call["id"], name=name))
    return {"messages": outputs, "charges": charges}


def _route(state: AgentState) -> str:
    return "tools" if getattr(state["messages"][-1], "tool_calls", None) else END


def _build_graph(checkpointer: OtelCheckpointer):
    graph = StateGraph(AgentState)
    graph.add_node("agent", _agent_node)
    graph.add_node("tools", _tools_node)
    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", _route, {"tools": "tools", END: END})
    graph.add_edge("tools", "agent")
    return graph.compile(checkpointer=checkpointer)


def _set_tool_attrs(span: Any, name: str, args: dict) -> None:
    if name == "search_products":
        span.set_attribute("purchase.product.query", str(args.get("query", ""))[:200])
    elif name == "charge_card":
        pan = re.sub(r"\D", "", str(args.get("card_number", "")))
        span.set_attribute("purchase.product.id", str(args.get("product_id", "")))
        span.set_attribute("purchase.quantity", int(args.get("quantity", 1) or 1))
        span.set_attribute("purchase.payment.last4", pan[-4:])
        span.set_attribute("purchase.payment.brand", _detect_brand(pan))


def _apply_tool_outcome(span: Any, name: str, payload: Any) -> None:
    if not isinstance(payload, dict):
        return
    status = payload.get("status")
    if status == "declined":
        span.set_attribute("purchase.payment.status", "declined")
        span.set_attribute("payment.failure.reason", payload.get("reason", "unknown"))
        span.set_attribute("payment.failure.stage", payload.get("stage", "unknown"))
        exc = RuntimeError(f"declined: {payload.get('reason')} at {payload.get('stage')}")
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, f"declined: {payload.get('reason')}"))
    elif status == "approved":
        span.set_attribute("purchase.payment.status", "approved")
        span.set_attribute("purchase.amount", payload.get("amount", 0.0))
        span.set_attribute("purchase.product.name", str(payload.get("product", "")))
        span.set_attribute("purchase.payment.method", "credit_card")
        span.set_attribute("payment.id", str(payload.get("payment_id", "")))


def _psycopg_connect(dsn: str):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row)


def _text(message: AnyMessage) -> str:
    content = getattr(message, "content", "") or ""
    if isinstance(content, list):
        content = " ".join(str(part) for part in content)
    return content


def _ui_events_from_update(update: dict) -> Iterator[dict]:
    for node, payload in update.items():
        for message in (payload or {}).get("messages", []):
            if node == "agent":
                calls = getattr(message, "tool_calls", None) or []
                for c in calls:
                    args = ", ".join(f"{k}={v}" for k, v in (c.get("args") or {}).items())
                    yield {"type": "decision", "kind": "select_tool",
                           "basis": _sanitize(f"{c['name']}({args})"), "outcome": "tool_call"}
            elif node == "tools":
                try:
                    data = json.loads(message.content)
                except Exception:
                    data = {"status": "unknown"}
                label = _TOOL_LABEL.get(message.name, message.name)
                if isinstance(data, list):
                    yield {"type": "tool", "name": label, "status": "ok",
                           "detail": f"{len(data)} candidate(s) found"}
                elif data.get("status") == "declined":
                    yield {"type": "tool", "name": label, "status": "error",
                           "detail": f"declined by the gateway: {data.get('reason')} (stage: {data.get('stage')}, code {data.get('gateway_code', 'XX')})"}
                elif data.get("status") == "approved":
                    yield {"type": "tool", "name": label, "status": "ok",
                           "detail": f"approved: {data.get('payment_id')} auth {data.get('auth_code')} — ${data.get('amount')} to {data.get('card_brand')} ****{data.get('card_last4')}"}
                    yield {"type": "effect", "resource": f"charge:{data.get('order_id', '')}",
                           "status": "confirmed", "detail": "mutable_write with compensation available (refund)"}
                elif data.get("status") == "error":
                    yield {"type": "tool", "name": label, "status": "error",
                           "detail": f"tool execution failed: {data.get('reason')}"}
                elif data.get("name"):
                    yield {"type": "tool", "name": label, "status": "ok",
                           "detail": f"{data.get('name')} — ${data.get('price')} (stock {data.get('stock')})"}


def run_purchase_agent(prompt: str, thread_id: str) -> Iterator[dict]:
    order_id = f"order-{int(time.time() * 1000)}"
    dsn = os.environ.get("OAAR_POSTGRES_DSN", "postgresql://oaar:oaar@postgres:5432/oaar")
    conn = _psycopg_connect(dsn)
    saver = OtelCheckpointer(conn)
    saver.setup()
    try:
        with correlation.operation(f"purchase:{order_id}"):
            with durable_run(thread_id, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger) as run:
                _current_order.set(order_id)
                trace_id = format(run.span_context.trace_id, "032x")
                yield {"type": "run", "run_id": run.run_id, "trace_id": trace_id, "order_id": order_id}

                graph = _build_graph(saver)
                config = {"configurable": {"thread_id": thread_id}, "recursion_limit": MAX_STEPS}
                final_content = ""
                charges: dict = {}
                for update in graph.stream(
                    {"messages": [("system", SYSTEM_PROMPT), ("user", prompt)], "charges": {}},
                    config, stream_mode="updates",
                ):
                    for event in _ui_events_from_update(update):
                        yield event
                    charges.update((update.get("tools") or {}).get("charges") or {})
                    for payload in (update or {}).values():
                        for message in (payload or {}).get("messages", []):
                            if not getattr(message, "tool_calls", None) and _text(message):
                                final_content = _text(message)

                if charges.get("approved"):
                    outcome = "accepted"
                elif charges.get("declined"):
                    outcome = "declined"
                else:
                    outcome = "pending"
                run.artifact_outcome = _ARTIFACT_OUTCOME[outcome]

                yield {"type": "final", "outcome": outcome, "order_id": order_id,
                       "reason": (charges.get("declined") or {}).get("reason", ""),
                       "stage": (charges.get("declined") or {}).get("stage", ""),
                       "summary": final_content or "No answer produced."}
                log_with_context(logger, logging.INFO,
                                 f"purchase {order_id} {outcome}: {_sanitize(final_content)[:180]}",
                                 run_id=run.run_id, thread_id=thread_id, category="purchase")
                if outcome in ("accepted", "declined"):
                    emit_outcome(
                        f"purchase:{order_id}",
                        "payment.authorization",
                        "1.0.0",
                        outcome,
                        registry=OUTCOME_REGISTRY,
                        count_metric=True,
                    )
    finally:
        conn.close()
