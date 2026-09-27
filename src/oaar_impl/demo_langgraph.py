import json
import logging
import os
import time

import psycopg
from psycopg.rows import dict_row

from langgraph.graph import END, START, StateGraph

from oaar_impl.checkpointer_otel import OtelCheckpointer
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import log_with_context

CONTEXT_PATH = "/tmp/oaar-langgraph-context.json"


class DemoState(dict):
    pass


def _plan(state):
    return {"stage": "planning", "turns": (state.get("turns") or 0) + 1}


def _execute(state):
    return {"stage": "executed", "turns": (state.get("turns") or 0) + 1}


def build_graph(checkpointer):
    builder = StateGraph(DemoState)
    builder.add_node("plan", _plan)
    builder.add_node("execute", _execute)
    builder.add_edge(START, "plan")
    builder.add_edge("plan", "execute")
    builder.add_edge("execute", END)
    return builder.compile(checkpointer=checkpointer)


def main() -> int:
    configure_telemetry()
    logger = logging.getLogger("oaar.langgraph")
    dsn = os.environ.get(
        "OAAR_POSTGRES_DSN", "postgresql://oaar:oaar@postgres:5432/oaar"
    )
    conn = psycopg.connect(dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row)
    saver = OtelCheckpointer(conn)
    saver.setup()
    graph = build_graph(saver)
    thread_id = f"thread-lg-{int(time.time())}"
    config = {"configurable": {"thread_id": thread_id}}

    with durable_run(
        thread_id, "demo.langgraph", "0.1.0", logger=logger
    ) as handle_a:
        graph.invoke({"stage": "initialized", "turns": 0}, config=config)

    saved_a = [c for c in saver.saved_checkpoints if c["thread_id"] == thread_id]
    if not saved_a:
        print("DEMO ERROR: run A produced no checkpoints", file=__import__("sys").stderr)
        conn.close()
        shutdown_telemetry()
        return 1
    last_checkpoint_a = saved_a[-1]["checkpoint_id"]

    with durable_run(
        thread_id,
        "demo.langgraph",
        "0.1.0",
        resume_from=handle_a,
        logger=logger,
    ) as handle_b:
        graph.invoke({"stage": "initialized", "turns": 0}, config=config)

    restored_b = [c for c in saver.restored_checkpoints if c["thread_id"] == thread_id]
    if not restored_b:
        print("DEMO ERROR: run B did not restore any checkpoint", file=__import__("sys").stderr)
        conn.close()
        shutdown_telemetry()
        return 1

    conn.close()
    context = {
        "thread_id": thread_id,
        "run_a": handle_a.run_id,
        "run_b": handle_b.run_id,
        "trace_a": format(handle_a.span_context.trace_id, "032x"),
        "trace_b": format(handle_b.span_context.trace_id, "032x"),
        "last_checkpoint_a": last_checkpoint_a,
        "saves_a": [
            {"checkpoint_id": c["checkpoint_id"], "state_version": c["state_version"]}
            for c in saved_a
        ],
        "restores_b": [
            {"checkpoint_id": c["checkpoint_id"], "state_version": c["state_version"]}
            for c in restored_b
        ],
    }
    with open(CONTEXT_PATH, "w") as f:
        json.dump(context, f, indent=2)
    log_with_context(
        logger,
        logging.INFO,
        f"langgraph demo done thread={thread_id} trace_a={context['trace_a']} trace_b={context['trace_b']}",
        run_id=handle_b.run_id,
        thread_id=thread_id,
        category="demo",
    )
    print(f"demo context written to {CONTEXT_PATH}")
    print(json.dumps(context, indent=2))
    shutdown_telemetry()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
