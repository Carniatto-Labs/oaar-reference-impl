import json
import logging
import os
import time
from types import SimpleNamespace

import psycopg
from psycopg.rows import dict_row
from typing_extensions import TypedDict

from langgraph.graph import END, START, StateGraph

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.checkpointer_otel import OtelCheckpointer
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import (
    decision_span,
    link,
    log_with_context,
    new_id,
)

CHILD_WORKFLOW = "scenario.delegation_child"
PARENT_WORKFLOW = "scenario.delegation_parent"
WORKFLOW_VERSION = "0.1.0"
CONTEXT_PATH = "/tmp/oaar-fanout-context.json"


class WorkState(TypedDict, total=False):
    task: str
    stage: str


def work(state):
    with decision_span(new_id("dec"), "plan", "child_script", "proceed", 1):
        m.record_decision("plan")
    return {"stage": "done"}


def build_graph(checkpointer):
    builder = StateGraph(WorkState)
    builder.add_node("work", work)
    builder.add_edge(START, "work")
    builder.add_edge("work", END)
    return builder.compile(checkpointer=checkpointer)


def main() -> int:
    configure_telemetry()
    logger = logging.getLogger("oaar.fanout")
    dsn = os.environ.get(
        "OAAR_POSTGRES_DSN", "postgresql://oaar:oaar@postgres:5432/oaar"
    )
    conn = psycopg.connect(
        dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row
    )
    saver = OtelCheckpointer(conn)
    saver.setup()
    graph = build_graph(saver)
    ts = int(time.time())
    task_id = f"t-{ts}"
    parent_run_id = new_id("run")

    children = []
    for child_name in ("research", "drafting"):
        thread_c = f"thread-fan-{child_name}-{ts}"
        with correlation.operation(f"task:{task_id}:{child_name}"):
            with durable_run(
                thread_c,
                CHILD_WORKFLOW,
                WORKFLOW_VERSION,
                parent=SimpleNamespace(run_id=parent_run_id),
                logger=logger,
            ) as child:
                graph.invoke({"task": child_name, "stage": "new"}, {"configurable": {"thread_id": thread_c}})
        children.append(child)

    parent_thread = f"thread-fan-parent-{ts}"
    links = []
    for child in children:
        links.append(link(child.span_context, "delegates_to"))
        links.append(link(child.span_context, "aggregates"))
    with correlation.operation(f"task:{task_id}"):
        with durable_run(
            parent_thread,
            PARENT_WORKFLOW,
            WORKFLOW_VERSION,
            run_id=parent_run_id,
            links=links,
            logger=logger,
        ) as parent:
            with decision_span(new_id("dec"), "delegate", "fanout", "aggregated", 1):
                m.record_decision("delegate")

    conn.close()
    context = {
        "parent_run_id": parent_run_id,
        "parent_trace": format(parent.span_context.trace_id, "032x"),
        "child_runs": [c.run_id for c in children],
        "child_traces": [format(c.span_context.trace_id, "032x") for c in children],
    }
    with open(CONTEXT_PATH, "w") as f:
        json.dump(context, f, indent=2)
    log_with_context(
        logger,
        logging.INFO,
        f"fanout scenario done parent={parent.run_id} children={len(children)}",
        run_id=parent.run_id,
        thread_id=parent_thread,
        category="demo",
    )
    print(f"fanout context written to {CONTEXT_PATH}")
    print(json.dumps(context, indent=2))
    shutdown_telemetry()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
