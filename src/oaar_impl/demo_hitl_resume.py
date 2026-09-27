import json
import logging
import os
import time

import psycopg
from psycopg.rows import dict_row
from typing_extensions import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from oaar_impl import correlation
from oaar_impl.checkpointer_otel import OtelCheckpointer
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import emit_lifecycle, log_with_context

WORKFLOW_NAME = "scenario.hitl_resume"
WORKFLOW_VERSION = "0.1.0"
CONTEXT_PATH = "/tmp/oaar-hitl-context.json"
HUMAN_THINK_SECONDS = 2.0


class ApprovalState(TypedDict, total=False):
    request: str
    approved: bool
    stage: str


def ask_approval(state):
    answer = interrupt({"prompt": "approve the request?"})
    return {"approved": bool(answer.get("approved")), "stage": "approved"}


def act(state):
    return {"stage": "acted"}


def build_graph(checkpointer):
    builder = StateGraph(ApprovalState)
    builder.add_node("ask_approval", ask_approval)
    builder.add_node("act", act)
    builder.add_edge(START, "ask_approval")
    builder.add_edge("ask_approval", "act")
    builder.add_edge("act", END)
    return builder.compile(checkpointer=checkpointer)


def main() -> int:
    configure_telemetry()
    logger = logging.getLogger("oaar.hitl")
    dsn = os.environ.get(
        "OAAR_POSTGRES_DSN", "postgresql://oaar:oaar@postgres:5432/oaar"
    )
    conn = psycopg.connect(
        dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row
    )
    saver = OtelCheckpointer(conn)
    saver.setup()
    graph = build_graph(saver)
    thread_id = f"thread-hitl-{int(time.time())}"
    config = {"configurable": {"thread_id": thread_id}}

    with correlation.operation(WORKFLOW_NAME):
        with durable_run(
            thread_id, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
        ) as run_wait:
            result = graph.invoke(
                {"request": "refund-001", "stage": "new"}, config
            )
            if "__interrupt__" not in result:
                print(
                    "DEMO ERROR: expected interrupt, got normal completion",
                    file=__import__("sys").stderr,
                )
                conn.close()
                shutdown_telemetry()
                return 1
            saves = [
                c
                for c in saver.saved_checkpoints
                if c["thread_id"] == thread_id
            ]
            emit_lifecycle(
                "agent.run.waiting",
                reason="human approval required",
                actor="human_gate",
                state_version=saves[-1]["state_version"] if saves else 0,
            )
    last_checkpoint_wait = saves[-1]["checkpoint_id"] if saves else None

    time.sleep(HUMAN_THINK_SECONDS)

    with correlation.operation(WORKFLOW_NAME):
        with durable_run(
            thread_id,
            WORKFLOW_NAME,
            WORKFLOW_VERSION,
            resume_from=run_wait,
            resume_reason="human_approval",
            logger=logger,
        ) as run_resume:
            graph.invoke(Command(resume={"approved": True}), config)

    conn.close()
    context = {
        "workflow_name": WORKFLOW_NAME,
        "thread_id": thread_id,
        "run_wait": run_wait.run_id,
        "run_resume": run_resume.run_id,
        "trace_wait": format(run_wait.span_context.trace_id, "032x"),
        "trace_resume": format(run_resume.span_context.trace_id, "032x"),
        "last_checkpoint_wait": last_checkpoint_wait,
    }
    with open(CONTEXT_PATH, "w") as f:
        json.dump(context, f, indent=2)
    log_with_context(
        logger,
        logging.INFO,
        f"hitl scenario done wait={run_wait.run_id} resume={run_resume.run_id}",
        run_id=run_resume.run_id,
        thread_id=thread_id,
        category="demo",
    )
    print(f"hitl context written to {CONTEXT_PATH}")
    print(json.dumps(context, indent=2))
    shutdown_telemetry()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
