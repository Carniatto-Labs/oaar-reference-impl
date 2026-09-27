import json
import logging
import os
import time
import urllib.parse

import psycopg
from psycopg.rows import dict_row
from typing_extensions import TypedDict

from langgraph.graph import END, START, StateGraph

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.checkpointer_otel import OtelCheckpointer
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.outcomes import emit_outcome
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import (
    decision_span,
    effect_span,
    link,
    log_with_context,
    new_id,
)
from oaar_impl.verify_pipeline import http_get

WORKFLOW_NAME = "scenario.abc_checkpoint"
WORKFLOW_VERSION = "0.1.0"
PROMETHEUS = "http://prometheus:9090"
ACCEPTANCE_DEFINITION = "customer_acceptance"
ACCEPTANCE_DEFINITION_VERSION = "v1"
OPERATION_P = "proposal:prop-001"
OPERATION_Q = "proposal:prop-002"


class GatewayCrash(RuntimeError):
    pass


class ScenarioState(TypedDict, total=False):
    proposal_id: str
    fail_in: str
    stage: str


def prom_runs_by_outcome(workflow_name: str) -> dict:
    query = urllib.parse.quote(
        f'sum by (agent_run_outcome) (oaar_runs_total{{agent_runtime_workflow_name="{workflow_name}"}})'
    )
    status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
    if status != 200:
        return {}
    data = json.loads(body)
    out = {}
    for result in data.get("data", {}).get("result", []):
        outcome = result.get("metric", {}).get("agent_run_outcome")
        if outcome:
            out[outcome] = float(result["value"][1])
    return out


def _prepare(state):
    with decision_span(new_id("dec"), "plan", "deterministic_script", "proceed", 1):
        m.record_decision("plan")
    return {"stage": "prepared"}


def _deliver(state):
    if state.get("fail_in") == "deliver":
        raise GatewayCrash("simulated gateway crash before artifact production")
    with effect_span(
        new_id("eff"),
        f"proposal:{state['proposal_id']}",
        "create",
        confirmation_status="confirmed",
        compensation_available=True,
    ):
        pass
    return {"stage": "delivered"}


def build_graph(checkpointer):
    builder = StateGraph(ScenarioState)
    builder.add_node("prepare", _prepare)
    builder.add_node("deliver", _deliver)
    builder.add_edge(START, "prepare")
    builder.add_edge("prepare", "deliver")
    builder.add_edge("deliver", END)
    return builder.compile(checkpointer=checkpointer)


def _resolve(handle, proposal_id: str, value: str) -> str:
    with decision_span(
        new_id("dec"),
        "respond",
        "customer_response",
        value,
        1,
        links=[link(handle.span_context, "triggered_by")],
    ) as span:
        m.record_decision("respond")
        emit_outcome(
            f"proposal:{proposal_id}",
            ACCEPTANCE_DEFINITION,
            ACCEPTANCE_DEFINITION_VERSION,
            value,
        )
        return format(span.get_span_context().trace_id, "032x")


def main(q_outcome: str = "accepted", context_path: str | None = None) -> int:
    configure_telemetry()
    logger = logging.getLogger("oaar.abc")
    context_path = context_path or os.environ.get(
        "OAAR_ABC_CONTEXT_PATH", "/tmp/oaar-abc-context.json"
    )
    dsn = os.environ.get(
        "OAAR_POSTGRES_DSN", "postgresql://oaar:oaar@postgres:5432/oaar"
    )
    conn = psycopg.connect(
        dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row
    )
    saver = OtelCheckpointer(conn)
    saver.setup()
    graph = build_graph(saver)

    baseline = prom_runs_by_outcome(WORKFLOW_NAME)
    thread_p = f"thread-abc-p-{int(time.time())}"
    thread_q = f"thread-abc-q-{int(time.time())}"
    config_p = {"configurable": {"thread_id": thread_p}}
    config_q = {"configurable": {"thread_id": thread_q}}

    # Run A: checkpoints, then explicitly fails before producing the artifact.
    # The GatewayCrash must CROSS durable_run's boundary (adapter marks the run
    # failed with logical outcome pending); the scenario catches it outside.
    with correlation.operation(OPERATION_P):
        try:
            with durable_run(
                thread_p, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
            ) as run_a:
                graph.invoke(
                    {
                        "proposal_id": "prop-001",
                        "fail_in": "deliver",
                        "stage": "new",
                    },
                    config_p,
                )
        except GatewayCrash:
            pass
    last_checkpoint_a = next(
        (
            c["checkpoint_id"]
            for c in reversed(saver.saved_checkpoints)
            if c["thread_id"] == thread_p
        ),
        None,
    )

    # Run B: new invocation restores A's checkpoint, produces P, completes
    with correlation.operation(OPERATION_P):
        with durable_run(
            thread_p,
            WORKFLOW_NAME,
            WORKFLOW_VERSION,
            resume_from=run_a,
            logger=logger,
        ) as run_b:
            graph.invoke({"fail_in": "", "stage": "restored"}, config_p)
            run_b.logical_outcome = "completed"

    # Run C: fresh thread, produces Q, completes
    with correlation.operation(OPERATION_Q):
        with durable_run(
            thread_q, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
        ) as run_c:
            graph.invoke(
                {"proposal_id": "prop-002", "fail_in": "", "stage": "new"}, config_q
            )
            run_c.logical_outcome = "completed"

    if run_a.outcome != "failed" or run_b.outcome != "completed" or run_c.outcome != "completed":
        print(
            f"DEMO ERROR: unexpected outcomes A={run_a.outcome} B={run_b.outcome} C={run_c.outcome}",
            file=__import__("sys").stderr,
        )
        conn.close()
        shutdown_telemetry()
        return 1

    # Business results arrive AFTER all runs have ended (three-level outcomes)
    resolution_p_trace = _resolve(run_b, "prop-001", "declined")
    resolution_q_trace = _resolve(run_c, "prop-002", q_outcome)

    conn.close()
    context = {
        "workflow_name": WORKFLOW_NAME,
        "q_outcome": q_outcome,
        "thread_p": thread_p,
        "thread_q": thread_q,
        "run_a": run_a.run_id,
        "run_b": run_b.run_id,
        "run_c": run_c.run_id,
        "trace_a": format(run_a.span_context.trace_id, "032x"),
        "trace_b": format(run_b.span_context.trace_id, "032x"),
        "trace_c": format(run_c.span_context.trace_id, "032x"),
        "resolution_p_trace": resolution_p_trace,
        "resolution_q_trace": resolution_q_trace,
        "operation_p": OPERATION_P,
        "operation_q": OPERATION_Q,
        "last_checkpoint_a": last_checkpoint_a,
        "baseline": baseline,
    }
    with open(context_path, "w") as f:
        json.dump(context, f, indent=2)
    log_with_context(
        logger,
        logging.INFO,
        (
            f"abc scenario done A={run_a.outcome} B={run_b.outcome} C={run_c.outcome} "
            f"q_outcome={q_outcome}"
        ),
        run_id=run_c.run_id,
        thread_id=thread_q,
        category="demo",
    )
    print(f"abc context written to {context_path}")
    print(json.dumps(context, indent=2))
    shutdown_telemetry()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
