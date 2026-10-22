import json
import logging
import os
import time

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
    compensate_span,
    effect_span,
    log_with_context,
    new_id,
)

WORKFLOW_NAME = "scenario.compensation_saga"
WORKFLOW_VERSION = "0.1.0"
CONTEXT_PATH = "/tmp/oaar-saga-context.json"

SAGA = {}


class HotelUnavailable(RuntimeError):
    pass


class BookingState(TypedDict, total=False):
    booking_id: str
    stage: str


def book_flight(state):
    effect_id = new_id("eff")
    with effect_span(
        effect_id,
        f"flight:{state['booking_id']}",
        "create",
        confirmation_status="confirmed",
        attempt_id=new_id("attempt"),
        compensation_available=True,
    ) as span:
        SAGA["flight_effect_id"] = effect_id
        SAGA["flight_effect_context"] = span.get_span_context()
    return {"stage": "flight_booked"}


def book_hotel(state):
    raise HotelUnavailable("no rooms available")


def build_graph(checkpointer):
    builder = StateGraph(BookingState)
    builder.add_node("book_flight", book_flight)
    builder.add_node("book_hotel", book_hotel)
    builder.add_edge(START, "book_flight")
    builder.add_edge("book_flight", "book_hotel")
    builder.add_edge("book_hotel", END)
    return builder.compile(checkpointer=checkpointer)


def main() -> int:
    configure_telemetry()
    logger = logging.getLogger("oaar.saga")
    dsn = os.environ.get(
        "OAAR_POSTGRES_DSN", "postgresql://oaar:oaar@postgres:5432/oaar"
    )
    conn = psycopg.connect(
        dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row
    )
    saver = OtelCheckpointer(conn)
    saver.setup()
    graph = build_graph(saver)
    booking_id = f"bk-{int(time.time())}"
    thread_id = f"thread-saga-{int(time.time())}"
    config = {"configurable": {"thread_id": thread_id}}
    operation_id = f"booking:{booking_id}"

    with correlation.operation(operation_id):
        try:
            with durable_run(
                thread_id, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
            ) as run:
                graph.invoke({"booking_id": booking_id, "stage": "new"}, config)
        except HotelUnavailable:
            pass

    # Saga compensation runs after the run failed; outcome is partial because
    # the seat was released but the issue fee is retained.
    with correlation.operation(operation_id):
        with compensate_span(
            SAGA["flight_effect_id"], SAGA["flight_effect_context"], "partial"
        ) as span:
            m.record_compensation("partial")
            saga_trace = format(span.get_span_context().trace_id, "032x")

    conn.close()
    context = {
        "workflow_name": WORKFLOW_NAME,
        "thread_id": thread_id,
        "operation_id": operation_id,
        "run": run.run_id,
        "trace_run": format(run.span_context.trace_id, "032x"),
        "saga_trace": saga_trace,
        "flight_effect_id": SAGA["flight_effect_id"],
    }
    with open(CONTEXT_PATH, "w") as f:
        json.dump(context, f, indent=2)
    log_with_context(
        logger,
        logging.INFO,
        f"saga scenario done run={run.run_id} compensation=partial",
        run_id=run.run_id,
        thread_id=thread_id,
        category="demo",
    )
    print(f"saga context written to {CONTEXT_PATH}")
    print(json.dumps(context, indent=2))
    shutdown_telemetry()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
