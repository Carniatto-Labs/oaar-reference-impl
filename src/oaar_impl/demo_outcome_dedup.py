"""Business outcomes at the operation level: one outcome per operation, however many runs.

This scenario exercises the failure mode that a Run-level denominator cannot answer on its
own: several runs of the same logical operation each report a business outcome. A naive
counter counts the operation once per attempt and inflates the business rate.

Three operations, four runs, six reported outcomes, three counted outcomes:

  1. purchase:order-dedup-retry     run 1 reports accepted (recorded)
                                    run 2 restores run 1 and reports accepted again
                                    -> duplicate: history keeps it, the counter does not
  2. purchase:order-dedup-pending   pending at the gateway, then accepted
                                    -> pending never counts; accepted is recorded once
  3. purchase:order-dedup-conflict  accepted, then a late "declined" for the same subject
                                    -> conflict: the first terminal value stands, the
                                       conflicting attempt is flagged, never overwritten

Expected deltas: oaar_purchases_total +3, duplicates +1, conflicts +1.
"""

import json
import logging
import time
import urllib.parse

from oaar_impl import correlation
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.outcomes import emit_outcome
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import decision_span, log_with_context, new_id
from oaar_impl.verify_pipeline import http_get

WORKFLOW_NAME = "scenario.outcome_dedup"
WORKFLOW_VERSION = "0.1.0"
PROMETHEUS = "http://prometheus:9090"
DEFINITION = "payment.authorization"
DEFINITION_VERSION = "1.0.0"

OPERATION_RETRY = "purchase:order-dedup-retry"
OPERATION_PENDING = "purchase:order-dedup-pending"
OPERATION_CONFLICT = "purchase:order-dedup-conflict"


def prom_counter(name: str, **labels) -> float:
    """Total of a counter, summed across instances.

    Each runtime process gets its own ``service.instance.id``, so the Prometheus exporter keeps
    one series per process. Reading the first series makes a check depend on scrape order; a
    counter is only meaningful summed.
    """
    selector = ",".join(f'{k}="{v}"' for k, v in labels.items())
    vector = f"{name}{{{selector}}}" if selector else name
    query = urllib.parse.quote(f"sum({vector})")
    status, body = http_get(f"{PROMETHEUS}/api/v1/query?query={query}")
    if status != 200:
        return 0.0
    results = json.loads(body).get("data", {}).get("result", [])
    return float(results[0]["value"][1]) if results else 0.0


def _baseline() -> dict:
    return {
        "purchases_accepted": prom_counter("oaar_purchases_total", agent_purchase_outcome="accepted"),
        "duplicates": prom_counter("oaar_outcome_duplicates_total"),
        "conflicts": prom_counter("oaar_outcome_conflicts_total"),
    }


def main() -> int:
    configure_telemetry()
    logger = logging.getLogger("oaar.dedup")
    baseline = _baseline()
    stamp = int(time.time())
    context = {
        "workflow_name": WORKFLOW_NAME,
        "definition": DEFINITION,
        "baseline": baseline,
    }

    # 1. One operation, two runs: retry after a restore reports the same outcome again.
    thread_retry = f"thread-dedup-retry-{stamp}"
    with correlation.operation(OPERATION_RETRY):
        with durable_run(thread_retry, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger) as run_1:
            with decision_span(new_id("dec"), "select_tool", "charge the card", "tool_call", 1):
                pass
            emit_outcome(OPERATION_RETRY, DEFINITION, DEFINITION_VERSION, "accepted", count_metric=True)
            run_1.artifact_outcome = "produced"

        with durable_run(
            thread_retry, WORKFLOW_NAME, WORKFLOW_VERSION, resume_from=run_1, logger=logger
        ) as run_2:
            emit_outcome(OPERATION_RETRY, DEFINITION, DEFINITION_VERSION, "accepted", count_metric=True)
            run_2.artifact_outcome = "produced"

    # 2. Pending at the reporting cut is not an outcome; the terminal value is recorded once.
    thread_pending = f"thread-dedup-pending-{stamp}"
    with correlation.operation(OPERATION_PENDING):
        with durable_run(thread_pending, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger) as run_3:
            emit_outcome(OPERATION_PENDING, DEFINITION, DEFINITION_VERSION, "pending", count_metric=True)
            emit_outcome(OPERATION_PENDING, DEFINITION, DEFINITION_VERSION, "accepted", count_metric=True)
            run_3.artifact_outcome = "produced"

    # 3. A late conflicting outcome never overwrites the recorded one.
    thread_conflict = f"thread-dedup-conflict-{stamp}"
    with correlation.operation(OPERATION_CONFLICT):
        with durable_run(thread_conflict, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger) as run_4:
            emit_outcome(OPERATION_CONFLICT, DEFINITION, DEFINITION_VERSION, "accepted", count_metric=True)
            emit_outcome(OPERATION_CONFLICT, DEFINITION, DEFINITION_VERSION, "declined", count_metric=True)
            run_4.artifact_outcome = "produced"

    context.update(
        {
            "operations": {
                "retry": OPERATION_RETRY,
                "pending": OPERATION_PENDING,
                "conflict": OPERATION_CONFLICT,
            },
            "trace_run_1": format(run_1.span_context.trace_id, "032x"),
            "trace_run_2": format(run_2.span_context.trace_id, "032x"),
            "trace_run_3": format(run_3.span_context.trace_id, "032x"),
            "trace_run_4": format(run_4.span_context.trace_id, "032x"),
            "run_ids": {
                "run_1": run_1.run_id,
                "run_2": run_2.run_id,
                "run_3": run_3.run_id,
                "run_4": run_4.run_id,
            },
        }
    )
    with open("/tmp/oaar-outcome-dedup-context.json", "w") as f:
        json.dump(context, f, indent=2)

    log_with_context(
        logger,
        logging.INFO,
        "outcome dedup scenario done: 3 operations / 4 runs / 6 observations / 3 counted outcomes",
        run_id=run_4.run_id,
        thread_id=thread_conflict,
        category="demo",
    )
    print(json.dumps(context, indent=2))
    shutdown_telemetry()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
