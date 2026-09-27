import json
import logging
import time

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.run_adapter import cancel_run, durable_run
from oaar_impl.spans import decision_span, guardrail_span, log_with_context, new_id

WORKFLOW_NAME = "scenario.guardrail_block"
WORKFLOW_VERSION = "0.1.0"
CONTEXT_PATH = "/tmp/oaar-guardrail-context.json"

# Sensitive content that must NEVER reach telemetry. The guardrail blocks the
# request before anything could log or trace it; if the implementation were
# careless this string would leak into traces or logs.
BAIT_CONTENT = "cpf 123.456.789-00 email vitima@exemplo.com"
POLICY_ID = "policy-pii-block"
POLICY_VERSION = "1.3.0"


def main() -> int:
    configure_telemetry()
    logger = logging.getLogger("oaar.guardrail")
    thread_id = f"thread-guard-{int(time.time())}"

    with correlation.operation("task:g-001"):
        with durable_run(
            thread_id, WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
        ) as handle:
            with guardrail_span(POLICY_ID, POLICY_VERSION, "blocked", "pii"):
                record_evaluation(POLICY_ID, POLICY_VERSION)
            with decision_span(
                new_id("dec"), "terminate", f"guardrail:{POLICY_ID}", "blocked", 1
            ):
                m.record_decision("terminate")
            log_with_context(
                logger,
                logging.INFO,
                "request blocked by guardrail before processing",
                run_id=handle.run_id,
                thread_id=thread_id,
                category="guardrail",
            )
            cancel_run(handle, "guardrail blocked")

    context = {
        "workflow_name": WORKFLOW_NAME,
        "run": handle.run_id,
        "trace": format(handle.span_context.trace_id, "032x"),
        "policy_id": POLICY_ID,
        "policy_version": POLICY_VERSION,
        "bait": BAIT_CONTENT,
    }
    with open(CONTEXT_PATH, "w") as f:
        json.dump(context, f, indent=2)
    print(f"guardrail context written to {CONTEXT_PATH}")
    print(json.dumps(context, indent=2))
    shutdown_telemetry()
    return 0


def record_evaluation(policy_id, policy_version):
    m.record_guardrail("blocked")


if __name__ == "__main__":
    raise SystemExit(main())
