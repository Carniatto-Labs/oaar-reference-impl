"""Effect evidence: dispatch, acknowledgement, and the authoritative readback that resolves it.

Five effects, one per case from the lost-ACK / readback family:

  1. confirmed          - dispatch, ACK lost, fresh authoritative readback bound to the attempt
                          confirms the effect; a retry shares the logical effect id with a new
                          attempt id, and the fresh readback becomes the next decision origin
  2. contradicted       - fresh authoritative readback says the effect did NOT happen; the
                          runtime compensates and only records the compensation once the
                          readback verifies it
  3. stale              - a readback exists and is authoritative, but it is stale: the effect
                          stays unresolved
  4. missing            - no readback at all: unresolved, never inferred from transport. A
                          compensation is requested and never verified
  5. estimated          - a runtime estimate claims CONFIRMED, but the source is not authorized
                          for that effect domain: unresolved

Expected: 1 confirmed, 1 unconfirmed, 3 unresolved at the reporting cut; one compensation
verified and one unverified.
"""

import json
import logging
import time

from oaar_impl import correlation
from oaar_impl import metrics as m
from oaar_impl.demo_outcome_dedup import prom_counter
from oaar_impl.effect_evidence import (
    CONFIRMED,
    Observation,
    TrustRecord,
    TrustedEvidenceRegistry,
    resolve,
)
from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.run_adapter import durable_run
from oaar_impl.spans import (
    ATTR_EFFECT_CONFIRMATION,
    ATTR_EFFECT_DISPATCH_ACK,
    ATTR_EFFECT_EVIDENCE_ID,
    ATTR_EFFECT_ID,
    ATTR_EFFECT_RESOLUTION_REASON,
    ATTR_COMPENSATION_STATE,
    ATTR_EFFECT_ATTEMPT_ID,
    compensate_span,
    decision_span,
    effect_observe_span,
    effect_span,
    emit_effect_event,
    log_with_context,
    new_id,
)

WORKFLOW_NAME = "scenario.effect_unresolved"
WORKFLOW_VERSION = "0.1.0"
CONTEXT_PATH = "/tmp/oaar-effect-unresolved-context.json"
FRESHNESS_MS = 30_000

DOMAIN = "durable-record"
OBSERVATION_KIND = "durable_state"
AUTHORITATIVE_SOURCE = "primary-store-readback"

logger = logging.getLogger("oaar.effect")


def build_registry() -> TrustedEvidenceRegistry:
    """The trusted evidence registry plus the source policy for each domain.

    Only ``primary-store-readback`` may speak about durable record state. The estimator is not
    in the policy, so its CONFIRMED verdict cannot resolve anything.
    """
    registry = TrustedEvidenceRegistry(
        policy={(DOMAIN, OBSERVATION_KIND): {AUTHORITATIVE_SOURCE}}
    )
    return registry


def _baseline() -> dict:
    return {
        "resolutions": {
            status: prom_counter(
                "oaar_effect_resolutions_total",
                agent_effect_confirmation_status=status,
            )
            for status in ("confirmed", "unconfirmed", "unresolved")
        },
        "compensations": {
            state: prom_counter(
                "oaar_compensations_total", agent_compensation_state=state
            )
            for state in ("requested", "unverified", "verified")
        },
    }


def dispatch(effect_id: str, resource: str, attempt_id: str, index: int, ack: str):
    """One dispatch attempt of a logical effect. The span starts unresolved, always."""
    with effect_span(
        effect_id,
        resource,
        "create",
        confirmation_status="unresolved",
        attempt_id=attempt_id,
        attempt_index=index,
        evidence_kind="transport_ack",
        compensation_available=True,
    ) as span:
        span.set_attribute(ATTR_EFFECT_DISPATCH_ACK, ack)
        emit_effect_event(
            "agent.effect.dispatched",
            {
                ATTR_EFFECT_ID: effect_id,
                ATTR_EFFECT_ATTEMPT_ID: attempt_id,
                ATTR_EFFECT_DISPATCH_ACK: ack,
            },
        )
        return span.get_span_context()


def observe(effect_id: str, attempt_id: str, obs: Observation):
    with effect_observe_span(
        effect_id,
        attempt_id,
        evidence_id=obs.evidence_id,
        evidence_digest=obs.evidence_digest,
        source_id=obs.source_id,
        observation_kind=obs.observation_kind,
        verdict=obs.verdict,
        age_ms=obs.age_ms,
        fresh=obs.age_ms <= FRESHNESS_MS,
    ):
        pass


def record_resolution(run, effect_id: str, attempt_id: str, resolution, observed: bool):
    """The logical effect's status, recorded once, wherever the runtime stands at the cut."""
    emit_effect_event(
        "agent.effect.resolved",
        {
            ATTR_EFFECT_ID: effect_id,
            ATTR_EFFECT_ATTEMPT_ID: attempt_id,
            ATTR_EFFECT_CONFIRMATION: resolution.status,
            ATTR_EFFECT_RESOLUTION_REASON: resolution.reason,
            ATTR_EFFECT_EVIDENCE_ID: resolution.evidence_id or "",
        },
    )
    m.record_effect_resolution(resolution.status)
    log_with_context(
        logger,
        logging.INFO,
        f"effect {effect_id} attempt {attempt_id}: {resolution.status} ({resolution.reason})"
        + ("" if observed else ", no observation"),
        run_id=run.run_id,
        thread_id=run.thread_id,
        category="effect",
    )


def case_confirmed(registry, stamp, context):
    """ACK lost -> retry -> fresh authoritative readback confirms."""
    effect_id = f"effect-confirmed-{stamp}"
    attempt_1 = new_id("attempt")
    attempt_2 = new_id("attempt")
    with correlation.operation(f"effect:{effect_id}"):
        with durable_run(
            f"thread-effect-confirmed-{stamp}", WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
        ) as run:
            dispatch(effect_id, f"record:{effect_id}", attempt_1, 1, "lost")
            dispatch(effect_id, f"record:{effect_id}", attempt_2, 2, "acknowledged")
            obs = Observation(
                logical_effect_id=effect_id,
                attempt_id=attempt_2,
                evidence_id=f"evidence-{effect_id}",
                evidence_digest="sha256:confirmed",
                effect_domain=DOMAIN,
                source_id=AUTHORITATIVE_SOURCE,
                observation_kind=OBSERVATION_KIND,
                verdict="CONFIRMED",
                age_ms=120,
            )
            registry.register(
                TrustRecord(
                    effect_id,
                    attempt_2,
                    obs.evidence_id,
                    obs.evidence_digest,
                    verifier_id="store-attestor",
                )
            )
            observe(effect_id, attempt_2, obs)
            resolution = resolve(
                obs, registry, logical_effect_id=effect_id, attempt_id=attempt_2
            )
            record_resolution(run, effect_id, attempt_2, resolution, observed=True)
            # the fresh authoritative state is the origin of the next decision (AEA-009)
            with decision_span(
                new_id("dec"),
                "select_tool",
                f"readback {obs.evidence_id} says record exists; proceed",
                "proceed",
                2,
            ):
                pass
            run.artifact_outcome = "produced"
            context["confirmed"] = {
                "trace": format(run.span_context.trace_id, "032x"),
                "run_id": run.run_id,
                "effect_id": effect_id,
                "attempts": [attempt_1, attempt_2],
                "status": resolution.status,
                "reason": resolution.reason,
            }


def case_contradicted(registry, stamp, context):
    """Fresh readback contradicts -> compensate -> verify the compensation."""
    effect_id = f"effect-contradicted-{stamp}"
    attempt = new_id("attempt")
    with correlation.operation(f"effect:{effect_id}"):
        with durable_run(
            f"thread-effect-contradicted-{stamp}", WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
        ) as run:
            effect_ctx = dispatch(effect_id, f"record:{effect_id}", attempt, 1, "lost")
            obs = Observation(
                logical_effect_id=effect_id,
                attempt_id=attempt,
                evidence_id=f"evidence-{effect_id}",
                evidence_digest="sha256:contradicted",
                effect_domain=DOMAIN,
                source_id=AUTHORITATIVE_SOURCE,
                observation_kind=OBSERVATION_KIND,
                verdict="CONTRADICTED",
                age_ms=200,
            )
            registry.register(
                TrustRecord(
                    effect_id, attempt, obs.evidence_id, obs.evidence_digest, "store-attestor"
                )
            )
            observe(effect_id, attempt, obs)
            resolution = resolve(obs, registry, logical_effect_id=effect_id, attempt_id=attempt)
            record_resolution(run, effect_id, attempt, resolution, observed=True)

            if resolution.status == "contradicted":
                # compensation is a distinct, linked effect; requested first, then verified
                emit_effect_event(
                    "agent.compensation.requested",
                    {ATTR_EFFECT_ID: effect_id, ATTR_COMPENSATION_STATE: "requested"},
                )
                compensate_obs = Observation(
                    logical_effect_id=effect_id,
                    attempt_id=attempt,
                    evidence_id=f"evidence-comp-{effect_id}",
                    evidence_digest="sha256:comp-verified",
                    effect_domain=DOMAIN,
                    source_id=AUTHORITATIVE_SOURCE,
                    observation_kind=OBSERVATION_KIND,
                    verdict="CONFIRMED",
                    age_ms=90,
                )
                registry.register(
                    TrustRecord(
                        effect_id,
                        attempt,
                        compensate_obs.evidence_id,
                        compensate_obs.evidence_digest,
                        "store-attestor",
                    )
                )
                with compensate_span(effect_id, effect_ctx, "succeeded", state="verified"):
                    observe(effect_id, attempt, compensate_obs)
                m.record_compensation("succeeded", "verified")
            run.artifact_outcome = "not_applicable"
            context["contradicted"] = {
                "trace": format(run.span_context.trace_id, "032x"),
                "run_id": run.run_id,
                "effect_id": effect_id,
                "attempt": attempt,
                "status": resolution.status,
            }


def case_stale(registry, stamp, context):
    """Readback exists and is authoritative, but it is stale: unresolved."""
    effect_id = f"effect-stale-{stamp}"
    attempt = new_id("attempt")
    with correlation.operation(f"effect:{effect_id}"):
        with durable_run(
            f"thread-effect-stale-{stamp}", WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
        ) as run:
            dispatch(effect_id, f"record:{effect_id}", attempt, 1, "lost")
            obs = Observation(
                logical_effect_id=effect_id,
                attempt_id=attempt,
                evidence_id=f"evidence-{effect_id}",
                evidence_digest="sha256:stale",
                effect_domain=DOMAIN,
                source_id=AUTHORITATIVE_SOURCE,
                observation_kind=OBSERVATION_KIND,
                verdict="CONFIRMED",
                age_ms=FRESHNESS_MS * 10,
            )
            registry.register(
                TrustRecord(
                    effect_id,
                    attempt,
                    obs.evidence_id,
                    obs.evidence_digest,
                    "store-attestor",
                    fresh=False,
                )
            )
            observe(effect_id, attempt, obs)
            resolution = resolve(obs, registry, logical_effect_id=effect_id, attempt_id=attempt)
            record_resolution(run, effect_id, attempt, resolution, observed=True)
            run.artifact_outcome = "pending"
            context["stale"] = {
                "trace": format(run.span_context.trace_id, "032x"),
                "run_id": run.run_id,
                "effect_id": effect_id,
                "attempt": attempt,
                "status": resolution.status,
                "reason": resolution.reason,
            }


def case_missing(registry, stamp, context):
    """No readback at all, and a requested compensation that never verifies."""
    effect_id = f"effect-missing-{stamp}"
    attempt = new_id("attempt")
    with correlation.operation(f"effect:{effect_id}"):
        with durable_run(
            f"thread-effect-missing-{stamp}", WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
        ) as run:
            effect_ctx = dispatch(effect_id, f"record:{effect_id}", attempt, 1, "lost")
            resolution = resolve(
                None, registry, logical_effect_id=effect_id, attempt_id=attempt
            )
            record_resolution(run, effect_id, attempt, resolution, observed=False)
            emit_effect_event(
                "agent.compensation.requested",
                {ATTR_EFFECT_ID: effect_id, ATTR_COMPENSATION_STATE: "requested"},
            )
            with compensate_span(effect_id, effect_ctx, None, state="unverified"):
                pass
            m.record_compensation(None, "unverified")
            run.artifact_outcome = "pending"
            context["missing"] = {
                "trace": format(run.span_context.trace_id, "032x"),
                "run_id": run.run_id,
                "effect_id": effect_id,
                "attempt": attempt,
                "status": resolution.status,
                "reason": resolution.reason,
            }


def case_estimated(registry, stamp, context):
    """A model estimate claims CONFIRMED; the source is not authorized to say so."""
    effect_id = f"effect-estimated-{stamp}"
    attempt = new_id("attempt")
    with correlation.operation(f"effect:{effect_id}"):
        with durable_run(
            f"thread-effect-estimated-{stamp}", WORKFLOW_NAME, WORKFLOW_VERSION, logger=logger
        ) as run:
            dispatch(effect_id, f"record:{effect_id}", attempt, 1, "lost")
            obs = Observation(
                logical_effect_id=effect_id,
                attempt_id=attempt,
                evidence_id=f"evidence-{effect_id}",
                evidence_digest="sha256:estimate",
                effect_domain=DOMAIN,
                source_id="runtime-estimator",
                observation_kind="model_estimate",
                verdict="CONFIRMED",
                age_ms=50,
            )
            # trusted as a record, but not authorized to speak about durable state
            registry.register(
                TrustRecord(
                    effect_id, attempt, obs.evidence_id, obs.evidence_digest, "estimator"
                )
            )
            observe(effect_id, attempt, obs)
            resolution = resolve(obs, registry, logical_effect_id=effect_id, attempt_id=attempt)
            record_resolution(run, effect_id, attempt, resolution, observed=True)
            run.artifact_outcome = "pending"
            context["estimated"] = {
                "trace": format(run.span_context.trace_id, "032x"),
                "run_id": run.run_id,
                "effect_id": effect_id,
                "attempt": attempt,
                "status": resolution.status,
                "reason": resolution.reason,
            }


def main() -> int:
    configure_telemetry()
    stamp = int(time.time())
    registry = build_registry()
    context = {"workflow_name": WORKFLOW_NAME, "baseline": _baseline()}
    case_confirmed(registry, stamp, context)
    case_contradicted(registry, stamp, context)
    case_stale(registry, stamp, context)
    case_missing(registry, stamp, context)
    case_estimated(registry, stamp, context)
    context["expected"] = {
        "confirmed": 1,
        "unconfirmed": 1,
        "unresolved": 3,
        "compensation_verified": 1,
        "compensation_unverified": 1,
    }
    with open(CONTEXT_PATH, "w") as f:
        json.dump(context, f, indent=2)
    print(json.dumps(context, indent=2))
    shutdown_telemetry()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
