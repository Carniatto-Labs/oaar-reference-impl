"""Effect evidence resolution.

This module is the pure decision function the runtime uses to turn an observation into a
confirmation status. It is deliberately separate from telemetry: the scenario replays it, and
the traces only record what it decided and why.

The rule it implements is the narrow one from the Agent Effect Authority contract, profile
``authoritative-effect-evidence.v0.2``:

    proposal != authorization != dispatch != acknowledgement != effect evidence

An effect is only resolved when the observation is bound to *this* logical effect and *this*
attempt, is corroborated by the trusted evidence registry with the exact (evidence id, digest)
pair, comes from a source the policy authorizes for that effect domain and observation kind,
happens after the attempt, and is fresh. Anything else stays ``unresolved`` — never promoted to
success and never demoted to failure.

``confirmed`` / ``contradicted`` / ``unresolved`` are the three verdicts the runtime can defend.
Transport acknowledgements, model estimates, cached state and self-declared source names cannot
produce evidence on their own.
"""

from __future__ import annotations

from dataclasses import dataclass

CONFIRMED = "confirmed"
#: A contradicted effect is knowledge, not failure: it is ``unconfirmed`` in the confirmation
#: vocabulary, with ``contradicted`` as the reason.
UNCONFIRMED = "unconfirmed"
CONTRADICTED = "contradicted"
UNRESOLVED = "unresolved"

VERDICTS = frozenset({"CONFIRMED", "CONTRADICTED"})

#: Observations older than this are stale and cannot resolve an effect.
DEFAULT_FRESHNESS_MS = 30_000


@dataclass(frozen=True)
class Observation:
    logical_effect_id: str
    attempt_id: str
    evidence_id: str
    evidence_digest: str
    effect_domain: str
    source_id: str
    observation_kind: str
    verdict: str
    age_ms: int


@dataclass(frozen=True)
class TrustRecord:
    """One entry of the trusted evidence registry."""

    logical_effect_id: str
    attempt_id: str
    evidence_id: str
    evidence_digest: str
    verifier_id: str
    trust: str = "verified"
    invalidation: str = "valid"
    observed_after_attempt: bool = True
    fresh: bool = True


@dataclass(frozen=True)
class Resolution:
    status: str
    reason: str
    evidence_id: str | None = None

    @property
    def resolved(self) -> bool:
        return self.status != UNRESOLVED


class TrustedEvidenceRegistry:
    """Registry that binds an observation to a verification record.

    ``policy`` maps ``(effect_domain, observation_kind) -> {authorized source ids}``. A source
    that is not listed for the pair cannot resolve an effect even if it reports a verdict: the
    registry is where "the source is allowed to speak about this kind of thing" lives.
    """

    def __init__(self, policy: dict | None = None):
        self.policy = {
            (domain, kind): set(sources)
            for (domain, kind), sources in (policy or {}).items()
        }
        self._records: dict[tuple[str, str, str, str], TrustRecord] = {}

    def authorize(self, effect_domain: str, observation_kind: str, source_id: str) -> bool:
        allowed = self.policy.get((effect_domain, observation_kind))
        return bool(allowed) and source_id in allowed

    def register(self, record: TrustRecord) -> None:
        key = (
            record.logical_effect_id,
            record.attempt_id,
            record.evidence_id,
            record.evidence_digest,
        )
        self._records[key] = record

    def lookup(self, observation: Observation) -> TrustRecord | None:
        return self._records.get(
            (
                observation.logical_effect_id,
                observation.attempt_id,
                observation.evidence_id,
                observation.evidence_digest,
            )
        )


def resolve(
    observation: Observation | None,
    registry: TrustedEvidenceRegistry,
    *,
    logical_effect_id: str,
    attempt_id: str,
    freshness_ms: int = DEFAULT_FRESHNESS_MS,
) -> Resolution:
    """Decide whether an observation resolves the effect. Order of checks is the contract."""

    if observation is None:
        return Resolution(UNRESOLVED, "no_observation")

    if observation.logical_effect_id != logical_effect_id:
        return Resolution(UNRESOLVED, "evidence_for_other_effect", observation.evidence_id)

    if observation.attempt_id != attempt_id:
        return Resolution(UNRESOLVED, "evidence_for_other_attempt", observation.evidence_id)

    if observation.verdict not in VERDICTS:
        return Resolution(UNRESOLVED, "unknown_verdict", observation.evidence_id)

    record = registry.lookup(observation)
    if record is None:
        # A self-declared source name is not evidence: the registry must corroborate the tuple.
        return Resolution(UNRESOLVED, "evidence_not_registered", observation.evidence_id)

    if record.trust != "verified":
        return Resolution(UNRESOLVED, "evidence_not_trusted", observation.evidence_id)

    if record.invalidation != "valid":
        return Resolution(UNRESOLVED, "evidence_invalidated", observation.evidence_id)

    if not registry.authorize(
        observation.effect_domain, observation.observation_kind, observation.source_id
    ):
        return Resolution(UNRESOLVED, "source_not_authorized", observation.evidence_id)

    if not record.observed_after_attempt or observation.age_ms < 0:
        return Resolution(UNRESOLVED, "observed_before_attempt", observation.evidence_id)

    if not record.fresh or observation.age_ms > freshness_ms:
        return Resolution(UNRESOLVED, "stale_evidence", observation.evidence_id)

    if observation.verdict == "CONFIRMED":
        return Resolution(CONFIRMED, "confirmed", observation.evidence_id)
    return Resolution(UNCONFIRMED, "contradicted", observation.evidence_id)
