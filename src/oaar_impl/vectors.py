"""Portable effect-evidence vectors (RFC §9.1).

The vectors are the interface: a runtime conforms if, for every case in the JSON file, it
reaches the same (status, reason) pair. This module is only the reference runner — it is not
part of the contract, and another runtime is expected to write its own in whatever language it
uses.

Usage:
    python -m oaar_impl.vectors [path-to-suite.json]
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

from oaar_impl.effect_evidence import (
    Observation,
    TrustRecord,
    TrustedEvidenceRegistry,
    resolve,
)

DEFAULT_SUITE = "effect-evidence.v0.1.json"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def suite_path(path: str | None = None) -> Path:
    return Path(path) if path else repo_root() / "vectors" / DEFAULT_SUITE


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def digest_of(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def registry_for(suite: dict, case: dict) -> TrustedEvidenceRegistry:
    policy = {
        (domain, kind): set(sources)
        for domain, kinds in (suite.get("policy") or {}).items()
        for kind, sources in kinds.items()
    }
    registry = TrustedEvidenceRegistry(policy=policy)
    for entry in case.get("registry") or []:
        registry.register(
            TrustRecord(
                logical_effect_id=entry["logical_effect_id"],
                attempt_id=entry["attempt_id"],
                evidence_id=entry["evidence_id"],
                evidence_digest=entry["evidence_digest"],
                verifier_id=entry.get("verifier_id", "unknown"),
                trust=entry.get("trust", "verified"),
                invalidation=entry.get("invalidation", "valid"),
                observed_after_attempt=entry.get("observed_after_attempt", True),
                fresh=entry.get("fresh", True),
            )
        )
    return registry


def observation_for(case: dict) -> Observation | None:
    raw = case.get("observation")
    if raw is None:
        return None
    return Observation(
        logical_effect_id=raw["logical_effect_id"],
        attempt_id=raw["attempt_id"],
        evidence_id=raw["evidence_id"],
        evidence_digest=raw["evidence_digest"],
        effect_domain=case["effect"]["domain"],
        source_id=raw["source_id"],
        observation_kind=raw["observation_kind"],
        verdict=raw["verdict"],
        age_ms=int(raw["age_ms"]),
    )


def run_case(suite: dict, case: dict):
    return resolve(
        observation_for(case),
        registry_for(suite, case),
        logical_effect_id=case["effect"]["logical_id"],
        attempt_id=case["effect"]["attempt_id"],
        freshness_ms=int(suite.get("freshness_ms", 30_000)),
    )


def verify(path: Path) -> tuple[list[tuple[str, bool, str]], dict]:
    suite = load(path)
    results = []

    spec_file = repo_root() / "spec.json"
    actual_digest = digest_of(path)
    if not spec_file.exists():
        results.append(("vendored copy matches the pinned digest", False, "spec.json not found"))
    else:
        expected_digest = json.loads(spec_file.read_text()).get("vectors_digest")
        if not expected_digest:
            results.append(
                ("vendored copy matches the pinned digest", False, "spec.json has no vectors_digest")
            )
        else:
            results.append(
                (
                    "vendored copy matches the pinned digest",
                    expected_digest == actual_digest,
                    actual_digest
                    if expected_digest == actual_digest
                    else f"{actual_digest} != {expected_digest} (re-pin spec.json)",
                )
            )

    for case in suite["cases"]:
        expect = case["expect"]
        try:
            got = run_case(suite, case)
        except Exception as exc:  # a malformed vector must fail loudly, not pass
            results.append((case["id"], False, f"runner error: {exc!r}"))
            continue
        ok = got.status == expect["status"] and got.reason == expect["reason"]
        detail = (
            f"{got.status}/{got.reason}"
            if ok
            else f"got {got.status}/{got.reason}, expected {expect['status']}/{expect['reason']}"
        )
        results.append((case["id"], ok, detail))

    return results, suite


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    path = suite_path(argv[0] if argv else None)
    results, suite = verify(path)
    print(
        f"vector suite: {suite['suite']} {suite['version']} "
        f"(spec {suite['spec']}, §{suite['section']}, {len(suite['cases'])} cases)"
    )
    width = max(len(name) for name, _, _ in results)
    failed = 0
    for name, ok, detail in results:
        if not ok:
            failed += 1
        print(f"{name.ljust(width)}  {'PASS' if ok else 'FAIL'}  {detail}")
    total = len(results)
    print(f"\n{total - failed}/{total} vectors conform")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
