import os
import re
import subprocess
import sys

SCENARIOS = [
    (
        "pipeline",
        "§2 §13 §11",
        "OTel pipeline hygiene: redaction, cardinality, correlated logs, semantic rules",
        (["python", "-m", "oaar_impl.demo_pipeline"], ["python", "-m", "oaar_impl.verify_pipeline"], None),
    ),
    (
        "langgraph",
        "§4 §5 §6",
        "Checkpoint save/restore spans, resume link, monotonic state versions",
        (["python", "-m", "oaar_impl.demo_langgraph"], ["python", "-m", "oaar_impl.verify_langgraph"], None),
    ),
    (
        "abc",
        "§4 §5 §10 §12",
        "Three denominators: executions 2/3, artifacts 2/2, acceptance 1/2 via logical operation",
        (["python", "-m", "oaar_impl.demo_abc_checkpoint"], ["python", "-m", "oaar_impl.verify_abc_checkpoint"], None),
    ),
    (
        "abc-unknown",
        "§5 §10",
        "Q-unknown variant: pending at reporting cut is not a failure",
        (
            ["python", "-m", "oaar_impl.demo_variant_q_unknown"],
            ["python", "-m", "oaar_impl.verify_abc_checkpoint"],
            {"OAAR_ABC_CONTEXT_PATH": "/tmp/oaar-abc-unknown-context.json"},
        ),
    ),
    (
        "hitl",
        "§5",
        "Human-in-the-loop: waiting lifecycle, checkpoint-age resume, waiting != failure",
        (["python", "-m", "oaar_impl.demo_hitl_resume"], ["python", "-m", "oaar_impl.verify_hitl_resume"], None),
    ),
    (
        "saga",
        "§9",
        "Compensation saga: partial rollback, compensates link, effect correlation",
        (["python", "-m", "oaar_impl.demo_compensation_saga"], ["python", "-m", "oaar_impl.verify_compensation_saga"], None),
    ),
    (
        "fanout",
        "§4",
        "Delegation fan-out: parent_run.id, delegates_to + aggregates links",
        (["python", "-m", "oaar_impl.demo_delegation_fanout"], ["python", "-m", "oaar_impl.verify_delegation_fanout"], None),
    ),
    (
        "guardrail",
        "§12",
        "Guardrail block: cancelled not failed, sensitive content absent from traces and logs",
        (["python", "-m", "oaar_impl.demo_guardrail_block"], ["python", "-m", "oaar_impl.verify_guardrail_block"], None),
    ),
    (
        "mcp",
        "§8",
        "MCP: W3C propagation, retry link, idempotent dedup observable per attempt",
        (["python", "-m", "oaar_impl.demo_mcp_retry_dedup"], ["python", "-m", "oaar_impl.verify_mcp_retry_dedup"], None),
    ),
    (
        "grafana",
        "§13",
        "Grafana reflects the run: provisioned dashboards + datasource queries return scenario data",
        (None, ["python", "-m", "oaar_impl.verify_dashboards"], None),
    ),
]

PASS_RE = re.compile(r"\[PASS\]| PASS ")
FAIL_RE = re.compile(r"\[FAIL\]| FAIL ")


def run(cmd, env_extra=None):
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


def main() -> int:
    print("=" * 78)
    print("OAAR REFERENCE IMPLEMENTATION — FULL VALIDATION")
    print("=" * 78)
    total_pass = 0
    total_fail = 0
    failed_scenarios = []

    for name, sections, description, (demo_cmd, verify_cmd, env_extra) in SCENARIOS:
        print(f"\n--- {name}  [{sections}]  {description}")
        if demo_cmd is not None:
            demo = run(demo_cmd, env_extra)
            if demo.returncode != 0:
                print(f"  DEMO FAILED (exit {demo.returncode})")
                print(demo.stdout[-1500:])
                print(demo.stderr[-1500:])
                total_fail += 1
                failed_scenarios.append(name)
                continue
        verify = run(verify_cmd, env_extra)
        out = verify.stdout
        for line in out.splitlines():
            if PASS_RE.search(line) or FAIL_RE.search(line):
                print(f"  {line.strip()}")
        p = len(PASS_RE.findall(out))
        f = len(FAIL_RE.findall(out))
        total_pass += p
        total_fail += f
        if verify.returncode != 0 or f:
            failed_scenarios.append(name)
            if not (PASS_RE.search(out) or FAIL_RE.search(out)):
                print(f"  VERIFY FAILED (exit {verify.returncode})")
                print(out[-1500:])
                print(verify.stderr[-1500:])

    print("\n" + "=" * 78)
    print("CONSOLIDATED MATRIX — RFC section → evidence")
    print("=" * 78)
    for name, sections, description, _ in SCENARIOS:
        status = "FAIL" if name in failed_scenarios else "PASS"
        print(f"  [{status}] {sections:<12} {name:<12} {description}")
    print("-" * 78)
    print(f"individual checks: {total_pass} pass, {total_fail} fail")
    if failed_scenarios:
        print(f"failed scenarios: {', '.join(failed_scenarios)}")
        return 1
    print("ALL SCENARIOS VALIDATED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
