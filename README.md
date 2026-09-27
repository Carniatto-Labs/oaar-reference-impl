# OAAR Reference Implementation

Reference implementation of the [Observability Architecture for Agentic Runtimes (OAAR) RFC](https://github.com/Carniatto-Labs/oaar) on a real durable agent runtime (LangGraph 1.0, Temporal planned as second framework).

**Status:** phase 4 complete — **first milestone: the three denominators validated** on a real durable runtime. `make abc-smoke` (8/8 PASS): Run A checkpoints and fails before producing the artifact, Run B restores A's last checkpoint and completes the same logical operation (`resume` link, `agent.operation.id` on events), Run C completes on a fresh thread → execution completion **2/3**, artifact production **2/2**, customer acceptance **1/2** observed post-run via `agent.outcome.observed` events (declined + accepted). `make abc-unknown-smoke` (8/8 PASS): pending/unknown at the reporting cut is not counted as failure. Earlier: langgraph smoke 6/6, pipeline smoke 5/5. See [PLAN.md](PLAN.md).

## Purpose

1. Validate every normative requirement of RFC 0.1.0 (Run identity, lifecycle, checkpoints, decisions, tools/MCP, external effects, compensation, causality links, metrics, privacy, collection architecture).
2. Validate experimentally the distinctions raised in [Discussion #1](https://github.com/Carniatto-Labs/Observability-Architecture-for-Agentic-Runtimes/discussions/1): execution identity vs. logical outcome identity, three-level outcomes, and metric denominators (unit, cohort, window, finality).

## The validation question

If Run A checkpoints and fails, and Run B restores and completes the same work:

| Denominator | Result |
|---|---|
| Execution completion (Runs) | 2/3 |
| Artifact production (operations) | 2/2 |
| Customer acceptance (proposals) | 1/2 |

This repository exists to demonstrate and test those distinctions on a production-grade runtime before the RFC standardizes any attribute names for them.

## Documentation

- [PLAN.md](PLAN.md) — full implementation plan, decisions, and acceptance criteria
- [OAAR RFC](https://github.com/Carniatto-Labs/oaar/blob/main/RFC.md) — the specification under validation

## License

TBD (will match the OAAR spec repository).
