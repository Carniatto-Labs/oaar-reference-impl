# OAAR Reference Implementation

Reference implementation of the [Observability Architecture for Agentic Runtimes (OAAR) RFC](https://github.com/Carniatto-Labs/oaar) on a real durable agent runtime (LangGraph 1.0, Temporal planned as second framework).

**Status:** phase 3 complete — LangGraph adapter (`OtelCheckpointer` + `durable_run`) validated on a real durable runtime: checkpoint save/restore spans, resume link across traces, and monotonic state versions across runs, langgraph smoke 6/6 PASS (`make langgraph-smoke`). Earlier: pipeline smoke 5/5 PASS (`make smoke`). See [PLAN.md](PLAN.md).

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
