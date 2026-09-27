# OAAR Reference Implementation

Reference implementation of the [Observability Architecture for Agentic Runtimes (OAAR) RFC](https://github.com/Carniatto-Labs/oaar) on a real durable agent runtime (LangGraph 1.0, Temporal planned as second framework).

**Status:** phase 6 complete — **`make validate` = 60/60 checks** across eleven scenarios, with **Grafana reflecting every run** (three provisioned dashboards; verified in-process by the `grafana` scenario). Phase 5 scenarios (each also runnable standalone via `make <target>`):

| Scenario | Target | Validates |
| --- | --- | --- |
| Human-in-the-loop resume | `make hitl-smoke` (5/5) | `agent.run.waiting` is a distinct lifecycle event — a run parked for human approval is not a failure; resume restores the exact waiting checkpoint (`resume` link + checkpoint age measured from telemetry) |
| Compensation saga | `make saga-smoke` (3/3) | Partial rollback after mid-flight failure: confirmed effect span + `compensates` link from the compensation span, `agent.compensation.outcome=partial`, compensation rate metrics |
| Delegation fan-out | `make fanout-smoke` (4/4) | Parent/child execution identity: `parent_run.id` on children, `delegates_to` + `aggregates` links from parent to both child traces |
| Guardrail block | `make guardrail-smoke` (4/4) | Pre-execution block → `agent.run.cancelled` (not failed); guardrail evaluation span; sensitive source content provably absent from traces AND logs |
| MCP retry + idempotent dedup | `make mcp-smoke` (5/5) | W3C trace context propagation to an MCP server, retry correlation via `retries` link, deduplication observable per attempt (`agent.tool.deduplication.status`) |
| E-commerce purchase flow | `make purchase-smoke` (6/6) | Agent orchestrates real product-api + payment-api services in ONE trace (W3C propagation, `service.name` per hop). Success test: product id/name/price, payment method ****4444, confirmed charge effect, outcome `accepted`. Declined test: failure mapped to `stage=acquirer_authorization`, `reason=gateway_declined`, error status + exception event on the attempt, NO charge effect — and the decline is a business outcome (`agent.purchase.outcome=declined`), never an execution failure. Full PAN never enters telemetry |

Phase 6 additions:

- `make validate` — full harness: runs every demo + verify pair (pipeline, langgraph, abc, abc-unknown, hitl, saga, fanout, guardrail, mcp, purchase, grafana) in one pass and prints the consolidated **RFC section → evidence** matrix. Current output: 60 pass / 0 fail.
- **Grafana** at `http://localhost:3000` (anonymous admin), folder **OAAR**:
  - *Runtime & Execution Identity* — total executions, outcomes by state, run duration, executions by workflow × outcome, decisions by kind.
  - *Logical Outcomes, Effects & Controls* — compensations by outcome, guardrail evaluations, tool calls by dedup status, tool table, and a live lifecycle log panel correlated by run id.
  - *Purchase Flow (e-commerce)* — purchase orders by business outcome (accepted vs declined), workflow executions (declined ≠ failed), product/payment tool calls, and live purchase log lines (order, product, payment method ****4444, status, failure stage — no PAN).
  - Datasources (Prometheus `oaar-prom`, Tempo `oaar-tempo`, Loki `oaar-loki`) and dashboards are fully provisioned from `grafana/`.
  - `make dash-smoke` proves the reflection programmatically: dashboards exist, and the exact panel queries return scenario data through Grafana's datasource proxy.

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
