# OAAR Reference Implementation

Reference implementation of the [Observability Architecture for Agentic Runtimes (OAAR) RFC](https://github.com/Carniatto-Labs/oaar) on a real durable agent runtime (LangGraph 1.0, Temporal planned as second framework).

**Status:** phase 7 — **`make validate` = 54/54 checks** across ten scenarios, plus the **interactive purchase assistant** (`make chat` → `http://localhost:8090`) where the agent runs real free-form prompts end to end. Grafana reflects every run (three provisioned dashboards). Validation scenarios (each also runnable standalone via `make <target>`):

| Scenario | Target | Validates |
| --- | --- | --- |
| Human-in-the-loop resume | `make hitl-smoke` (5/5) | `agent.run.waiting` is a distinct lifecycle event — a run parked for human approval is not a failure; resume restores the exact waiting checkpoint (`resume` link + checkpoint age measured from telemetry) |
| Compensation saga | `make saga-smoke` (3/3) | Partial rollback after mid-flight failure: confirmed effect span + `compensates` link from the compensation span, `agent.compensation.outcome=partial`, compensation rate metrics |
| Delegation fan-out | `make fanout-smoke` (4/4) | Parent/child execution identity: `parent_run.id` on children, `delegates_to` + `aggregates` links from parent to both child traces |
| Guardrail block | `make guardrail-smoke` (4/4) | Pre-execution block → `agent.run.cancelled` (not failed); guardrail evaluation span; sensitive source content provably absent from traces AND logs |
| MCP retry + idempotent dedup | `make mcp-smoke` (5/5) | W3C trace context propagation to an MCP server, retry correlation via `retries` link, deduplication observable per attempt (`agent.tool.deduplication.status`) |

## Interactive purchase assistant (the day-to-day use case)

`make chat` starts the assistant at `http://localhost:8090`. Type a free-form prompt (English or Portuguese) and a **real LLM** drives the agent: no scripted parsing, no stubs — a LangGraph `StateGraph` (`agent → tools → route`) with an Ollama model (`gemma4:12b-mlx` by default) doing tool-calling end to end. The agent searches the catalog, picks the product, charges the card, confirms the effect — streaming every step into the UI and every semantic into telemetry:

- **A real model decides** — every turn is an actual LLM inference (emitted as a `chat {model}` GenAI inference span), followed by an `agent.decision` span (`select_tool` or `respond`) carrying the sanitized model rationale. Durable state lives in a real LangGraph `PostgresSaver` checkpoint per `thread.id` — stop the agent mid-run and the thread resumes from Postgres.
- **One trace, three services** — agent (`oaar-impl`), `product-api` (search/lookup/reserve/release) and `payment-api` (authorization) all emit into the same W3C-propagated trace, visible in Tempo via the "Open trace in Grafana" link on each answer.
- **Real gateway rules, not scripts** — declines happen where they happen in production: `insufficient_limit` (per-brand ceilings: visa $1500, mastercard $800, amex $2000, elo $500), `card_blocked` (cards ending `0000`), `out_of_stock` (inventory reserve inside the payment authorization), `not_found` (catalog search). The agent maps each failure to `payment.failure.reason` + `payment.failure.stage` on the charge span, emits the exception, creates **no effect**, and answers with a natural summary.
- **Outcomes, not crashes** — every decline is a completed execution with logical outcome `declined`; only real runtime errors produce failed runs. `pending` is its own outcome: a prompt without a card number ends with the model asking for one, and the run is not a failure. Approved charges emit a confirmed `agent.effect` (compensation available = refund) and increment `oaar.purchases_total{outcome}`.
- **Inventory compensation is real** — a reserve that fails at authorization is released; stock decrements only on approval.
- Each browser session keeps one `thread.id`; every prompt is a new run in the same thread, so the Grafana Purchase Flow dashboard accumulates real, user-driven traffic.
- Model is configurable via `OAAR_MODEL` and `OAAR_OLLAMA_BASE_URL` (defaults: `gemma4:12b-mlx`, `http://host.docker.internal:11434`); pull the model on the host with `ollama pull gemma4:12b-mlx` before the first run.

RFC coverage for this flow (§4 identity, §5 lifecycle, §7 decisions, §8 tools/W3C, §9 effects, §10 outcomes, §11 metrics, §12 privacy — full PAN never enters telemetry) is now demonstrated by the assistant itself rather than a scripted test.

Phase 6 additions:

- `make validate` — full harness: runs every demo + verify pair (pipeline, langgraph, abc, abc-unknown, hitl, saga, fanout, guardrail, mcp, grafana) in one pass and prints the consolidated **RFC section → evidence** matrix. Current output: 54 pass / 0 fail.
- `make chat` — builds and starts the **interactive purchase assistant** at `http://localhost:8090` (chat UI, live agent-step timeline, Tempo trace links).
- **Grafana** at `http://localhost:3000` (anonymous admin), folder **OAAR**:
  - *Runtime & Execution Identity* — total executions, outcomes by state, run duration, executions by workflow × outcome, decisions by kind.
  - *Logical Outcomes, Effects & Controls* — compensations by outcome, guardrail evaluations, tool calls by dedup status, tool table, and a live lifecycle log panel correlated by run id.
  - *Purchase Flow (e-commerce)* — purchase orders by business outcome (accepted vs declined), workflow executions (declined ≠ failed), agent tool calls (search/lookup/charge), and live purchase log lines (order, product, payment method and last4, status, failure stage — no PAN).
  - Datasources (Prometheus `oaar-prom`, Tempo `oaar-tempo`, Loki `oaar-loki`) and dashboards are fully provisioned from `grafana/`.
  - `make dash-smoke` proves the reflection programmatically: dashboards exist, and the exact panel queries return scenario data through Grafana's datasource proxy.

## Validation matrix (RFC §4–§13 → evidence)

Every normative section of the RFC is exercised by at least one check. The `make validate` output prints this matrix with live pass/fail per row.

| RFC § | Normative requirement | Evidence (scenario → check) |
| --- | --- | --- |
| §4 Run identity & correlation | Root `agent.run` span with `run.id`, `thread.id`, `workflow.name/version`, `parent_run.id`; `trace_id` connects distributed causality | All scenarios carry the identity block; fanout: children reference `parent_run.id`; purchase: one `trace_id` across agent + product-api + payment-api |
| §5 Run lifecycle | Events for created/started/waiting/completed/failed/cancelled/resumed/compensating/compensated, each with cause, actor, state version | hitl (waiting → resumed, checkpoint age from telemetry), guardrail (cancelled ≠ failed), saga (compensating → compensated), purchase (declined ≠ failed) |
| §6 State & checkpoints | `agent.state.transition` events (version, from/to, changed_keys, diff_hash, reason, actor); `agent.checkpoint.save/restore` spans with id/thread/version/backend/duration/outcome; restore = NEW run + link | langgraph (versions monotonic 1→2→3; restore span + `resume` link; no checkpoint after restore), abc (Run B restores the exact failed checkpoint), hitl (checkpoint age ≥ 1.5 s measured from span timestamps) |
| §7 Decisions & the agentic loop | `agent.decision` spans with closed kind vocabulary (`plan/select_tool/delegate/respond/compensate/terminate`), sanitized basis, iteration id | fanout (`delegate`), purchase (`select_tool` with price in basis), guardrail (`terminate`), saga (`compensate`); the semantic layer REJECTS unknown kinds (`confirm_purchase` → SemanticViolation) and private chain-of-thought in basis |
| §8 Tools & MCP | `gen_ai.execute_tool` + `gen_ai.tool.*`; effect class ∈ read_only/idempotent_write/mutable_write; retry.count, idempotency key, deduplication status; W3C propagation | mcp (retry + dedup observable per attempt), purchase (3 services in one trace); semantic layer rejects `mutable` (closed vocabulary) |
| §9 External effects & compensation | `agent.effect` span (resource id, mutation type, idempotency key, confirmation, originating decision/tool, compensation availability); `agent.compensate` linked via `compensates` + `agent.effect.id`; outcomes succeeded/failed/partial | saga (flight compensated, hotel+car survive → `partial`), purchase (charge effect only when confirmed; declined → no effect at all) |
| §10 Non-linear causality | Typed span links only: `resume`, `delegates_to`, `triggered_by`, `compensates`, `retries`, `aggregates` | langgraph (`resume`), fanout (`delegates_to` + `aggregates`), saga (`compensates`), mcp (`retries`) |
| §11 Metrics | Run duration, decision/tool/compensation/guardrail counts, outcome rates; unique ids MUST NOT be metric dimensions | metrics allowlist rejects `run.id` as label (SemanticViolation; cardinality bounded at 788 series); denominators kept separate: waiting/cancelled/declined never count as `failed` (abc-unknown, hitl, purchase: `failed` delta 0 while `completed` +2); business outcomes as separate low-cardinality series (`oaar.purchases_total{outcome}`) |
| §12 Logs, privacy, guardrails | Logs carry trace/span/run/thread ids; content is opt-in; `agent.guardrail.evaluate` with policy id@version, no sensitive source | pipeline (50 correlated Loki lines; collector redacts `sk-` key + email), guardrail (bait policy name + body absent from traces AND logs), purchase (full PAN never present) |
| §13 Collection architecture | Collector is the enforcement point (validation, redaction, filtering, routing) before backends; apps minimize at source | compose: OTLP → collector → Tempo/Prometheus/Loki; pipeline proves redaction happens at the collector; grafana scenario + dash-smoke prove backends serve the scenario data |

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
- [discussion-1-reply.md](discussion-1-reply.md) — draft reply for [Discussion #1](https://github.com/Carniatto-Labs/Observability-Architecture-for-Agentic-Runtimes/discussions/1), presenting the measured denominator evidence
- [OAAR RFC](https://github.com/Carniatto-Labs/oaar/blob/main/RFC.md) — the specification under validation

## License

TBD (will match the OAAR spec repository).
