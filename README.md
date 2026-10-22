# OAAR Reference Implementation

Reproduction for the [Observability Architecture for Agentic Runtimes (OAAR)](https://github.com/Carniatto-Labs/Observability-Architecture-for-Agentic-Runtimes)
specification, on a real durable agent runtime (LangGraph 1.x, Temporal planned as second framework).

| | |
|---|---|
| **Spec** | OAAR **v0.3.0** — separate repository, pinned by tag |
| **This repo** | the reproduction: harness, scenarios, dashboards, evidence |
| **Pin** | `spec.json` (printed by `make validate`); the spec side is its `REFERENCE_IMPLEMENTATION.md` |
| **Reproduce** | `make up && make validate` — 66 checks, 12 scenarios |

Spec and reproduction are separate repositories on purpose: a red `make validate` is evidence
about a runtime, not about the specification. Attribute names, vocabularies and required fields
are defined by the spec only; changing one of them is a spec change first, then a change here.

**Status:** phase 9 — **66 checks across twelve scenarios** (`make validate`; see **Known limits** for the current suite status), plus the **interactive purchase assistant** (`make chat` → `http://localhost:8090`) where the agent runs real free-form prompts end to end. Grafana reflects every run (three provisioned dashboards). Validation scenarios (each also runnable standalone via `make <target>`):

| Scenario | Target | Validates |
| --- | --- | --- |
| Effect evidence (lost ACK) | `make effect-smoke` (6/6) | `agent.effect` starts `unresolved`; transport ACK is not evidence; a fresh, registry-corroborated readback from an authorised source resolves it; stale, missing and model-estimated observations do not; compensation `requested` is not `verified` |
| Business-outcome dedup | `make outcome-dedup-smoke` (6/6) | One business outcome per logical operation: a restored run that reports the same outcome again is kept as history but does not count; `pending` never counts; a late conflicting outcome is flagged and never overwrites the recorded one |
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
- **Outcomes, not crashes** — every decline is a completed execution with business outcome `declined`; only real runtime errors produce failed runs. The run span carries the *artifact* level (`agent.artifact.outcome`), never the business value: a declined purchase is `not_applicable`, an approved one `produced`, and a prompt still waiting for a card is `pending`. `pending` is its own business outcome too: a prompt without a card number ends with the model asking for one, and the run is not a failure. Approved charges emit a confirmed `agent.effect` (compensation available = refund) and increment `oaar.purchases_total{agent_purchase_outcome}` once per logical operation.
- **Inventory compensation is real** — a reserve that fails at authorization is released; stock decrements only on approval.
- Each browser session keeps one `thread.id`; every prompt is a new run in the same thread, so the Grafana Purchase Flow dashboard accumulates real, user-driven traffic.
- Model is configurable via `OAAR_MODEL` and `OAAR_OLLAMA_BASE_URL` (defaults: `gemma4:12b-mlx`, `http://host.docker.internal:11434`); pull the model on the host with `ollama pull gemma4:12b-mlx` before the first run.

RFC coverage for this flow (§4 identity, §5 lifecycle, §7 decisions, §8 tools/W3C, §9 effects, §10 outcomes, §11 metrics, §12 privacy — full PAN never enters telemetry) is now demonstrated by the assistant itself rather than a scripted test.

Phase 6 additions:

- `make validate` — full harness: runs every demo + verify pair (pipeline, langgraph, abc, abc-unknown, outcome-dedup, hitl, saga, effect-unresolved, fanout, guardrail, mcp, grafana) in one pass and prints the consolidated **RFC section → evidence** matrix. Expected output: 66 pass / 0 fail.
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
| §3.1 / §6.1 Outcome levels | Execution, artifact and business are separate, with disjoint vocabularies; the run span never carries a business value | abc (A artifact `pending`, B `produced`), outcome-dedup, purchase (declined → `not_applicable`, no effect); the semantic layer REJECTS a business value on `agent.artifact.outcome` |
| §6 State & checkpoints | `agent.state.transition` events (version, from/to, changed_keys, diff_hash, reason, actor); `agent.checkpoint.save/restore` spans with id/thread/version/backend/duration/outcome; restore = NEW run + link | langgraph (versions monotonic 1→2→3; restore span + `resume` link; no checkpoint after restore), abc (Run B restores the exact failed checkpoint), hitl (checkpoint age ≥ 1.5 s measured from span timestamps) |
| §7 Decisions & the agentic loop | `agent.decision` spans with closed kind vocabulary (`plan/select_tool/delegate/respond/compensate/terminate`), sanitized basis, iteration id | fanout (`delegate`), purchase (`select_tool` with price in basis), guardrail (`terminate`), saga (`compensate`); the semantic layer REJECTS unknown kinds (`confirm_purchase` → SemanticViolation) and private chain-of-thought in basis |
| §8 Tools & MCP | `gen_ai.execute_tool` + `gen_ai.tool.*`; effect class ∈ read_only/idempotent_write/mutable_write; retry.count, idempotency key, deduplication status; W3C propagation | mcp (retry + dedup observable per attempt), purchase (3 services in one trace); semantic layer rejects `mutable` (closed vocabulary) |
| §9 External effects & compensation | `agent.effect` span per attempt (logical effect id, attempt id/index, mutation type, evidence kind, confirmation status, originating decision/tool, compensation availability); `agent.compensate` linked via `compensates` + `agent.effect.id`; outcomes succeeded/failed/partial/not_applicable, plus `agent.compensation.state` | saga (flight compensated, hotel+car survive → `partial`), purchase (charge effect only when confirmed; declined → no effect at all), effect-unresolved (verified and unverified compensations) |
| §9.1 Intent / attempt / evidence | Effect starts `unresolved`; ACK is transport evidence, not effect evidence; a readback resolves only if bound to the effect and attempt, corroborated by the trusted registry, from a source the policy authorises, post-attempt and fresh; otherwise it stays `unresolved` with a reason; unresolved effects are counted by status, never as success or failure | effect-unresolved: confirmed (2 attempts, 1 logical id), contradicted → unconfirmed, stale → unresolved, no observation → unresolved, model estimate → unresolved; counters 1/1/3 |
| §10 Non-linear causality | Typed span links only: `resume`, `delegates_to`, `triggered_by`, `compensates`, `retries`, `aggregates` | langgraph (`resume`), fanout (`delegates_to` + `aggregates`), saga (`compensates`), mcp (`retries`) |
| §11 Metrics | Run duration, decision/tool/compensation/guardrail counts, outcome rates; unique ids MUST NOT be metric dimensions | metrics allowlist rejects `run.id` as label (SemanticViolation; cardinality bounded at 788 series); denominators kept separate: waiting/cancelled/declined never count as `failed` (abc-unknown, hitl, purchase: `failed` delta 0 while `completed` +2); business outcomes as separate low-cardinality series (`oaar.purchases_total{outcome}`); one business outcome per logical operation — retries/restores suppressed, conflicts flagged (outcome-dedup: 3 operations / 4 runs / 6 observations → +3) |
| §12 Logs, privacy, guardrails | Logs carry trace/span/run/thread ids; content is opt-in; `agent.guardrail.evaluate` with policy id@version, no sensitive source | pipeline (50 correlated Loki lines; collector redacts `sk-` key + email), guardrail (bait policy name + body absent from traces AND logs), purchase (full PAN never present) |
| §13 Collection architecture | Collector is the enforcement point (validation, redaction, filtering, routing) before backends; apps minimize at source | compose: OTLP → collector → Tempo/Prometheus/Loki; pipeline proves redaction happens at the collector; grafana scenario + dash-smoke prove backends serve the scenario data |

## Purpose

1. Validate every normative requirement of RFC 0.3.0 (operation identity, lifecycle, checkpoints, outcome levels, decisions, tools/MCP, external effects, compensation, causality links, metrics, privacy, collection architecture), and state the ones it does not yet exercise (see **Known limits**).
2. Validate experimentally the distinctions raised in [Discussion #1](https://github.com/Carniatto-Labs/Observability-Architecture-for-Agentic-Runtimes/discussions/1): execution identity vs. logical outcome identity, three-level outcomes, and metric denominators (unit, cohort, window, finality).
3. Prove the accounting rule those denominators depend on: **one business outcome per logical operation**, with retries and restores suppressed and conflicts flagged (`make outcome-dedup-smoke`).

## The validation question

If Run A checkpoints and fails, and Run B restores and completes the same work:

| Denominator | Result |
|---|---|
| Execution completion (Runs) | 2/3 |
| Artifact production (operations) | 2/2 |
| Customer acceptance (proposals) | 1/2 |

This repository exists to demonstrate and test those distinctions on a production-grade runtime before the RFC standardizes any attribute names for them.

## Known limits

Stated up front, because they bound what the measured numbers mean:

- **The authoritative source is simulated.** `effect-unresolved` exercises lost acknowledgements, stale and missing readbacks, unauthorized sources and unverified compensation, but the readback comes from an in-process registry, not from a real device, ledger or acquirer. What is proven is that the *semantics* survive those conditions — not that a particular external system can be trusted.
- **Effect evidence is not a conformance claim against AEA.** The field set follows the `authoritative-effect-evidence.v0.2` profile closely enough to be comparable, but this repo does not run that verifier and does not claim AEA conformance.
- **The outcome registry is process-local.** `outcome_registry.py` implements the *semantics* (one terminal value per subject, duplicates suppressed, conflicts flagged) behind an interface whose production implementation is a unique index on `(subject, definition, definition_version)`. The scenario proves the accounting rule, not crash durability.
- **One framework.** LangGraph only. RFC §14 asks for validation across more than one framework; Temporal is the planned second one.
- **Run vs execution naming.** The RFC (and this implementation) treat a restore as a **new Run** with the same `thread.id`. Upstream PR open-telemetry/semantic-conventions-genai#445 keeps one `gen_ai.agent.execution.id` across suspend/resume. Both can coexist only if the mapping is stated explicitly; today it is not.
- **Suite status at `v0.3.0`.** The check count is 66 (58 unique checks, `verify_abc_checkpoint` running twice). The last full green run was 54/54; the outcome-accounting, effect-evidence and semantic changes were validated offline (registry, counters, idempotency, vocabularies, effect resolution against in-memory spans) but the full harness has not been re-run against a live stack since. Run `make validate` before quoting the number.

## Reproduce

```bash
git clone https://github.com/Carniatto-Labs/oaar-reference-impl && cd oaar-reference-impl
git checkout v0.3.0
make up          # pinned stack (app, product-api, payment-api, postgres, collector, tempo, prometheus, loki, grafana)
make validate    # every demo + verify pair, plus the RFC section -> evidence matrix
```

Expected: `individual checks: 66 pass, 0 fail` and `ALL SCENARIOS VALIDATED`.

Requirements: Docker or colima with ~4 GB free. No model is needed for validation; the
interactive assistant additionally needs `ollama pull gemma4:12b-mlx` and `make chat`.

Individual scenarios run standalone: `make abc-smoke`, `make abc-unknown-smoke`,
`make outcome-dedup-smoke`, `make hitl-smoke`, `make saga-smoke`, `make effect-smoke`, `make fanout-smoke`,
`make guardrail-smoke`, `make mcp-smoke`, `make dash-smoke`.

## Documentation

- [PLAN.md](PLAN.md) — full implementation plan, decisions, and acceptance criteria
- [discussion-1-reply.md](discussion-1-reply.md) — draft reply for [Discussion #1](https://github.com/Carniatto-Labs/Observability-Architecture-for-Agentic-Runtimes/discussions/1), presenting the measured denominator evidence
- [OAAR RFC](https://github.com/Carniatto-Labs/oaar/blob/main/RFC.md) — the specification under validation

## License

TBD (will match the OAAR spec repository).
