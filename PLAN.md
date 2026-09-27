# Implementation Plan — OAAR Reference Implementation

**Status:** Approved, not started
**Version:** 0.1.0
**Date:** 2026-09-27
**Spec under validation:** [OAAR RFC 0.1.0](https://github.com/Carniatto-Labs/oaar/blob/main/RFC.md)
**Driving discussion:** [Carniatto-Labs/oaar Discussion #1](https://github.com/Carniatto-Labs/Observability-Architecture-for-Agentic-Runtimes/discussions/1)

## 1. Purpose

Build a reference implementation that validates the OAAR RFC against a real, production-grade durable agent runtime, with two goals:

1. **Maximum RFC coverage.** Exercise every normative requirement in RFC 0.1.0 (§4–§13): Run identity, lifecycle, state/checkpoints, decisions, tools/MCP, external effects, compensation, non-linear causality, metrics, logs/privacy/guardrails, and collection architecture.
2. **Validate the Discussion #1 distinctions experimentally.** Execution identity vs. logical outcome identity, three-level outcomes (execution / artifact / business), metric denominators (unit, cohort, window, finality), and the cardinality boundary between traces/events and metrics.

This repository is the validation artifact the RFC requires before proposing upstream standardization (RFC §14: extensions "SHOULD be evaluated for upstream standardization after they have been validated in real implementations across more than one framework").

## 2. Background — Discussion #1 in one page

The RFC makes checkpoint restoration a **new Run** (§6) while completion/failure rates are defined at **Run level** (§11). If Run A checkpoints and Run B restores it and completes the same work, that is two executions but one logical outcome. A thread may also contain unrelated later work, so thread identity does not recover outcome counts.

The reviewer's worked example (all in one thread):

| Run | Logical operation | Execution result |
|---|---|---|
| A | Produce proposal P | Checkpoint saved, then execution explicitly failed |
| B | Produce proposal P | Restored A, produced P, completed |
| C | Produce proposal Q | Produced Q, completed |

With P and Q produced, P declined and Q accepted:

- Execution completion: **2/3 Runs**
- Artifact-production success: **2/2 operations**
- Customer acceptance: **1/2 proposals**

If Q's decision has not arrived at the reporting cut: one decline and one **unknown**, not an invented rejection.

Reviewer recommendations adopted by this plan:

1. **Logical operation:** optional, application-assigned correlation concept; continuity rules normative, "same work" definition left to the application; association allowed on events (a batch Run may contribute to several operations); never derived from thread or trace ID.
2. **Run relationships:** explicit relationship semantics encoded through existing OTel links/events; typed span links when prior span context is available; durable checkpoint/Run references retained because traces may expire; neither a missing trace nor an absent link implies a new logical operation.
3. **Metrics boundary:** per-instance IDs (run, operation, checkpoint, idempotency) live in traces/events, never as metric dimensions; hashing a unique ID does not reduce cardinality; metrics use an allowlist of bounded classifications with a combination budget; cardinality policy even for workflow versions.
4. **Three outcomes:** execution success ≠ artifact produced ≠ customer accepted; record subject, definition/version, observation time; allow pending/unknown; a business result may arrive after all Runs have ended.
5. **Denominator failure modes to test against:** double-emitted restored Runs inflate execution counts; joining one outcome to several attempts duplicates weight; dropping unfinished operations inflates completion; completion-window numerators over start-window denominators mix cohorts; failure-biased sampling skews naive rates.

## 3. Locked decisions

| Decision | Choice |
|---|---|
| Language | Python 3.12 + uv, fully pinned (`uv.lock`) |
| Durable foundation (phase 1) | **LangGraph 1.0.x** — real agent runtime, cited by the RFC; native `thread_id`, per-superstep checkpoints, `interrupt()`/`Command` HITL |
| Durable foundation (phase 2, later) | **Temporal Python SDK 1.x** as second framework, satisfying RFC §14 multi-framework validation |
| Inference | Real OpenAI-compatible provider behind `OAAR_LLM_PROVIDER` flag; deterministic local stub when unset. Scenario control flow is scripted so validation asserts never depend on LLM output |
| Persistence | PostgreSQL 17 (production shape, not SQLite) |
| CI | None initially; `Makefile`-driven local validation |
| Repository | New repo, local-first; spec and implementation version independently |

## 4. Validation goals (acceptance criteria)

### 4.1 Discussion #1 (core)

- [ ] Run B (restore) has a new `agent.runtime.run.id`, same `thread.id`, references the restored `checkpoint.id`, and carries a `resume` span link to Run A
- [ ] Execution completion rate counts A=failed, B=completed, C=completed → **2/3**
- [ ] Artifact-production rate → **2/2**; customer acceptance rate → **1/2**
- [ ] Q-unknown variant: pending outcome at reporting cut is **not** counted as failure
- [ ] No unique ID appears as a metric dimension (programmatically enforced)
- [ ] Active metric series contain only allowlisted dimensions (Prometheus series dump asserted)
- [ ] Logical-operation correlation key present on events, absent from metrics

### 4.2 RFC coverage

- [ ] §4: `agent.run` root span with full identity attribute set, incl. `parent_run.id`, `resume.reason`
- [ ] §5: lifecycle events for create/start/wait/complete/fail/cancel/resume/compensate, each with cause, timestamp, actor, state version
- [ ] §6: `agent.state.transition` events (version, from/to, changed_keys, diff_hash, reason, actor); `agent.checkpoint.save`/`.restore` spans with checkpoint.id, thread.id, state version, backend, duration, outcome
- [ ] §7: GenAI-convention inference spans (model, provider, tokens, duration); `agent.decision` spans with id/kind/basis/outcome/iteration; iteration identifier groups the loop
- [ ] §8: `gen_ai.execute_tool` with effect class, retry count, idempotency key, dedup status, MCP server id; W3C context reaches the MCP server
- [ ] §9: `agent.effect` spans with resource, mutation type, confirmation status, originating decision/tool, compensation availability; `agent.compensate` linked via span link + `agent.effect.id`; outcomes incl. `partial`
- [ ] §10: all six link types exercised (`resume`, `delegates_to`, `triggered_by`, `compensates`, `retries`, `aggregates`)
- [ ] §11: full metric set; no unique-ID dimensions; estimated cost with pricing-table version, currency, source
- [ ] §12: logs with trace/span/run/thread IDs; opt-in content with redaction; `agent.guardrail.evaluate` with policy id/version, no sensitive source content
- [ ] §13: OTel SDK → OTLP → Collector with validation, redaction, filtering, sampling, routing → backends

## 5. Architecture & stack (all pinned)

```text
docker-compose:
  app               # python:3.12 + uv; LangGraph 1.0.x runtime + OAAR semantic layer
  mcp-server        # simulated MCP server (python); receives W3C trace context
  llm-gateway       # deterministic OpenAI-compatible stub (default)
  postgres:17       # LangGraph checkpoints (PostgresSaver)
  otel-collector    # policy point: validation, redaction, filtering, sampling, routing
  tempo             # traces backend
  prometheus        # metrics backend (scrapes collector exporter :8889)
  loki              # logs backend
  grafana           # dashboards + provisioned datasources
```

## 6. LangGraph → RFC semantic mapping

| RFC concept | LangGraph primitive | Implementation |
|---|---|---|
| Run (§4) | One `invoke`/`stream` on a `thread_id` | `run()` wrapper: root span `agent.run`, fresh `run.id` |
| `thread.id` (§4) | Native `thread_id` | Pass-through |
| Checkpoint (§6) | Per-superstep checkpoints via `PostgresSaver` | Decorator around `PostgresSaver`: emits `agent.checkpoint.save`/`.restore`, captures `checkpoint.id` |
| Restore = new Run (§6, Discussion #1) | A new invocation resumes from the last checkpoint — native semantics, not staged | New Run with `resume` link to prior Run + durable `checkpoint.id` reference |
| `state.version`, `changed_keys`, `diff_hash` (§6) | Checkpoint `channel_versions` | Extracted in the checkpointer decorator → `agent.state.transition` events |
| HITL wait/resume (§5) | `interrupt()` + `Command(resume=...)` | Two Runs on one thread; wait/resume duration + checkpoint-age metrics |
| Delegation (§4, §10) | Subagent graphs as separate Runs | `parent_run.id` + `delegates_to`/`aggregates` links across traces |

## 7. Planned repository structure

```text
oaar-reference-impl/
  docker-compose.yml
  Makefile                     # up / run-all / validate / evidence
  collector/config.yaml        # redaction, filtering, routing (§13)
  grafana/provisioning/        # datasources + "Three Denominators" dashboard
  src/oaar_impl/
    otel_setup.py              # resource, tracer/meter/logger providers, OTLP exporters
    spans.py                   # agent.run / decision / checkpoint.* / effect / compensate / guardrail
    lifecycle.py               # §5 state machine + transition events
    state.py                   # §6 state-transition extraction
    run_adapter.py             # Run wrapper (identity, root span, links)
    checkpointer_otel.py       # decorator around PostgresSaver
    tools.py                   # gen_ai.execute_tool + agent.tool.* (effect class, dedup)
    mcp_client.py              # W3C propagation, retries, idempotency
    compensation.py            # saga: effects → compensate with link + effect.id
    metrics.py                 # §11 + dimension allowlist enforcement (fail on unapproved dim)
    outcomes.py                # outcome events: subject/definition/version/observed_at/pending
    correlation.py             # optional logical-operation key on events only
    pricing.py                 # versioned pricing table (currency, source)
    llm.py                     # flag-based provider (real / deterministic stub)
  scenarios/
    abc_checkpoint.py          # A/B/C + three denominators
    variant_q_unknown.py       # Q decision missing at reporting cut
    compensation_saga.py
    delegation_fanout.py
    hitl_resume.py
    guardrail_block.py
    mcp_retry_dedup.py
  validate.py                  # asserts vs Prometheus/Tempo/Loki APIs (poll + retry)
```

## 8. Scenario matrix

| Scenario | RFC sections | Discussion #1 | Expected evidence |
|---|---|---|---|
| `abc_checkpoint` | §4, §5, §6, §10, §11 | Core | A: supersteps → checkpoint → explicit failure. B: new invocation restores (real restore) → completed. C: new thread → completed. Rates 2/3, 2/2, 1/2 |
| `variant_q_unknown` | §11, outcomes | Core | Pending outcome at cut ≠ failure; one decline + one unknown |
| `hitl_resume` | §5, §6, §11 | — | `interrupt()`/`Command` as two Runs; wait/resume duration; checkpoint age at recovery |
| `compensation_saga` | §9, §10, §11 | — | Mid-workflow failure → saga; `compensates` link + `effect.id`; `partial` outcome; compensation rate |
| `delegation_fanout` | §4, §10 | — | Subagents as Runs; `parent_run.id`; `delegates_to`/`aggregates` links across traces |
| `guardrail_block` | §12 | — | `agent.guardrail.evaluate` with policy id/version; no sensitive content |
| `mcp_retry_dedup` | §8, §10 | — | W3C context at MCP server; `retries` link; idempotency key; dedup status |

Cross-cutting, asserted in every scenario: correlated logs in Loki; no unique-ID metric dimensions; deterministic control flow; GenAI inference attributes present.

## 9. Validation harness

`validate.py`:

1. Run scenarios with fixed seeds → deterministic telemetry
2. Query Prometheus: execution completion rate = 2/3 (A failed, B completed, C completed)
3. Query outcome events (Loki/logs): artifact rate 2/2, acceptance 1/2
4. Q-unknown variant: assert pending/unknown not counted as failure
5. Cardinality: dump active Prometheus series; assert allowlist-only dimensions
6. `make evidence`: export JSON + trace links/screenshots from Tempo

Asserts operate on flow-level and outcome events, never on LLM content — both provider modes (real and stub) must produce identical validation results.

## 10. Phases & milestones

| Phase | Deliverable | Milestone |
|---|---|---|
| 1 | Scaffold: uv project, pinned docker-compose stack healthy (all services green) | Stack boots |
| 2 | OTel foundation: setup, spans, collector with redaction | Telemetry flows end-to-end |
| 3 | LangGraph adapter: Run wrapper + checkpointer decorator (the mapping core) | Checkpoints visible as spans/events |
| 4 | `abc_checkpoint` + `variant_q_unknown` | **First milestone: three denominators validated** |
| 5 | Remaining scenarios (HITL, saga, delegation, guardrail, MCP) | Full RFC coverage |
| 6 | Harness: validate.py, Grafana dashboards, evidence export | Automated validation green |
| 7 | README validation matrix (RFC § → code → evidence) | Documentation complete |

## 11. Post-validation sequence (out of scope for this plan)

1. Update the RFC with minimal non-normative additions, now backed by this implementation: execution-vs-outcome identity section, the A/B/C worked example, per-metric unit/cohort/window/finality declarations, relationship-semantics table for links, cardinality policy guidance
2. Reply to Discussion #1 with repository link + measured numbers
3. Temporal as the second framework (RFC §14 multi-framework validation)
4. Housekeeping: the `oaar` repo contains a duplicate `RFC` file alongside `RFC.md` — clean up during the RFC update

## 12. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Checkpointer decorator depends on LangGraph internal interfaces | Pin exact LangGraph version; fallback to emitting spans at invoke boundaries if the interface changes |
| Metric export latency causing flaky asserts | 5s export intervals + poll-with-retry in `validate.py` |
| Real LLM varies content across runs | Asserts on flow/outcome level only (by design); deterministic stub default |
| Cardinality drift via unbounded attributes | `metrics.py` raises on unapproved dimensions; series dump asserted in validation |

## 13. References

- [OAAR RFC 0.1.0](https://github.com/Carniatto-Labs/oaar/blob/main/RFC.md)
- [OAAR Adoption Guide](https://github.com/Carniatto-Labs/oaar/blob/main/ADOPTION_GUIDE.md)
- [Discussion #1 — denominator question](https://github.com/Carniatto-Labs/Observability-Architecture-for-Agentic-Runtimes/discussions/1)
- [OpenTelemetry GenAI Semantic Conventions](https://github.com/open-telemetry/semantic-conventions-genai)
- [OpenTelemetry Trace API — Links](https://opentelemetry.io/docs/specs/otel/trace/api/#link)
- [OpenTelemetry Metrics SDK — Cardinality Limits](https://opentelemetry.io/docs/specs/otel/metrics/sdk/#cardinality-limits)
- [LangGraph Persistence](https://docs.langchain.com/oss/python/langgraph/persistence)
- [Temporal Python SDK](https://github.com/temporalio/sdk-python)
