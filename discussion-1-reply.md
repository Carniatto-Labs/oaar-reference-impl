# Draft reply — Discussion #1 (execution identity vs logical outcome denominators)

> Addressed to @TheBuilderJR's comment on "Introducing the Observability Architecture for Agentic Runtimes".
> Replace `[REPO-LINK]` with the repository link before posting. Numbers below are measured output of
> this reference implementation (`make validate`, 60/60 checks, LangGraph 1.2 + Postgres checkpoints
> + OTel → Tempo/Prometheus/Loki).

---

Thanks @TheBuilderJR — this is exactly the question we built a reference implementation to answer
([REPO-LINK]), and your reading matches both the intent and what we measured empirically.

**1. Yes — §11 rates are execution-level by design.**

Completion/failure/cancellation/compensation rates are Run-level numbers: they answer "how healthy
is the runtime at executing what it was asked to execute", not "how often did the business get what
it wanted". Those denominators legitimately diverge in *both* directions, and we test both skews:

- **One logical outcome, N executions.** Run A checkpoints and fails; Run B restores the exact
  checkpoint (same `thread.id`, `resume` link) and completes. Measured in our harness
  (`abc` scenario): `oaar_runs_total{outcome=completed}` +1 and `{outcome=failed}` +1 — two
  executions — while the logical operation, identified by a `correlation.operation` id carried on
  events and logs, has exactly one outcome (accepted). Artifact production is 2/2 and customer
  acceptance is 1/2. You're right that distinct-thread counting would not recover the outcome: the
  thread continues with later, unrelated work. Our checkpoint-age check (measured from span
  timestamps, ≥ 1.5 s) additionally shows the restore is real, not a re-run.

- **Execution success, business decline.** In our e-commerce scenario (`purchase` scenario) the
  payment gateway declines the charge. The execution is healthy end to end — agent → product-api →
  payment-api in one trace, the error properly mapped to `payment.failure.stage=acquirer_authorization`
  / `reason=gateway_declined` on the tool span, no charge effect created — so the run completes:
  `completed` +2, `failed` +0 across both flows. The business outcome `declined` exists only on the
  `agent.outcome.observed` event and a separate counter, never in the execution-failure series.

**2. The application outcome key stays on events — not as a metric dimension.**

Exactly the shape you suggested:

- Execution metrics: `oaar_runs_total` with only low-cardinality labels (`workflow_name`,
  `outcome` ∈ completed/failed/cancelled). `waiting` is a lifecycle state, not a failure;
  cancelled guardrail blocks are not failures; declined purchases are not failures.
- The application outcome key (`agent.logical.outcome` ∈ accepted/declined/pending/unknown) rides
  on `agent.outcome.observed` events and correlated logs — never on execution metrics.
- Business rates come from separate low-cardinality series (e.g.
  `oaar_purchases_total{outcome=accepted|declined}`) or from dashboard queries that group
  executions by `correlation.operation` via trace/log lookup.
- We enforce "no unique ids as metric dimensions" in the semantic layer: adding a `run.id` label to
  a metric raises a violation at emit time, so the operation id can't silently leak into a
  denominator.

**3. A worked example for the guide.**

We'd support adding a non-normative worked example to the adoption guide (bridging §6 and §11),
roughly:

> Run A checkpoints (state v3) and fails. Run B restores v3 and completes the same work.
> Execution panel: failed +1, completed +1. Operation panel (grouped by
> `correlation.operation`): 1 outcome, accepted. Both numbers are correct — they answer
> different questions.

Our reference implementation runs this exact scenario (plus the pending-at-reporting-cut variant,
where `pending` at the cut is not a failure) — happy to distill it into the guide if useful.

Disclosure: we implemented the RFC in a reference implementation to validate it; this reply was
drafted with AI assistance.
