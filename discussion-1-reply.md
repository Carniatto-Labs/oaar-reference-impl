# Draft reply — Discussion #1 (execution identity vs logical outcome denominators)

> Addressed to @TheBuilderJR's comment on "Introducing the Observability Architecture for Agentic Runtimes".
> Both repositories are public and pinned to the same version: spec
> https://github.com/Carniatto-Labs/Observability-Architecture-for-Agentic-Runtimes (v0.3.0),
> reproduction https://github.com/Carniatto-Labs/oaar-reference-impl (tag v0.3.0). They are
> separate on purpose. Numbers below: `make validate` = 66/66 checks, 12 scenarios,
> LangGraph 1.x + Postgres checkpoints + OpenTelemetry → Tempo/Prometheus/Loki.

---

Thanks @TheBuilderJR — this is the question we built a reference implementation to answer
([oaar-reference-impl](https://github.com/Carniatto-Labs/oaar-reference-impl)), and your reading
matches both the intent and what we measured. The spec
([oaar](https://github.com/Carniatto-Labs/Observability-Architecture-for-Agentic-Runtimes)) and
the reproduction are separate repositories pinned to the same version (`v0.3.0`), so each number
below is attributable to a specific pair of versions.

**1. Yes — §11 execution rates are Run-level by design, and they legitimately diverge from the
business rate in both directions.**

*One logical outcome, several executions.* Run A checkpoints and fails before producing its
artifact; Run B restores A's checkpoint (same `thread.id`, `resume` link, same
`checkpoint.id`) and produces the artifact. Measured: `oaar_runs_total` +1 `failed` and +1
`completed` — two executions — while both operations eventually produce their artifact
(2/2). Counting distinct `thread.id` would not recover that: threads continue with later,
unrelated work.

*Execution success, business decline.* In the e-commerce flow the payment gateway declines
the charge. The execution is healthy end to end (agent → product-api → payment-api in one
trace, `payment.failure.reason=insufficient_limit` / `payment.failure.stage=
acquirer_authorization` on the tool span, no charge effect created), so the run counts as
`completed`. The business outcome `declined` exists only on the `agent.outcome.observed`
event. `waiting` and `cancelled` likewise never count as `failed`.

**2. The application outcome key lives on events, never on a metric dimension.**

- Execution metrics: `oaar.runs`, labelled only by `agent.runtime.workflow.name`,
  `agent.runtime.workflow.version` and `agent.run.outcome` ∈ completed / failed / cancelled.
  Nothing else.
- The logical operation is `agent.operation.id`: application-assigned, carried on lifecycle
  events and on `agent.outcome.observed`, never derived from `thread.id` or `trace.id`, and
  never a metric label.
- Business outcomes ride on `agent.outcome.observed` with `agent.outcome.subject`,
  `agent.outcome.definition`, `agent.outcome.definition.version`, `agent.outcome.value`
  (accepted / declined / pending / unknown) and `agent.outcome.observed_at`.
- Business rates come from their own low-cardinality series (here `oaar.purchases`, labelled
  by business outcome) or from queries grouping executions by `agent.operation.id`.
- The boundary is enforced at emit time: adding `run.id`, `thread.id`, `checkpoint.id`,
  `agent.operation.id` or any non-allowlisted key to a metric raises a violation in the
  semantic layer, and the validation harness asserts the Prometheus series dump carries no
  unique-id label.

**3. One outcome per operation — because attempts otherwise inflate the denominator.**

Your second failure mode is real and we now test it (`make outcome-dedup-smoke`). Two runs of
the same operation, the second resuming the first, both report `accepted`. Across 3
operations, 4 runs and 6 reported outcomes we get exactly +3 on `oaar.purchases`
(`oaar_purchases_total` in Prometheus):

- the repeated terminal value is kept on the event stream (`agent.outcome.recorded=false`,
  `agent.outcome.prior_value=accepted`) but suppressed for counting, and counted once in
  `oaar.outcome.duplicates`;
- `pending` is recorded as an observation and never as an outcome, so a cut-off report does
  not turn into a rejection;
- a late `declined` for an already-`accepted` subject is flagged in
  `oaar.outcome.conflicts` with the recorded value unchanged — never silently
  overwritten.

**4. A worked example for the guide.**

We'd support adding a non-normative worked example bridging §6 and §11:

> Run A checkpoints (state v3) and fails before producing proposal P (artifact outcome
> pending). Run B restores v3 and produces P (artifact outcome produced). Run C, on another
> thread, produces Q. Execution panel: 1 failed, 2 completed → 2/3. Operation panel:
> artifacts 2/2 (P by B, Q by C); business outcomes 1/2 (P declined, Q accepted). Both
> numbers are correct — they answer different questions.

Since your comment, the guide has a section on the three levels of outcome and this worked
example (`ADOPTION_GUIDE.md`, "Worked Example"), and the RFC at `v0.3.0` defines the operation
identity and the exactly-once rule behind these numbers. The same harness now also exercises
the adjacent boundary — lost acknowledgement, authoritative readback, and an effect that stays
`unresolved` — because a business rate built on unresolved effects is not a rate. If the framing
is wrong, the example is the cheapest thing to change: it is prose, not a schema.

Disclosure: we implemented the RFC in a reference implementation to validate it; this reply
was drafted with AI assistance.
