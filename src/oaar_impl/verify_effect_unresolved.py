import json
import sys

from oaar_impl.demo_outcome_dedup import prom_counter
from oaar_impl.verify_langgraph import attr_value, fetch_trace, iter_events, iter_spans
from oaar_impl.verify_pipeline import poll

CONTEXT_PATH = "/tmp/oaar-effect-unresolved-context.json"

RUN_SPAN = "agent.run"
EFFECT_SPAN = "agent.effect"
OBSERVE_SPAN = "agent.effect.observe"
COMPENSATE_SPAN = "agent.compensate"
RESOLVED_EVENT = "agent.effect.resolved"
REQUESTED_EVENT = "agent.compensation.requested"

A_EFFECT_ID = "agent.effect.id"
A_ATTEMPT = "agent.effect.attempt.id"
A_STATUS = "agent.effect.confirmation.status"
A_REASON = "agent.effect.resolution.reason"
A_EVIDENCE_VERDICT = "agent.effect.evidence.verdict"
A_COMP_STATE = "agent.compensation.state"
A_COMP_OUTCOME = "agent.compensation.outcome"


def _spans(doc, name):
    return [s for s in iter_spans(doc or {}) if s.get("name") == name]


def _events(doc, name):
    out = []
    for span in iter_spans(doc or {}):
        for event in iter_events(span):
            if event.get("name") == name:
                out.append(event)
    return out


def _resolution(doc, effect_id):
    for event in _events(doc, RESOLVED_EVENT):
        if attr_value(event.get("attributes"), A_EFFECT_ID) == effect_id:
            return event
    return None


def check_confirmed_and_retry(ctx):
    def fn():
        case = ctx["confirmed"]
        doc = fetch_trace(case["trace"])
        if doc is None:
            return False, "confirmed trace not in Tempo yet"
        event = _resolution(doc, case["effect_id"])
        if event is None:
            return False, "no agent.effect.resolved event for the confirmed effect"
        status = attr_value(event.get("attributes"), A_STATUS)
        reason = attr_value(event.get("attributes"), A_REASON)
        if status != "confirmed" or reason != "confirmed":
            return False, f"expected confirmed/confirmed, got {status}/{reason}"
        attempts = _spans(doc, EFFECT_SPAN)
        ids = [attr_value(s.get("attributes"), A_ATTEMPT) for s in attempts]
        logical = {attr_value(s.get("attributes"), A_EFFECT_ID) for s in attempts}
        if len(attempts) != 2 or len(set(ids)) != 2 or len(logical) != 1:
            return False, (
                f"expected 2 attempts sharing one logical effect id, got {len(attempts)} spans, "
                f"attempt ids={ids}, effect ids={logical}"
            )
        return True, "fresh readback resolved it; 2 attempts, 1 logical effect id, 2 attempt ids"

    return fn


def check_contradicted_and_compensated(ctx):
    def fn():
        case = ctx["contradicted"]
        doc = fetch_trace(case["trace"])
        if doc is None:
            return False, "contradicted trace not in Tempo yet"
        event = _resolution(doc, case["effect_id"])
        if event is None:
            return False, "no agent.effect.resolved event"
        status = attr_value(event.get("attributes"), A_STATUS)
        if status != "unconfirmed":
            return False, f"contradicted readback must be unconfirmed, got {status!r}"
        requested = _events(doc, REQUESTED_EVENT)
        if not requested:
            return False, "compensation was not recorded as requested"
        comps = _spans(doc, COMPENSATE_SPAN)
        if not comps:
            return False, "no agent.compensate span"
        state = attr_value(comps[0].get("attributes"), A_COMP_STATE)
        outcome = attr_value(comps[0].get("attributes"), A_COMP_OUTCOME)
        if state != "verified" or outcome != "succeeded":
            return False, f"expected verified/succeeded, got {state}/{outcome}"
        return True, "unconfirmed effect, compensation requested then verified (succeeded)"

    return fn


def check_stale_stays_unresolved(ctx):
    def fn():
        case = ctx["stale"]
        doc = fetch_trace(case["trace"])
        if doc is None:
            return False, "stale trace not in Tempo yet"
        event = _resolution(doc, case["effect_id"])
        if event is None:
            return False, "no agent.effect.resolved event"
        status = attr_value(event.get("attributes"), A_STATUS)
        reason = attr_value(event.get("attributes"), A_REASON)
        if status != "unresolved" or reason != "stale_evidence":
            return False, f"expected unresolved/stale_evidence, got {status}/{reason}"
        observes = _spans(doc, OBSERVE_SPAN)
        if not observes:
            return False, "no observation recorded — the case must have one, just stale"
        verdict = attr_value(observes[0].get("attributes"), A_EVIDENCE_VERDICT)
        if verdict != "CONFIRMED":
            return False, f"the stale observation should carry a CONFIRMED verdict, got {verdict}"
        return True, "authoritative CONFIRMED readback, stale -> still unresolved (not promoted)"

    return fn


def check_missing_stays_unresolved(ctx):
    def fn():
        case = ctx["missing"]
        doc = fetch_trace(case["trace"])
        if doc is None:
            return False, "missing trace not in Tempo yet"
        event = _resolution(doc, case["effect_id"])
        if event is None:
            return False, "no agent.effect.resolved event"
        status = attr_value(event.get("attributes"), A_STATUS)
        reason = attr_value(event.get("attributes"), A_REASON)
        if status != "unresolved" or reason != "no_observation":
            return False, f"expected unresolved/no_observation, got {status}/{reason}"
        if _spans(doc, OBSERVE_SPAN):
            return False, "an observation span exists in the no-observation case"
        return True, "lost ACK with no readback -> unresolved, never inferred from transport"

    return fn


def check_estimated_and_counters(ctx):
    def fn():
        case = ctx["estimated"]
        doc = fetch_trace(case["trace"])
        if doc is None:
            return False, "estimated trace not in Tempo yet"
        event = _resolution(doc, case["effect_id"])
        if event is None:
            return False, "no agent.effect.resolved event"
        status = attr_value(event.get("attributes"), A_STATUS)
        reason = attr_value(event.get("attributes"), A_REASON)
        if status != "unresolved" or reason != "source_not_authorized":
            return False, f"expected unresolved/source_not_authorized, got {status}/{reason}"
        base = ctx["baseline"]["resolutions"]
        deltas = {
            s: prom_counter("oaar_effect_resolutions_total",
                            agent_effect_confirmation_status=s) - base.get(s, 0.0)
            for s in ("confirmed", "unconfirmed", "unresolved")
        }
        if deltas != {"confirmed": 1, "unconfirmed": 1, "unresolved": 3}:
            return False, f"unexpected resolution counters: {deltas}"
        return True, "model estimate cannot resolve; counters 1 confirmed / 1 unconfirmed / 3 unresolved"

    return fn


def check_unverified_compensation(ctx):
    def fn():
        case = ctx["missing"]
        doc = fetch_trace(case["trace"])
        if doc is None:
            return False, "missing trace not in Tempo yet"
        comps = _spans(doc, COMPENSATE_SPAN)
        if not comps:
            return False, "no compensation span for the unverified case"
        state = attr_value(comps[0].get("attributes"), A_COMP_STATE)
        outcome = attr_value(comps[0].get("attributes"), A_COMP_OUTCOME)
        if state != "unverified":
            return False, f"expected state=unverified, got {state!r}"
        if outcome is not None:
            return False, f"unverified compensation must not carry an outcome, got {outcome!r}"
        base = ctx["baseline"]["compensations"].get("unverified", 0.0)
        delta = prom_counter("oaar_compensations_total", agent_compensation_state="unverified") - base
        if delta != 1:
            return False, f"expected +1 unverified compensation, got +{delta}"
        return True, "compensation requested but never verified: state=unverified, no outcome"

    return fn


def main() -> int:
    with open(CONTEXT_PATH) as f:
        ctx = json.load(f)
    checks = [
        ("confirmed: fresh readback + retry identity", check_confirmed_and_retry(ctx)),
        ("contradicted: linked, verified compensation", check_contradicted_and_compensated(ctx)),
        ("stale readback stays unresolved", check_stale_stays_unresolved(ctx)),
        ("no readback stays unresolved", check_missing_stays_unresolved(ctx)),
        ("model estimate cannot resolve; counters", check_estimated_and_counters(ctx)),
        ("compensation requested != verified", check_unverified_compensation(ctx)),
    ]
    width = max(len(name) for name, _ in checks)
    failed = 0
    for name, fn in checks:
        ok, detail = poll(fn)
        if not ok:
            failed += 1
        print(f"{name.ljust(width)}  {'PASS' if ok else 'FAIL'}  {detail}")
    total = len(checks)
    print(f"\n{total - failed}/{total} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
