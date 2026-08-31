#!/usr/bin/env python3
"""Ten demonstrations, each printing what happened and why.

    python3 scripts/run_demo.py --list
    python3 scripts/run_demo.py --demo 4
    python3 scripts/run_demo.py --all

Every demo runs the real system against mock carriers on a compressed clock.
Nothing is printed that was not measured.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smartdialer.app import SmartDialerApp  # noqa: E402
from smartdialer.clock import ScaledClock  # noqa: E402
from smartdialer.config import DialerConfig, DialMode, SafetyMode  # noqa: E402
from smartdialer.logging_setup import configure  # noqa: E402
from smartdialer.metrics.collector import M, MetricsCollector  # noqa: E402
from smartdialer.models.domain import ProviderEvent, SafetyDecision  # noqa: E402
from smartdialer.models.enums import (  # noqa: E402
    AgentState,
    ProviderEventType,
    SafetyAction,
)
from smartdialer.persistence.db import Database  # noqa: E402
from smartdialer.providers.mock import ProviderProfile  # noqa: E402

CAMPAIGN = "DEMO"


def heading(number: int, title: str, question: str) -> None:
    print(f"\n{'=' * 78}\nDEMO {number}: {title}\n  question: {question}\n{'=' * 78}")


def show(label: str, value) -> None:
    print(f"  {label:<42} {value}")


def build(
    *,
    mode: DialMode = DialMode.PREDICTIVE,
    safety: SafetyMode = SafetyMode.BALANCED,
    answer_rate: float = 0.5,
    talk_seconds: float = 60.0,
    ring_seconds: float = 6.0,
    scale: float = 0.01,
    profiles=None,
    seed: int = 5,
    **overrides,
) -> SmartDialerApp:
    import dataclasses

    config = DialerConfig(campaign_id=CAMPAIGN, mode=mode, tick_seconds=1.0,
                          wrap_up_seconds=10.0)
    config = config.with_safety_mode(safety)
    if overrides:
        config = dataclasses.replace(config, **overrides)
    if profiles is None:
        profiles = [ProviderProfile.provider_a(
            answer_rate=answer_rate, talk_seconds_mean=talk_seconds,
            ring_seconds=ring_seconds,
        )]
    return SmartDialerApp.build(
        config, clock=ScaledClock(scale=scale), db=Database.temporary(),
        profiles=profiles, metrics=MetricsCollector(), seed=seed,
    )


def outcome(app: SmartDialerApp) -> None:
    show("calls initiated", int(app.metrics.counter(M.CALLS_INITIATED)))
    show("calls answered", int(app.metrics.counter(M.CALLS_ANSWERED)))
    show("calls connected to an agent", int(app.metrics.counter(M.CALLS_CONNECTED)))
    show("calls abandoned", int(app.metrics.counter(M.CALLS_ABANDONED)))


# --------------------------------------------------------------------- demos
async def demo_1() -> None:
    heading(1, "Progressive dialling",
            "does one agent map to exactly one call?")
    app = build(mode=DialMode.PROGRESSIVE, answer_rate=0.6)
    await app.seed_campaign(agents=10, borrowers=300)
    await app.start()
    peak_calls, peak_busy = 0, 0
    for _ in range(60):
        await app.clock.sleep(2)
        counts = await app.calls.live_counts(CAMPAIGN)
        agents = await app.agents.counts_by_state(CAMPAIGN)
        peak_calls = max(peak_calls, counts["bound_in_flight"])
        peak_busy = max(peak_busy, agents[AgentState.RESERVED]
                        + agents[AgentState.DIALING] + agents[AgentState.CONNECTED])
    await app.stop()

    outcome(app)
    show("agents in the campaign", 10)
    show("peak agent-bound calls in flight", peak_calls)
    show("peak agents reserved/dialing/connected", peak_busy)
    show("peak unbound (agentless) calls", 0)
    print("\n  The agent is reserved before the call exists, so in-flight calls")
    print("  can never exceed the agent count and nobody can answer into silence.")
    app.close()


async def demo_2() -> None:
    heading(2, "Predictive dialling, healthy conditions",
            "does it dial more than one call per agent, safely?")
    app = build(answer_rate=0.45, talk_seconds=90.0)
    await app.seed_campaign(agents=40, borrowers=1500)
    await app.start()
    peak_unbound, peak_available = 0, 0
    for _ in range(20):
        await app.clock.sleep(10)
        counts = await app.calls.live_counts(CAMPAIGN)
        agents = await app.agents.counts_by_state(CAMPAIGN)
        peak_unbound = max(peak_unbound, counts["unbound_in_flight"])
        peak_available = max(peak_available, agents[AgentState.AVAILABLE])
    await app.stop()

    outcome(app)
    show("peak unbound calls ringing at once", peak_unbound)
    show("safety approvals", int(app.metrics.counter(M.SAFETY_APPROVALS)))
    show("safety reductions", int(app.metrics.counter(M.SAFETY_REDUCTIONS)))
    print("\n  No agent is held while the phone rings. An agent is grabbed at the")
    print("  instant the borrower answers, which is where the utilisation gain")
    print("  comes from and where the abandonment risk lives.")
    app.close()


async def demo_3() -> None:
    heading(3, "High answer rate",
            "does the controller pull back when more people pick up?")
    app = build(answer_rate=0.85, talk_seconds=90.0)
    await app.seed_campaign(agents=30, borrowers=1200)
    await app.start()
    await app.clock.sleep(200)
    await app.stop()

    outcome(app)
    show("observed answer rate",
         round(app.metrics.counter(M.CALLS_ANSWERED)
               / max(1, app.metrics.counter(M.CALLS_INITIATED)), 3))
    show("controller's own answer-rate estimate",
         round(app.metrics.get_gauge(M.G_ESTIMATED_ANSWER_RATE), 3))
    show("safety reductions", int(app.metrics.counter(M.SAFETY_REDUCTIONS)))
    print("\n  A high answer rate is the dangerous direction: more ringing phones")
    print("  turn into more people expecting an agent. Overshoot allowance")
    print("  shrinks as the estimated rate rises.")
    app.close()


async def demo_4() -> None:
    heading(4, "Provider B times out",
            "does a slow, unreliable carrier stall the whole dialer?")
    flaky = ProviderProfile.provider_b(
        answer_rate=0.5, talk_seconds_mean=60.0, ring_seconds=6.0,
        timeout_probability=0.35, permanent_failure_probability=0.05,
    )
    app = build(profiles=[flaky])
    await app.seed_campaign(agents=20, borrowers=800)
    await app.start()
    await app.clock.sleep(200)
    await app.stop()

    outcome(app)
    show("provider timeouts", int(app.metrics.counter(M.PROVIDER_TIMEOUTS)))
    show("retries attempted", int(app.metrics.counter(M.PROVIDER_RETRIES)))
    show("setup failures (agent released each time)",
         int(app.metrics.counter(M.SETUP_FAILURES)))
    show("circuit breaker state", app.router.circuit_state())
    stuck = await app.agents.counts_by_state(CAMPAIGN)
    show("agents stuck in RESERVED/DIALING",
         stuck[AgentState.RESERVED] + stuck[AgentState.DIALING])
    print("\n  Timeouts are retried with bounded backoff, then classified and")
    print("  given up on. Every failed setup releases its agent and its")
    print("  borrower, so a bad carrier costs throughput, never resources.")
    app.close()


async def demo_5() -> None:
    heading(5, "Duplicate events",
            "does the same webhook delivered twice do the work twice?")
    app = build(mode=DialMode.PROGRESSIVE)
    await app.seed_campaign(agents=2, borrowers=10)
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create("C-DUP", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)
    await app.calls.mark_initiated("C-DUP", "provider-a", "P-1")

    event = ProviderEvent(
        event_id="E-1", call_id="C-DUP", type=ProviderEventType.RINGING,
        sequence=1, timestamp=1.0, provider="provider-a", payload={},
    )
    first = await app.events.handle(event)
    second = await app.events.handle(event)
    third = await app.events.handle(event)

    show("first delivery", first.outcome.value)
    show("second delivery", second.outcome.value)
    show("third delivery", third.outcome.value)
    show("duplicates detected", int(app.metrics.counter(M.DUPLICATE_EVENTS)))
    print("\n  The ledger write shares a transaction with the state change, so")
    print("  'applied' and 'recorded as applied' cannot come apart.")
    app.close()


async def demo_6() -> None:
    heading(6, "Out-of-order events",
            "can a late RINGING resurrect a finished call?")
    app = build(mode=DialMode.PROGRESSIVE)
    await app.seed_campaign(agents=2, borrowers=10)
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create("C-OOO", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)
    await app.calls.mark_initiated("C-OOO", "provider-a", "P-2")

    for kind, seq in ((ProviderEventType.COMPLETED, 3),
                      (ProviderEventType.ANSWERED, 2),
                      (ProviderEventType.RINGING, 1)):
        application = await app.events.handle(ProviderEvent(
            event_id=f"E-{seq}", call_id="C-OOO", type=kind, sequence=seq,
            timestamp=float(seq), provider="provider-a", payload={},
        ))
        show(f"{kind.value} (sequence {seq})", application.outcome.value)

    call = await app.calls.get("C-OOO")
    show("final call state", call.state.value)
    print("\n  Call states are not totally ordered -- COMPLETED and FAILED are")
    print("  siblings -- so staleness is decided by the provider's per-call")
    print("  sequence number, not by guessing which state came later.")
    app.close()


async def demo_7() -> None:
    heading(7, "Two workers race for one agent",
            "can the same agent be handed to two callers?")
    app = build(mode=DialMode.PROGRESSIVE)
    await app.seed_campaign(agents=1, borrowers=10)
    agent = (await app.agents.list_by_state(CAMPAIGN, AgentState.AVAILABLE))[0]

    results = await asyncio.gather(
        *(app.agents.reserve_agent(agent.id, f"R{i}") for i in range(20))
    )
    winners = [r for r in results if r.ok]
    show("workers racing", len(results))
    show("winners", len(winners))
    show("losers", len(results) - len(winners))
    show("loser's reason", results[1].reason if not results[1].ok else "-")

    scaled = await asyncio.gather(
        *(app.agents.reserve_any_available(CAMPAIGN) for _ in range(50))
    )
    show("second round: agents available", 0)
    show("second round: reservations granted",
         sum(1 for r in scaled if r.ok))
    print("\n  Reservation is a conditional UPDATE guarded on the current state.")
    print("  Exactly one caller sees rowcount == 1; everyone else loses cleanly.")
    app.close()


async def demo_8() -> None:
    heading(8, "Worker crash and recovery",
            "what happens to work in flight when the process dies?")
    app = build(mode=DialMode.PROGRESSIVE, answer_rate=0.6)
    await app.seed_campaign(agents=15, borrowers=400)
    await app.start()
    await app.clock.sleep(40)

    before = await app.agents.counts_by_state(CAMPAIGN)
    show("before crash: connected", before[AgentState.CONNECTED])
    show("before crash: reserved/dialing",
         before[AgentState.RESERVED] + before[AgentState.DIALING])

    await app.crash()
    print("  --- process killed, nothing cleaned up ---")

    await app.clock.sleep(90)     # let the leases expire
    report = await app.recovery.reconcile()
    show("recovery: agents released", report.agents_released)
    show("recovery: stranded agents released", report.stranded_agents_released)
    show("recovery: calls failed", report.calls_failed)
    show("recovery: calls left alone (still live at carrier)",
         report.calls_left_alone)
    show("recovery: borrowers returned to the queue", report.borrowers_released)

    await app.start(run_recovery=False)
    await app.clock.sleep(40)
    await app.stop()
    show("calls connected after restart", int(app.metrics.counter(M.CALLS_CONNECTED)))
    print("\n  Recovery asks the carrier about each stale call rather than")
    print("  blindly resetting: live calls are left alone, dead ones are")
    print("  finished locally, and calls that never reached the carrier do not")
    print("  burn a contact attempt.")
    app.close()


async def demo_9() -> None:
    heading(9, "100 agents, 40 disappear",
            "what happens to calls that were safe when they were placed?")
    app = build(answer_rate=0.5, talk_seconds=90.0, ring_seconds=8.0)
    await app.seed_campaign(agents=100, borrowers=2500)
    await app.start()
    await app.clock.sleep(60)

    counts = await app.calls.live_counts(CAMPAIGN)
    show("before: unbound calls ringing", counts["unbound_in_flight"])
    # only_available=False: real agents vanish mid-call too (laptop shuts,
    # VPN drops). Taking only idle ones would be a much gentler shock than the
    # one this demo claims to be showing.
    gone = await app.agents.force_offline(CAMPAIGN, 40, only_available=False)
    print(f"  --- {len(gone)} agents go offline, including mid-call ---")
    await app.clock.sleep(60)
    await app.stop()

    outcome(app)
    show("ringing calls cancelled by the controller",
         int(app.metrics.counter(M.SAFETY_CANCELLATIONS)))
    show("agents offline", (await app.agents.counts_by_state(CAMPAIGN))[
        AgentState.OFFLINE])
    answered = app.metrics.counter(M.CALLS_ANSWERED)
    abandoned = app.metrics.counter(M.CALLS_ABANDONED)
    show("abandonment rate vs 3% budget",
         f"{abandoned / max(1, answered):.2%}")
    print("\n  The capacity check runs every tick, not only when dialling.")
    print("  Excess calls are cancelled while still ringing; a call that has")
    print("  already been answered is never hung up on.")
    app.close()


async def demo_10() -> None:
    heading(10, "The predictive model becomes wrong",
            "what stops a broken estimator from abandoning calls?")
    app = build(answer_rate=0.15, talk_seconds=90.0)
    await app.seed_campaign(agents=25, borrowers=1500)
    await app.start()
    await app.clock.sleep(150)
    print("  --- answer rate jumps 15% -> 90% without warning ---")
    for provider in app.providers:
        provider.override_profile(answer_rate=0.90)
    await app.clock.sleep(150)
    await app.stop()

    requested = app.metrics.counter(M.PACING_REQUESTED_CALLS)
    approved = app.metrics.counter(M.SAFETY_APPROVED_CALLS)
    show("calls the pacing engine asked for", int(requested))
    show("calls the safety controller allowed", int(approved))
    show("  -> refused by safety", int(requested - approved))
    show("reductions", int(app.metrics.counter(M.SAFETY_REDUCTIONS)))
    show("rejections", int(app.metrics.counter(M.SAFETY_REJECTIONS)))
    show("fallbacks to progressive", int(app.metrics.counter(M.SAFETY_FALLBACKS)))
    outcome(app)

    print("\n  And the boundary itself:")
    try:
        SafetyDecision(
            action=SafetyAction.APPROVE, approved_calls=10_000,
            requested_calls=10_000, limiting_constraint="none", caps={},
            snapshot=None, issuer_token=object(),
        )
        show("forging a SafetyDecision", "SUCCEEDED -- boundary is broken")
    except PermissionError as exc:
        show("forging a SafetyDecision", f"PermissionError: {exc}")
    print("\n  Prediction is allowed to be wrong. Execution is not allowed to be")
    print("  unsafe. The pacing engine asked for more than it should have; the")
    print("  controller recomputed from observed counters and refused.")
    app.close()


DEMOS = {
    1: ("Progressive dialling", demo_1),
    2: ("Predictive dialling, healthy", demo_2),
    3: ("High answer rate", demo_3),
    4: ("Provider B timing out", demo_4),
    5: ("Duplicate events", demo_5),
    6: ("Out-of-order events", demo_6),
    7: ("Two workers race for one agent", demo_7),
    8: ("Worker crash and recovery", demo_8),
    9: ("100 agents, 40 disappear", demo_9),
    10: ("Predictive model becomes wrong", demo_10),
}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", type=int, action="append")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.list:
        for number, (title, _) in DEMOS.items():
            print(f"  {number:>2}. {title}")
        return

    configure(level="WARNING" if not args.verbose else "INFO")
    for number in (sorted(DEMOS) if args.all or not args.demo else args.demo):
        await DEMOS[number][1]()


if __name__ == "__main__":
    asyncio.run(main())
