"""Adversarial pass.

Written by asking "how would I break this if I wanted it to look good in a
demo and fail in production?" Each test is an attack. Where an attack succeeds,
the test documents the actual behaviour rather than asserting the behaviour I
wish it had.
"""

from __future__ import annotations

import asyncio
import math

import pytest

from smartdialer.config import DialMode, SafetyConfig, SafetyMode
from smartdialer.metrics.collector import M, MetricsCollector
from smartdialer.models.domain import ProviderEvent
from smartdialer.models.enums import AgentState, CallState, ProviderEventType
from smartdialer.safety.controller import SafetyController, _solve_max_unbound

from tests.conftest import CAMPAIGN
from tests.factories import build_app, estimate, pacing_request, snapshot


def controller(mode=SafetyMode.STRICT, **cfg):
    return SafetyController(SafetyConfig(mode=mode, **cfg), MetricsCollector())


def _event(call_id, kind, sequence, event_id=None):
    return ProviderEvent(
        event_id=event_id or f"{call_id}-{kind.value}-{sequence}",
        call_id=call_id, type=kind, sequence=sequence, timestamp=1.0,
        provider="provider-a", payload={},
    )


# ------------------------------------------------- attacking the arithmetic
@pytest.mark.parametrize("rate", [float("nan"), float("inf"), -1.0, 1e9, 0.0])
def test_garbage_answer_rate_from_the_estimator_cannot_inflate_the_cap(rate):
    """The estimator is upstream and untrusted. Feed it poison."""
    ctrl = controller(SafetyMode.BALANCED)
    snap = snapshot(available_agents=10)
    request = pacing_request(
        10_000, estimate=estimate(point=rate, planning_rate=rate)
    )
    decision = ctrl.evaluate(request, snap)
    assert 0 <= decision.approved_calls <= ctrl.max_unbound_in_flight(snap) + 1
    assert not math.isnan(decision.approved_calls)


@pytest.mark.parametrize("capacity,p,z", [
    (0, 0.5, 2.0), (-5, 0.5, 2.0), (10, 0.0, 2.0), (10, 1.0, 0.0),
    (10, 0.5, -1.0), (1e9, 0.5, 2.0),
])
def test_solver_never_returns_garbage(capacity, p, z):
    result = _solve_max_unbound(capacity, p, z)
    assert result >= 0
    assert not math.isnan(result)
    assert not math.isinf(result)


def test_negative_snapshot_counts_do_not_produce_negative_approvals():
    """Defensive: a counting bug upstream must not become a dialling bug."""
    ctrl = controller(SafetyMode.BALANCED)
    snap = snapshot(available_agents=-5, unbound_calls_in_flight=-3,
                    eligible_borrowers=-100)
    decision = ctrl.evaluate(pacing_request(100), snap)
    assert decision.approved_calls == 0


def test_absurd_config_does_not_crash_the_controller():
    ctrl = controller(SafetyMode.BALANCED, max_abandon_rate=0.0,
                      min_estimator_confidence=1.0, overshoot_sigmas=100.0,
                      max_overshoot_calls=0)
    decision = ctrl.evaluate(pacing_request(50), snapshot())
    assert decision.approved_calls >= 0


def test_zero_agents_approves_nothing_in_both_modes():
    for mode in (SafetyMode.STRICT, SafetyMode.BALANCED):
        ctrl = controller(mode)
        snap = snapshot(available_agents=0, wrap_up_agents=0, connected_agents=50)
        assert ctrl.evaluate(pacing_request(100), snap).approved_calls == 0


# ---------------------------------------------------- attacking the events
async def test_event_with_a_negative_sequence_is_rejected(tmp_path):
    app = build_app(tmp_path)
    await app.seed_campaign(agents=1, borrowers=5)
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create("C1", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)

    application = await app.events.handle(
        _event("C1", ProviderEventType.ANSWERED, -99)
    )
    assert application.outcome.name in ("STALE", "INVALID")
    call = await app.calls.get("C1")
    assert call.state is CallState.RESERVED
    app.close()


async def test_enormous_sequence_does_not_lock_out_later_events(tmp_path):
    """A carrier sending sequence=2^62 freezes the call.

    Every later event is stale, so the call cannot leave RINGING. The agent was
    already covered -- the lease sweep includes DIALING -- but the borrower
    reservation leaked permanently, because the borrower sweep skips borrowers
    whose call is still live.

    Closed by including RINGING in the stale-setup sweep, which is keyed on
    updated_at and asks the provider before ending anything. A call that is
    genuinely progressing is never selected.
    """
    app = build_app(tmp_path)
    await app.seed_campaign(agents=1, borrowers=5)
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create("C1", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)

    await app.events.handle(_event("C1", ProviderEventType.RINGING, 2 ** 62))
    later = await app.events.handle(_event("C1", ProviderEventType.ANSWERED, 5))
    assert later.outcome.name == "STALE"

    call = await app.calls.get("C1")
    assert call.state is CallState.RINGING

    stuck = await app.calls.find_stuck_in_setup(0.0)
    assert any(c.id == "C1" for c in stuck), "frozen call is invisible to recovery"

    await app.clock.sleep(app.config.reliability.call_setup_ttl_seconds + 5)
    await app.recovery.reconcile()
    call = await app.calls.get("C1")
    assert call.state.is_terminal(), "frozen call was never ended"
    app.close()


async def test_two_different_events_sharing_an_event_id(tmp_path):
    """A carrier reusing an idempotency key must not corrupt the call."""
    app = build_app(tmp_path)
    await app.seed_campaign(agents=1, borrowers=5)
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create("C1", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)

    await app.events.handle(_event("C1", ProviderEventType.RINGING, 1, "SAME"))
    second = await app.events.handle(
        _event("C1", ProviderEventType.COMPLETED, 2, "SAME")
    )
    assert second.outcome.name == "DUPLICATE"
    call = await app.calls.get("C1")
    assert call.state is CallState.RINGING, "ledger let a second event through"
    app.close()


async def test_terminal_call_cannot_be_reopened_by_any_event(tmp_path):
    app = build_app(tmp_path)
    await app.seed_campaign(agents=1, borrowers=5)
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create("C1", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)
    await app.calls.force_terminal("C1", CallState.FAILED, "test")

    for seq, kind in enumerate(ProviderEventType, start=100):
        await app.events.handle(_event("C1", kind, seq))
        call = await app.calls.get("C1")
        assert call.state is CallState.FAILED, f"{kind} reopened a terminal call"
    app.close()


# -------------------------------------------------- attacking the resources
async def test_releasing_an_agent_twice_is_harmless(tmp_path):
    app = build_app(tmp_path)
    await app.seed_campaign(agents=1, borrowers=5)
    agent = (await app.agents.list_by_state(CAMPAIGN, AgentState.AVAILABLE))[0]
    await app.agents.reserve_agent(agent.id, "R1")

    assert await app.agents.release(agent.id, "first") is True
    assert await app.agents.release(agent.id, "second") is False
    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.AVAILABLE] == 1
    app.close()


async def test_exhausted_borrower_queue_stops_dialling_cleanly(tmp_path):
    """Three borrowers, three attempts each, then nothing. No spinning."""
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.0)
    await app.seed_campaign(agents=5, borrowers=3, max_attempts=1)
    await app.start()
    await app.clock.sleep(60)
    await app.stop()
    await app.events.drain()

    assert await app.borrowers.count_eligible(CAMPAIGN) == 0
    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.RESERVED] + counts[AgentState.DIALING] == 0
    app.close()


async def test_campaign_with_zero_borrowers_does_not_burn_agents(tmp_path):
    """No borrowers, so no calls -- and no agents consumed either.

    This test found the dial path's cancellation gap: run_progressive reserves
    an agent, finds no borrower and releases it, and cancelling the
    orchestrator between those steps left the agent RESERVED until its lease
    expired. Failed roughly one run in three. The dial path is now shielded and
    drained the same way the event path is, so agents are back to AVAILABLE by
    the time stop() returns -- no reconciliation needed.
    """
    app = build_app(tmp_path, mode=DialMode.PREDICTIVE)
    await app.seed_campaign(agents=10, borrowers=0)
    await app.start()
    await app.clock.sleep(30)
    await app.stop()

    assert app.metrics.counter(M.CALLS_INITIATED) == 0

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.AVAILABLE] == 10, (
        f"agents left mid-dial by shutdown: {counts}"
    )
    app.close()


async def test_all_agents_offline_mid_campaign_stops_dialling(tmp_path):
    app = build_app(tmp_path, mode=DialMode.PREDICTIVE, safety=SafetyMode.BALANCED)
    await app.seed_campaign(agents=10, borrowers=200)
    await app.start()
    await app.clock.sleep(30)
    await app.agents.force_offline(CAMPAIGN, 10, only_available=False)
    # Calls already mid-wave when the agents vanished still land, so the first
    # window after the shock is allowed a small tail. The second must be flat.
    await app.clock.sleep(15)
    settled = app.metrics.counter(M.CALLS_INITIATED)
    await app.clock.sleep(30)
    after = app.metrics.counter(M.CALLS_INITIATED)
    await app.stop()
    await app.events.drain()

    assert after == settled, "kept dialling with nobody to answer"
    app.close()


# ------------------------------------------------ attacking the safety story
async def test_recovery_asks_the_carrier_before_killing_a_silent_call(tmp_path):
    """Silent carrier: calls accepted, no events ever delivered to us.

    The interesting part is what recovery does *not* do. It asks the provider
    about each stale call, and a call the carrier still reports as live is left
    alone -- its agent stays busy, correctly, because there is a borrower on the
    other end. Only calls the carrier has no record of are failed locally and
    their agents released. Blindly resetting everything to AVAILABLE would hang
    up on live conversations.
    """
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE)
    await app.seed_campaign(agents=5, borrowers=50)
    for provider in app.providers:
        provider.set_event_sink(lambda event: asyncio.sleep(0))

    await app.start()
    await app.clock.sleep(30)
    await app.stop()

    await app.clock.sleep(app.config.reliability.call_setup_ttl_seconds + 10)
    report = await app.recovery.reconcile()

    # Whether a given call is "still live at the carrier" depends on where the
    # mock's own lifecycle got to, so the counts vary run to run. What must hold
    # every time is the policy: each call is either left alone or failed *and*
    # its agent released -- never failed while its agent stays busy.
    assert report.calls_failed + report.calls_left_alone > 0
    assert report.agents_released == report.calls_failed

    # Every agent still busy is busy against a call the carrier confirms is up.
    for agent in await app.agents.list_by_state(CAMPAIGN, AgentState.DIALING):
        call = await app.calls.get(agent.current_call_id)
        assert not call.state.is_terminal()
        assert call.provider_call_id is not None
    app.close()


async def test_balanced_cannot_be_reached_without_asking_for_it():
    """The default must be the safe mode, not the fast one."""
    assert SafetyConfig().mode is SafetyMode.STRICT


async def test_strict_mode_ignores_the_overshoot_configuration():
    """Turning the BALANCED knobs up must not loosen STRICT."""
    tame = controller(SafetyMode.STRICT, overshoot_sigmas=0.0,
                      max_overshoot_calls=0)
    wild = controller(SafetyMode.STRICT, overshoot_sigmas=10.0,
                      max_overshoot_calls=10_000)
    snap = snapshot(available_agents=12)
    assert tame.max_unbound_in_flight(snap) == wild.max_unbound_in_flight(snap) == 12


async def test_abandonment_is_counted_not_hidden(tmp_path):
    """Every path that abandons a call must increment the same counter.

    Three of them exist: the bridge grace expiring, the reconciler finding an
    orphaned answer, and reaping a call whose agent went offline. If any one
    of them stopped counting, the abandonment numbers would quietly improve.
    """
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[2] / "src" / "smartdialer"
    incrementers = [
        path.relative_to(src).as_posix()
        for path in src.rglob("*.py")
        if "M.CALLS_ABANDONED" in path.read_text()
    ]
    assert set(incrementers) >= {
        "events/processor.py", "reliability/recovery.py",
    }, incrementers
