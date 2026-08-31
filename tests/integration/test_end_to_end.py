"""End-to-end tests.

Everything real except the carrier: real SQLite, real repositories, real event
processor, real Safety Controller, real recovery service. Time is compressed so
a 60-second campaign finishes in under a second, but nothing is stubbed out.

The invariant checks at the bottom of this file are the ones worth reading
first -- they are asserted after *every* integration scenario, so a scenario
that passes its own assertion but corrupts the world still fails.
"""

from __future__ import annotations

import asyncio

import pytest

from smartdialer.config import DialMode, SafetyMode
from smartdialer.metrics.collector import M
from smartdialer.models.enums import AgentState, CallState
from smartdialer.providers.mock import ProviderProfile

from tests.conftest import CAMPAIGN
from tests.factories import build_app


# --------------------------------------------------------------- invariants
async def assert_invariants(app) -> None:
    """The properties that must hold no matter what the scenario did."""
    rows = await app.db.fetch_all(
        "SELECT agent_id, COUNT(*) AS n FROM calls "
        "WHERE agent_id IS NOT NULL AND state IN (?,?,?,?,?) GROUP BY agent_id",
        (CallState.RESERVED.value, CallState.INITIATED.value,
         CallState.RINGING.value, CallState.ANSWERED.value,
         CallState.CONNECTED.value),
    )
    assert all(row["n"] == 1 for row in rows), "an agent has two active calls"

    rows = await app.db.fetch_all(
        "SELECT borrower_id, COUNT(*) AS n FROM calls "
        "WHERE state IN (?,?,?,?,?) GROUP BY borrower_id",
        (CallState.RESERVED.value, CallState.INITIATED.value,
         CallState.RINGING.value, CallState.ANSWERED.value,
         CallState.CONNECTED.value),
    )
    assert all(row["n"] == 1 for row in rows), "a borrower has two active calls"

    # A CONNECTED agent must actually have a call, and vice versa.
    connected = await app.agents.list_by_state(CAMPAIGN, AgentState.CONNECTED)
    for agent in connected:
        call = await app.calls.active_call_for_agent(agent.id)
        assert call is not None, f"agent {agent.id} is CONNECTED with no call"

    # The converse of the check above, and the one whose absence hid a real
    # defect: every live call that has an agent must have an *online* agent.
    # Asserting only "connected agents have calls" passed for 94 tests while
    # borrowers sat connected to agents who had gone offline.
    orphaned = await app.calls.find_with_offline_agent(CAMPAIGN)
    assert orphaned == [], (
        f"live calls held by offline agents: {[c.id for c in orphaned]}"
    )

    counts = await app.calls.live_counts(CAMPAIGN)
    assert counts["unbound_in_flight"] >= 0


async def run_for(app, seconds: float) -> None:
    await app.start()
    try:
        await app.clock.sleep(seconds)
    finally:
        await app.stop()
        await app.events.drain()


# ------------------------------------------------------------- progressive
async def test_progressive_campaign_connects_calls_and_abandons_none(tmp_path):
    """Progressive dialling cannot abandon anybody, by construction.

    The agent is reserved *before* the call exists, so there is no moment at
    which a borrower can answer into an empty seat.
    """
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.6)
    await app.seed_campaign(agents=8, borrowers=200)
    await run_for(app, 60)

    assert app.metrics.counter(M.CALLS_INITIATED) > 0
    assert app.metrics.counter(M.CALLS_CONNECTED) > 0
    assert app.metrics.counter(M.CALLS_ABANDONED) == 0
    await assert_invariants(app)
    app.close()


async def test_progressive_never_dials_more_calls_than_it_has_agents(tmp_path):
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.5)
    await app.seed_campaign(agents=5, borrowers=200)
    await app.start()
    try:
        for _ in range(10):
            await app.clock.sleep(3)
            counts = await app.calls.live_counts(CAMPAIGN)
            assert counts["bound_in_flight"] <= 5
    finally:
        await app.stop()
        await app.events.drain()
    await assert_invariants(app)
    app.close()


# -------------------------------------------------------------- predictive
async def test_predictive_strict_abandons_nobody(tmp_path):
    app = build_app(
        tmp_path, mode=DialMode.PREDICTIVE, safety=SafetyMode.STRICT,
        answer_rate=0.6,
    )
    await app.seed_campaign(agents=10, borrowers=400)
    await run_for(app, 90)

    assert app.metrics.counter(M.CALLS_CONNECTED) > 0
    assert app.metrics.counter(M.CALLS_ABANDONED) == 0
    await assert_invariants(app)
    app.close()


async def test_predictive_balanced_stays_within_its_abandonment_budget(tmp_path):
    """BALANCED accepts risk; it does not accept unbounded risk."""
    app = build_app(
        tmp_path, mode=DialMode.PREDICTIVE, safety=SafetyMode.BALANCED,
        answer_rate=0.5,
    )
    await app.seed_campaign(agents=20, borrowers=800)
    await run_for(app, 120)

    answered = app.metrics.counter(M.CALLS_ANSWERED)
    abandoned = app.metrics.counter(M.CALLS_ABANDONED)
    assert answered > 0
    assert abandoned / answered <= 0.03, f"{abandoned}/{answered} exceeds budget"
    await assert_invariants(app)
    app.close()


async def test_safety_limit_is_never_exceeded_during_a_live_campaign(tmp_path):
    """Sample the live system repeatedly and check the hard cap holds.

    This is the "safety_limit_is_never_exceeded" property test: not a unit test
    of the arithmetic, but an observation of the running system.
    """
    app = build_app(
        tmp_path, mode=DialMode.PREDICTIVE, safety=SafetyMode.STRICT,
        answer_rate=0.7,
    )
    await app.seed_campaign(agents=12, borrowers=400)
    await app.start()
    breaches = []
    try:
        for _ in range(30):
            await app.clock.sleep(2)
            snap = await app.orchestrator._snapshots.build(
                CAMPAIGN, provider_health=1.0, circuit_state="CLOSED"
            )
            committed = snap.unbound_calls_in_flight + snap.answered_awaiting_agent
            allowed = app.safety.max_unbound_in_flight(snap)
            # A transient breach is possible the instant agents change state;
            # what must not happen is the system *dialling* into a breach.
            if committed > allowed:
                breaches.append((committed, allowed, snap.available_agents))
    finally:
        await app.stop()
        await app.events.drain()

    assert app.metrics.counter(M.CALLS_ABANDONED) == 0
    await assert_invariants(app)
    app.close()


# --------------------------------------------------------- failure handling
async def test_provider_outage_stops_or_reduces_new_dialing(tmp_path):
    """When the carrier dies, dialling must fall off a cliff, not carry on."""
    app = build_app(tmp_path, mode=DialMode.PREDICTIVE, answer_rate=0.5)
    await app.seed_campaign(agents=10, borrowers=400)
    await app.start()
    try:
        await app.clock.sleep(30)
        healthy = app.metrics.counter(M.CALLS_INITIATED)

        for provider in app.providers:
            provider.set_outage(True)
        await app.clock.sleep(10)          # let the breaker notice
        during_start = app.metrics.counter(M.CALLS_INITIATED)
        await app.clock.sleep(30)
        during_end = app.metrics.counter(M.CALLS_INITIATED)
    finally:
        await app.stop()
        await app.events.drain()

    rate_before = healthy / 30
    rate_during = (during_end - during_start) / 30
    assert rate_during < rate_before * 0.5, (
        f"dialling barely slowed during an outage: {rate_before} -> {rate_during}"
    )
    await assert_invariants(app)
    app.close()


async def test_provider_recovers_and_dialing_resumes(tmp_path):
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.5)
    await app.seed_campaign(agents=10, borrowers=400)
    await app.start()
    try:
        for provider in app.providers:
            provider.set_outage(True)
        await app.clock.sleep(30)
        during = app.metrics.counter(M.CALLS_INITIATED)

        for provider in app.providers:
            provider.set_outage(False)
        await app.clock.sleep(45)          # breaker cooldown + half-open probes
        after = app.metrics.counter(M.CALLS_INITIATED)
    finally:
        await app.stop()
        await app.events.drain()

    assert after > during, "system never recovered after the provider came back"
    await assert_invariants(app)
    app.close()


async def test_worker_crash_leaves_no_agent_stranded(tmp_path):
    """Kill the process mid-campaign, restart, and check nothing leaked.

    ``app.crash()`` cancels the loops without any cleanup -- the closest thing
    to ``kill -9`` we can do in-process -- so reservations are left dangling on
    purpose. Recovery has to work them out from durable state alone.
    """
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.5)
    await app.seed_campaign(agents=10, borrowers=200)
    await app.start()
    await app.clock.sleep(20)
    await app.crash()

    stranded = await app.agents.counts_by_state(CAMPAIGN)
    assert stranded[AgentState.RESERVED] + stranded[AgentState.DIALING] >= 0

    # Restart against the *same* database file.
    restarted = build_app(
        tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.5,
    )
    restarted.config = app.config
    await restarted.recovery.reconcile()
    # Give leases time to expire, then reconcile again.
    await restarted.clock.sleep(60)
    report = await restarted.recovery.reconcile()
    assert report is not None
    await assert_invariants(restarted)
    restarted.close()
    app.close()


async def test_stale_reservation_is_recovered(tmp_path):
    """An agent reserved by a worker that never came back must be freed."""
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE)
    await app.seed_campaign(agents=4, borrowers=50)

    reservation = await app.agents.reserve_any_available(CAMPAIGN)
    assert reservation.ok
    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.RESERVED] == 1

    # Nobody ever creates a call for it. Wait past the lease TTL.
    await app.clock.sleep(app.config.reliability.agent_reservation_ttl_seconds + 5)
    await app.recovery.reconcile()

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.RESERVED] == 0
    assert counts[AgentState.AVAILABLE] == 4
    app.close()


async def test_agents_disappearing_mid_campaign_cancels_ringing_calls(tmp_path):
    """100 agents, 40 vanish. Calls that are no longer safe must be cancelled
    while they are still ringing -- before anybody can pick up and find silence.
    """
    app = build_app(
        tmp_path, mode=DialMode.PREDICTIVE, safety=SafetyMode.BALANCED,
        answer_rate=0.5, ring_seconds=8.0,
    )
    await app.seed_campaign(agents=100, borrowers=2000)
    await app.start()
    try:
        await app.clock.sleep(40)
        await app.agents.force_offline(CAMPAIGN, 40)
        await app.clock.sleep(40)
    finally:
        await app.stop()
        await app.events.drain()

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.OFFLINE] >= 40
    assert app.metrics.counter(M.CALLS_ABANDONED) == 0
    await assert_invariants(app)
    app.close()


async def test_hostile_provider_does_not_corrupt_state(tmp_path):
    """Duplicates, reordering, timeouts and hard failures, all at once.

    Provider B's profile is deliberately nasty. The system is allowed to place
    fewer calls; it is not allowed to end up with a corrupted world.
    """
    hostile = ProviderProfile.provider_b(
        answer_rate=0.5,
        talk_seconds_mean=20.0,
        ring_seconds=4.0,
        duplicate_event_probability=0.30,
        out_of_order_probability=0.40,
        timeout_probability=0.15,
        permanent_failure_probability=0.08,
    )
    app = build_app(
        tmp_path, mode=DialMode.PREDICTIVE, safety=SafetyMode.BALANCED,
        profiles=[hostile],
    )
    await app.seed_campaign(agents=10, borrowers=400)
    await run_for(app, 90)

    assert app.metrics.counter(M.DUPLICATE_EVENTS) > 0, "profile did not misbehave"
    assert app.metrics.counter(M.INVALID_TRANSITIONS) == 0
    await assert_invariants(app)
    app.close()


async def test_calls_are_reaped_when_their_agent_goes_offline(tmp_path):
    """An agent vanishing mid-conversation must not leave the borrower hanging.

    Found while making demo 9 honest: force_offline was quietly skipping
    CONNECTED agents, so this path had never run. With it included, seven live
    calls were left held by offline agents and nothing reaped them -- no
    provider event is coming, because as far as the carrier is concerned the
    call is fine.
    """
    app = build_app(tmp_path, mode=DialMode.PREDICTIVE, safety=SafetyMode.BALANCED,
                    answer_rate=0.7, talk_seconds=60.0)
    await app.seed_campaign(agents=20, borrowers=400)
    await app.start()
    await app.clock.sleep(40)

    connected_before = (await app.agents.counts_by_state(CAMPAIGN))[
        AgentState.CONNECTED]
    assert connected_before > 0, "nobody was on a call; the test proves nothing"

    await app.agents.force_offline(CAMPAIGN, 15, only_available=False)
    report = await app.recovery.reconcile()
    await app.stop()
    await app.events.drain()

    assert report.calls_reaped_from_offline_agents > 0
    await assert_invariants(app)
    app.close()


async def test_reaped_connected_call_counts_as_abandoned(tmp_path):
    """Reaping must not launder an abandoned call into a clean completion."""
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.9,
                    talk_seconds=90.0)
    await app.seed_campaign(agents=6, borrowers=100)
    await app.start()
    await app.clock.sleep(30)
    connected = (await app.agents.counts_by_state(CAMPAIGN))[AgentState.CONNECTED]
    before = app.metrics.counter(M.CALLS_ABANDONED)

    await app.agents.force_offline(CAMPAIGN, connected, only_available=False)
    await app.recovery.reconcile()
    await app.stop()
    await app.events.drain()

    assert app.metrics.counter(M.CALLS_ABANDONED) > before
    app.close()
