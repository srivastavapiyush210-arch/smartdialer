"""The predictive bridge race.

Found by ``test_agents_disappearing_mid_campaign_cancels_ringing_calls``, which
failed the "a CONNECTED agent has an active call" invariant. Reproduced here
deterministically rather than by hammering the system and hoping.

The window: bridging an answered call is two writes -- ``mark_connected`` sets
``calls.agent_id``, then the agent moves RESERVED -> CONNECTED. If the carrier's
COMPLETED event lands *between* those two writes, the event processor finalises
the call using the copy of the row it read inside the event transaction, where
``agent_id`` is still NULL. Nobody releases the agent, and it sits CONNECTED
forever against a call that ended.

The interleaving is forced by wrapping ``mark_connected``: as soon as it
commits, we deliver the terminal event, then let the bridge carry on.
"""

from __future__ import annotations

from smartdialer.config import DialMode
from smartdialer.models.domain import ProviderEvent
from smartdialer.models.enums import AgentState, CallState, ProviderEventType

from tests.conftest import CAMPAIGN
from tests.factories import build_app


def _event(call_id, kind, sequence):
    return ProviderEvent(
        event_id=f"{call_id}-{kind.value}-{sequence}",
        call_id=call_id,
        type=kind,
        sequence=sequence,
        timestamp=1_000.0 + sequence,
        provider="provider-a",
        payload={},
    )


async def _answered_unbound_call(app, call_id: str = "C-RACE"):
    await app.seed_campaign(agents=1, borrowers=5)
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create(call_id, CAMPAIGN, borrower.id, DialMode.PREDICTIVE)
    await app.calls.mark_initiated(call_id, "provider-a", "P-1")
    await app.events.handle(_event(call_id, ProviderEventType.RINGING, 1))
    return call_id


async def test_call_completing_mid_bridge_does_not_strand_the_agent(tmp_path):
    app = build_app(tmp_path, mode=DialMode.PREDICTIVE)
    call_id = await _answered_unbound_call(app)

    original = app.calls.mark_connected
    fired = {"done": False}

    async def mark_connected_then_complete(cid, agent_id):
        connected = await original(cid, agent_id)
        if connected and not fired["done"]:
            fired["done"] = True
            # The carrier hangs up in the two-write window.
            await app.events.handle(
                _event(cid, ProviderEventType.COMPLETED, 3)
            )
        return connected

    app.calls.mark_connected = mark_connected_then_complete
    try:
        await app.events.handle(_event(call_id, ProviderEventType.ANSWERED, 2))
        await app.events.drain()
    finally:
        app.calls.mark_connected = original

    assert fired["done"], "the race was never triggered; the test proves nothing"

    call = await app.calls.get(call_id)
    assert call.state.is_terminal()

    agents = await app.agents.list_by_state(CAMPAIGN, AgentState.CONNECTED)
    assert agents == [], "agent left CONNECTED against a call that has ended"

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.AVAILABLE] + counts[AgentState.WRAP_UP] == 1
    app.close()


async def test_call_completing_before_the_bridge_wins_an_agent(tmp_path):
    """The other ordering: the call ends before ``mark_connected`` runs.

    Already handled before this bug existed, but asserted so that fixing one
    ordering cannot silently break the other.
    """
    app = build_app(tmp_path, mode=DialMode.PREDICTIVE)
    call_id = await _answered_unbound_call(app)

    original = app.agents.reserve_any_available

    async def reserve_then_complete(campaign_id, reservation_id=None):
        reservation = await original(campaign_id, reservation_id)
        if reservation.ok:
            await app.events.handle(_event(call_id, ProviderEventType.COMPLETED, 3))
        return reservation

    app.agents.reserve_any_available = reserve_then_complete
    try:
        await app.events.handle(_event(call_id, ProviderEventType.ANSWERED, 2))
        await app.events.drain()
    finally:
        app.agents.reserve_any_available = original

    call = await app.calls.get(call_id)
    assert call.state is CallState.COMPLETED
    assert call.agent_id is None

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.AVAILABLE] == 1
    assert counts[AgentState.CONNECTED] == 0
    app.close()


async def test_reconciler_releases_an_agent_stuck_against_a_dead_call(tmp_path):
    """Defence in depth.

    Even with the ordering bug fixed, an agent can be left busy against a call
    that no longer exists -- a crash between the two writes would do it. The
    reconciler is the backstop, so it gets its own test with the corrupt state
    written directly rather than raced into existence.
    """
    app = build_app(tmp_path, mode=DialMode.PREDICTIVE)
    await app.seed_campaign(agents=2, borrowers=5)
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create("C-DEAD", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)

    agent = (await app.agents.list_by_state(CAMPAIGN, AgentState.AVAILABLE))[0]
    await app.db.execute(
        "UPDATE agents SET state=?, current_call_id=?, state_changed_at=? WHERE id=?",
        (AgentState.CONNECTED.value, "C-DEAD", app.clock.now(), agent.id),
    )
    await app.calls.force_terminal("C-DEAD", CallState.COMPLETED, "carrier hung up")

    report = await app.recovery.reconcile()
    assert report.stranded_agents_released == 1

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.CONNECTED] == 0
    assert counts[AgentState.AVAILABLE] == 2
    app.close()


async def test_reconciler_leaves_a_genuinely_busy_agent_alone(tmp_path):
    """The sweep must not free agents who are actually on a call."""
    app = build_app(tmp_path, mode=DialMode.PREDICTIVE)
    await app.seed_campaign(agents=2, borrowers=5)
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create("C-LIVE", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)
    await app.calls.mark_initiated("C-LIVE", "provider-a", "P-1")
    await app.events.handle(_event("C-LIVE", ProviderEventType.RINGING, 1))
    await app.events.handle(_event("C-LIVE", ProviderEventType.ANSWERED, 2))
    await app.events.drain()

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.CONNECTED] == 1

    report = await app.recovery.reconcile()
    assert report.stranded_agents_released == 0

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.CONNECTED] == 1
    app.close()
