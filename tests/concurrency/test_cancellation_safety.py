"""Cancellation safety around accepted events.

The shield exists because cancelling the carrier's delivery task *between* the
committed state change and the follow-up work stranded agents. That fix creates
its own question: does cancellation now produce some other half-finished state?
These tests answer it at each of the points where cancellation can land.

The property under test throughout: once ``apply_provider_event`` commits, the
matching side effects run to completion, or the reconciler can still repair the
result. Never a silent leak.
"""

from __future__ import annotations

import asyncio

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


async def _bound_connected_call(app, call_id="C-CANCEL"):
    """A progressive call with an agent attached, sitting in CONNECTED."""
    await app.seed_campaign(agents=2, borrowers=5)
    agent = (await app.agents.list_by_state(CAMPAIGN, AgentState.AVAILABLE))[0]
    reservation = await app.agents.reserve_agent(agent.id, "R-1")
    assert reservation.ok
    _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
    await app.calls.create(
        call_id, CAMPAIGN, borrower.id, DialMode.PROGRESSIVE, agent_id=agent.id
    )
    await app.agents.transition(
        agent.id, AgentState.DIALING, "test", expected=AgentState.RESERVED,
        call_id=call_id,
    )
    await app.calls.mark_initiated(call_id, "provider-a", "P-1")
    await app.events.handle(_event(call_id, ProviderEventType.RINGING, 1))
    await app.events.handle(_event(call_id, ProviderEventType.ANSWERED, 2))
    await app.events.drain()
    return agent.id, call_id


async def test_cancellation_immediately_after_acceptance_still_finalises(tmp_path):
    """Cancel the delivering task the instant the event is accepted.

    This is the exact shape of the bug: the state change is durable, the agent
    release is not. The shield has to carry the side effects through.
    """
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE)
    agent_id, call_id = await _bound_connected_call(app)
    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.CONNECTED] == 1

    async def deliver():
        await app.events.handle(_event(call_id, ProviderEventType.COMPLETED, 3))

    task = asyncio.create_task(deliver())
    await asyncio.sleep(0)      # let it reach the first await
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await app.events.drain()

    call = await app.calls.get(call_id)
    if call.state.is_terminal():
        counts = await app.agents.counts_by_state(CAMPAIGN)
        assert counts[AgentState.CONNECTED] == 0, "agent stranded on a dead call"
    app.close()


async def test_cancellation_during_the_side_effect_completes_it(tmp_path):
    """Cancel while the side effect is mid-flight, not before it starts."""
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE)
    agent_id, call_id = await _bound_connected_call(app)

    reached = asyncio.Event()
    original = app.agents.transition

    async def slow_transition(*args, **kwargs):
        reached.set()
        await asyncio.sleep(0.02)   # widen the window on purpose
        return await original(*args, **kwargs)

    app.agents.transition = slow_transition
    try:
        task = asyncio.create_task(
            app.events.handle(_event(call_id, ProviderEventType.COMPLETED, 3))
        )
        await reached.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await app.events.drain()
    finally:
        app.agents.transition = original

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.CONNECTED] == 0
    assert counts[AgentState.WRAP_UP] + counts[AgentState.AVAILABLE] == 2
    app.close()


async def test_shutdown_while_events_are_in_flight_leaves_no_stranded_agent(tmp_path):
    """A live campaign stopped mid-conversation.

    This is the scenario that produced the third defect: ``stop()`` cancels the
    carrier's delivery tasks, and an event accepted a millisecond earlier must
    still finish its work.
    """
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.8,
                    talk_seconds=6.0)
    await app.seed_campaign(agents=20, borrowers=400)
    await app.start()
    await app.clock.sleep(40)
    await app.stop()

    stranded = []
    for agent in await app.agents.list_by_state(CAMPAIGN, AgentState.CONNECTED):
        if await app.calls.active_call_for_agent(agent.id) is None:
            stranded.append(agent.id)
    assert stranded == [], f"stranded after shutdown: {stranded}"
    app.close()


async def test_drain_waits_for_many_accepted_events(tmp_path):
    """Accept a burst of terminal events at once, then drain.

    If ``drain`` returned before the shielded tasks finished, some of these
    agents would still be CONNECTED.
    """
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE)
    await app.seed_campaign(agents=30, borrowers=100)

    call_ids = []
    for n in range(30):
        agent = (await app.agents.list_by_state(CAMPAIGN, AgentState.AVAILABLE))[0]
        await app.agents.reserve_agent(agent.id, f"R{n}")
        _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
        call_id = f"C{n}"
        await app.calls.create(
            call_id, CAMPAIGN, borrower.id, DialMode.PROGRESSIVE, agent_id=agent.id
        )
        await app.agents.transition(
            agent.id, AgentState.DIALING, "test", expected=AgentState.RESERVED,
            call_id=call_id,
        )
        await app.calls.mark_initiated(call_id, "provider-a", f"P{n}")
        await app.events.handle(_event(call_id, ProviderEventType.RINGING, 1))
        await app.events.handle(_event(call_id, ProviderEventType.ANSWERED, 2))
        call_ids.append(call_id)
    await app.events.drain()

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.CONNECTED] == 30

    await asyncio.gather(*(
        app.events.handle(_event(cid, ProviderEventType.COMPLETED, 3))
        for cid in call_ids
    ))
    await app.events.drain()

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.CONNECTED] == 0
    assert counts[AgentState.WRAP_UP] == 30
    app.close()


async def test_event_arriving_as_shutdown_begins(tmp_path):
    """Deliver an event concurrently with ``stop()``.

    Either the event is accepted and fully applied, or it never lands. What is
    not allowed is accepted-and-half-applied.
    """
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE)
    agent_id, call_id = await _bound_connected_call(app)
    await app.start()

    stopping = asyncio.create_task(app.stop())
    delivering = asyncio.create_task(
        app.events.handle(_event(call_id, ProviderEventType.COMPLETED, 3))
    )
    await asyncio.gather(stopping, delivering, return_exceptions=True)
    await app.events.drain()

    call = await app.calls.get(call_id)
    counts = await app.agents.counts_by_state(CAMPAIGN)
    if call.state.is_terminal():
        assert counts[AgentState.CONNECTED] == 0
    else:
        # Event never landed: the agent is legitimately still on a live call.
        assert counts[AgentState.CONNECTED] == 1
    app.close()


async def test_hard_crash_still_needs_the_reconciler(tmp_path):
    """The shield is not a substitute for reconciliation.

    ``crash()`` drops everything without draining -- the shielded tasks die with
    the process. State can therefore be inconsistent, and the reconciler is what
    repairs it. Asserted so nobody later concludes the reconciler is redundant.
    """
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.8,
                    talk_seconds=6.0)
    await app.seed_campaign(agents=20, borrowers=400)
    await app.start()
    await app.clock.sleep(30)
    await app.crash()

    await app.recovery.reconcile()
    for agent in await app.agents.list_by_state(CAMPAIGN, AgentState.CONNECTED):
        assert await app.calls.active_call_for_agent(agent.id) is not None
    app.close()


async def test_cancelling_the_orchestrator_mid_dial_leaves_no_reservation(tmp_path):
    """The dial path, cancelled at its most awkward moment.

    A dial is reserve-then-release: win an agent, look for a borrower, give the
    agent back if there is none. With no borrowers at all, every dial takes the
    release branch, so cancelling the tick repeatedly lands in that window.
    Before the dial path was shielded this left agents stuck RESERVED and the
    zero-borrower adversarial test failed about one run in three.
    """
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE)
    await app.seed_campaign(agents=12, borrowers=0)

    for _ in range(15):
        task = asyncio.create_task(app.orchestrator.tick())
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    await app.allocator.drain()

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.AVAILABLE] == 12, counts
    app.close()


async def test_dials_in_flight_complete_when_the_campaign_stops(tmp_path):
    """Shutdown must not abandon a dial half-way through."""
    app = build_app(tmp_path, mode=DialMode.PROGRESSIVE, answer_rate=0.5)
    await app.seed_campaign(agents=10, borrowers=200)
    await app.start()
    await app.clock.sleep(15)
    await app.stop()

    counts = await app.agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.RESERVED] == 0, f"reservation left by shutdown: {counts}"

    for call in await app.calls.find_stuck_in_setup(0.0):
        assert call.provider_call_id is not None, (
            f"call {call.id} was created but never reached the carrier"
        )
    app.close()
