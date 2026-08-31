"""Concurrency tests.

These use real threads against a real SQLite file. Nothing here is simulated
with a mutex in Python: if the compare-and-swap in the repository is wrong,
these tests fail. That is the point -- a concurrency test that fakes the
concurrency proves nothing.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from smartdialer.config import DialMode
from smartdialer.models.domain import ProviderEvent
from smartdialer.models.enums import AgentState, BorrowerState, ProviderEventType

from tests.conftest import CAMPAIGN


# ------------------------------------------------------------------- agents
async def test_two_workers_racing_for_one_agent_produce_exactly_one_winner(repos):
    """The canonical race. Two workers, one agent, one winner."""
    agents, _, _ = repos
    await agents.bulk_create(CAMPAIGN, 1)
    agent = (await agents.list_by_state(CAMPAIGN, AgentState.AVAILABLE))[0]

    results = await asyncio.gather(
        agents.reserve_agent(agent.id, str(uuid.uuid4())),
        agents.reserve_agent(agent.id, str(uuid.uuid4())),
    )
    winners = [r for r in results if r.ok]
    losers = [r for r in results if not r.ok]
    assert len(winners) == 1
    assert len(losers) == 1
    # The loser is told *why* it lost, not merely that it did: the agent is now
    # RESERVED. That distinction is what makes a production log line useful.
    assert losers[0].reason == "agent_is_RESERVED"
    assert losers[0].reservation_id is None


async def test_fifty_workers_racing_for_ten_agents_reserve_exactly_ten(repos):
    """Scaled-up version: the invariant must not depend on the number of racers."""
    agents, _, _ = repos
    await agents.bulk_create(CAMPAIGN, 10)

    results = await asyncio.gather(
        *(agents.reserve_any_available(CAMPAIGN) for _ in range(50))
    )
    winners = [r for r in results if r.ok]
    assert len(winners) == 10

    # Every winner got a *different* agent. Two workers holding the same agent
    # is the failure this whole mechanism exists to prevent.
    assert len({r.entity_id for r in winners}) == 10

    counts = await agents.counts_by_state(CAMPAIGN)
    assert counts[AgentState.RESERVED] == 10
    assert counts[AgentState.AVAILABLE] == 0


async def test_reservation_ids_are_not_reused_across_winners(repos):
    agents, _, _ = repos
    await agents.bulk_create(CAMPAIGN, 5)
    results = await asyncio.gather(
        *(agents.reserve_any_available(CAMPAIGN) for _ in range(20))
    )
    ids = [r.reservation_id for r in results if r.ok]
    assert len(ids) == len(set(ids)) == 5


# ---------------------------------------------------------------- borrowers
async def test_concurrent_workers_never_hand_out_the_same_borrower(repos):
    """A borrower called twice at once is a compliance problem, not a glitch."""
    _, borrowers, _ = repos
    await borrowers.bulk_create(CAMPAIGN, 20, max_attempts=3)

    results = await asyncio.gather(
        *(borrowers.reserve_next(CAMPAIGN) for _ in range(60))
    )
    reserved = [b for reservation, b in results if reservation.ok and b]
    assert len(reserved) == 20
    assert len({b.id for b in reserved}) == 20

    counts = await borrowers.counts_by_state(CAMPAIGN)
    assert counts[BorrowerState.PENDING] == 0


async def test_exhausted_queue_reports_cleanly_rather_than_erroring(repos):
    _, borrowers, _ = repos
    await borrowers.bulk_create(CAMPAIGN, 2, max_attempts=3)
    results = await asyncio.gather(
        *(borrowers.reserve_next(CAMPAIGN) for _ in range(10))
    )
    ok = [r for r, _ in results if r.ok]
    assert len(ok) == 2
    assert all(not r.ok for r, b in results if b is None)


# -------------------------------------------------------------------- calls
async def test_database_refuses_two_active_calls_for_one_borrower(repos):
    """The invariant is enforced by a partial unique index, not by application
    code being careful. Application code can be bypassed; the index cannot."""
    from smartdialer.repositories.calls import DuplicateActiveCall

    agents, borrowers, calls = repos
    await borrowers.bulk_create(CAMPAIGN, 1, max_attempts=3)
    reservation, borrower = await borrowers.reserve_next(CAMPAIGN)
    assert borrower is not None

    await calls.create("C1", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)
    with pytest.raises(DuplicateActiveCall):
        await calls.create("C2", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)


async def test_concurrent_call_creation_for_one_borrower_yields_one_call(repos):
    from smartdialer.repositories.calls import DuplicateActiveCall

    _, borrowers, calls = repos
    await borrowers.bulk_create(CAMPAIGN, 1, max_attempts=3)
    _, borrower = await borrowers.reserve_next(CAMPAIGN)

    async def attempt(n: int):
        try:
            await calls.create(f"C{n}", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)
            return True
        except DuplicateActiveCall:
            return False

    results = await asyncio.gather(*(attempt(n) for n in range(8)))
    assert sum(results) == 1


async def test_database_refuses_two_active_calls_for_one_agent(repos):
    from smartdialer.repositories.calls import DuplicateActiveCall

    agents, borrowers, calls = repos
    await agents.bulk_create(CAMPAIGN, 1)
    await borrowers.bulk_create(CAMPAIGN, 2, max_attempts=3)
    agent = (await agents.list_by_state(CAMPAIGN, AgentState.AVAILABLE))[0]

    _, b1 = await borrowers.reserve_next(CAMPAIGN)
    _, b2 = await borrowers.reserve_next(CAMPAIGN)
    await calls.create("C1", CAMPAIGN, b1.id, DialMode.PROGRESSIVE, agent_id=agent.id)
    with pytest.raises(DuplicateActiveCall):
        await calls.create(
            "C2", CAMPAIGN, b2.id, DialMode.PROGRESSIVE, agent_id=agent.id
        )


# ------------------------------------------------------------------- events
def _event(call_id: str, kind: ProviderEventType, sequence: int,
           event_id: str | None = None) -> ProviderEvent:
    return ProviderEvent(
        event_id=event_id or f"{call_id}-{kind.value}-{sequence}",
        call_id=call_id,
        type=kind,
        sequence=sequence,
        timestamp=1_000.0 + sequence,
        provider="provider-a",
        payload={},
    )


async def test_duplicate_event_has_no_duplicate_side_effect(repos):
    """The same event delivered twice must change the world exactly once.

    Carriers retry webhooks. Our own retries can also double-deliver. The
    ledger write happens in the *same* transaction as the state change, so
    "applied" and "recorded as applied" cannot come apart.
    """
    _, borrowers, calls = repos
    await borrowers.bulk_create(CAMPAIGN, 1, max_attempts=3)
    _, borrower = await borrowers.reserve_next(CAMPAIGN)
    await calls.create("C1", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)

    event = _event("C1", ProviderEventType.RINGING, 1)
    first = await calls.apply_provider_event(event)
    second = await calls.apply_provider_event(event)

    assert first.outcome.name == "APPLIED"
    assert second.outcome.name == "DUPLICATE"


async def test_concurrent_delivery_of_the_same_event_applies_once(repos):
    """Two webhook workers, one event, delivered simultaneously."""
    _, borrowers, calls = repos
    await borrowers.bulk_create(CAMPAIGN, 1, max_attempts=3)
    _, borrower = await borrowers.reserve_next(CAMPAIGN)
    await calls.create("C1", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)

    event = _event("C1", ProviderEventType.RINGING, 1)
    results = await asyncio.gather(
        *(calls.apply_provider_event(event) for _ in range(10))
    )
    applied = [r for r in results if r.outcome.name == "APPLIED"]
    assert len(applied) == 1


async def test_out_of_order_events_do_not_regress_the_call(repos):
    """Full reversal: COMPLETED arrives, then ANSWERED, then RINGING."""
    _, borrowers, calls = repos
    await borrowers.bulk_create(CAMPAIGN, 1, max_attempts=3)
    _, borrower = await borrowers.reserve_next(CAMPAIGN)
    await calls.create("C1", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)

    await calls.apply_provider_event(_event("C1", ProviderEventType.COMPLETED, 3))
    await calls.apply_provider_event(_event("C1", ProviderEventType.ANSWERED, 2))
    await calls.apply_provider_event(_event("C1", ProviderEventType.RINGING, 1))

    call = await calls.get("C1")
    assert call.state.is_terminal()
    assert call.state.name == "COMPLETED"


async def test_late_answered_event_records_the_fact_without_changing_state(repos):
    """Monotonic fact merge.

    A late ANSWERED on a finished call is still *true* -- the borrower really
    did pick up -- so we record ``answered_at`` for reporting. What we refuse
    to do is resurrect the call or allocate an agent to it.
    """
    _, borrowers, calls = repos
    await borrowers.bulk_create(CAMPAIGN, 1, max_attempts=3)
    _, borrower = await borrowers.reserve_next(CAMPAIGN)
    await calls.create("C1", CAMPAIGN, borrower.id, DialMode.PREDICTIVE)

    await calls.apply_provider_event(_event("C1", ProviderEventType.COMPLETED, 5))
    application = await calls.apply_provider_event(
        _event("C1", ProviderEventType.ANSWERED, 6)
    )

    call = await calls.get("C1")
    assert call.state.name == "COMPLETED"
    assert not application.needs_agent   # crucially, no agent is allocated


async def test_events_for_an_unknown_call_are_rejected_not_crashed(repos):
    _, _, calls = repos
    application = await calls.apply_provider_event(
        _event("C-does-not-exist", ProviderEventType.RINGING, 1)
    )
    assert application.outcome.name == "UNKNOWN_CALL"


async def test_interleaved_events_for_many_calls_stay_consistent(repos):
    """Twenty calls, events delivered concurrently and shuffled per call."""
    _, borrowers, calls = repos
    await borrowers.bulk_create(CAMPAIGN, 20, max_attempts=3)

    call_ids = []
    for n in range(20):
        _, borrower = await borrowers.reserve_next(CAMPAIGN)
        call_id = f"C{n}"
        await calls.create(call_id, CAMPAIGN, borrower.id, DialMode.PREDICTIVE)
        call_ids.append(call_id)

    events = []
    for call_id in call_ids:
        events += [
            _event(call_id, ProviderEventType.RINGING, 1),
            _event(call_id, ProviderEventType.ANSWERED, 2),
            _event(call_id, ProviderEventType.COMPLETED, 3),
        ]
    # Deliver everything at once, in an order nobody designed for.
    events.sort(key=lambda e: e.event_id)
    await asyncio.gather(*(calls.apply_provider_event(e) for e in events))

    for call_id in call_ids:
        call = await calls.get(call_id)
        assert call.state.is_terminal(), (call_id, call.state)
