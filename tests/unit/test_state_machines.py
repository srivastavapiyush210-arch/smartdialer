"""State machine unit tests.

These are the cheapest tests in the suite and they guard the most expensive
class of bug: an agent or a call quietly ending up in a state nobody designed
for. The transition tables are data, so the tests read as tables too.
"""

from __future__ import annotations

import pytest

from smartdialer.models.domain import ProviderEvent
from smartdialer.models.enums import (
    AgentState,
    CallState,
    EventOutcome,
    ProviderEventType,
)
from smartdialer.state import agent_fsm, call_fsm


# --------------------------------------------------------------------- agents
@pytest.mark.parametrize(
    "source,target",
    [
        (AgentState.AVAILABLE, AgentState.RESERVED),
        (AgentState.RESERVED, AgentState.DIALING),
        (AgentState.RESERVED, AgentState.AVAILABLE),      # dial failed, give back
        (AgentState.RESERVED, AgentState.CONNECTED),      # predictive bridge
        (AgentState.DIALING, AgentState.CONNECTED),
        (AgentState.DIALING, AgentState.AVAILABLE),       # no answer
        (AgentState.CONNECTED, AgentState.WRAP_UP),
        (AgentState.WRAP_UP, AgentState.AVAILABLE),
    ],
)
def test_legal_agent_transitions(source, target):
    assert agent_fsm.can_transition(source, target)


@pytest.mark.parametrize(
    "source,target",
    [
        (AgentState.AVAILABLE, AgentState.CONNECTED),   # never skip reservation
        (AgentState.AVAILABLE, AgentState.WRAP_UP),
        (AgentState.CONNECTED, AgentState.RESERVED),
        (AgentState.WRAP_UP, AgentState.CONNECTED),
        (AgentState.DIALING, AgentState.RESERVED),      # no going backwards
    ],
)
def test_illegal_agent_transitions_are_rejected(source, target):
    assert not agent_fsm.can_transition(source, target)
    with pytest.raises(agent_fsm.IllegalTransition):
        agent_fsm.validate_transition(source, target, "test")


def test_connected_may_skip_wrap_up():
    """Wrap-up is configurable and campaigns may set it to zero seconds.

    Rather than special-case that, CONNECTED -> AVAILABLE is legal and the
    WrapUpService is what decides whether the intermediate state is used.
    """
    assert agent_fsm.can_transition(AgentState.CONNECTED, AgentState.AVAILABLE)


def test_offline_is_reachable_from_every_state():
    """An agent can always be yanked offline; that is the one universal edge.

    Anything else would mean a crashed or logged-out agent could get stuck in
    a state the system refuses to leave.
    """
    for source in AgentState:
        if source is AgentState.OFFLINE:
            continue
        assert agent_fsm.can_transition(source, AgentState.OFFLINE), source


# ---------------------------------------------------------------------- calls
@pytest.mark.parametrize(
    "source,target",
    [
        (CallState.RESERVED, CallState.INITIATED),
        (CallState.INITIATED, CallState.RINGING),
        (CallState.RINGING, CallState.ANSWERED),
        (CallState.ANSWERED, CallState.CONNECTED),
        (CallState.ANSWERED, CallState.FAILED),      # nobody free in time
        (CallState.CONNECTED, CallState.COMPLETED),
        (CallState.RINGING, CallState.FAILED),       # no answer / busy
    ],
)
def test_legal_call_transitions(source, target):
    assert call_fsm.can_transition(source, target)


def test_carrier_events_may_beat_our_own_bookkeeping():
    """A webhook can arrive before the API response we are still awaiting.

    This is not a hypothetical: it is the bug that produced seven invalid
    transitions in the first progressive smoke test. RESERVED must therefore
    accept the events that normally follow INITIATED.
    """
    for target in (CallState.RINGING, CallState.ANSWERED, CallState.COMPLETED):
        assert call_fsm.can_transition(CallState.RESERVED, target), target


@pytest.mark.parametrize("terminal", [
    CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED,
])
def test_terminal_states_have_no_exits(terminal):
    assert terminal.is_terminal()
    for target in CallState:
        if target is terminal:
            continue
        assert not call_fsm.can_transition(terminal, target), (terminal, target)


# ------------------------------------------------------- event decision policy
def _event(kind: ProviderEventType, sequence: int) -> ProviderEvent:
    return ProviderEvent(
        event_id=f"E{sequence}",
        call_id="C1",
        type=kind,
        sequence=sequence,
        timestamp=1.0,
        provider="provider-a",
        payload={},
    )


def test_stale_event_is_rejected_by_sequence():
    """Out-of-order delivery is detected by sequence, not by guessing.

    Call states are not totally ordered -- COMPLETED and FAILED are siblings --
    so "is this state later than that one?" has no answer. The provider's
    per-call sequence number does.
    """
    decision = call_fsm.decide_event(
        CallState.ANSWERED, last_sequence=5, event=_event(ProviderEventType.RINGING, 3)
    )
    assert decision.outcome is EventOutcome.STALE
    assert not decision.applies


def test_terminal_state_does_not_regress():
    decision = call_fsm.decide_event(
        CallState.COMPLETED, last_sequence=1, event=_event(ProviderEventType.RINGING, 9)
    )
    assert decision.outcome is EventOutcome.TERMINAL_PROTECTED
    assert decision.target_state is None


def test_repeat_of_current_state_is_a_no_op_not_an_error():
    decision = call_fsm.decide_event(
        CallState.RINGING, last_sequence=1, event=_event(ProviderEventType.RINGING, 4)
    )
    assert decision.outcome is EventOutcome.NO_OP


def test_illegal_jump_is_reported_as_invalid():
    decision = call_fsm.decide_event(
        CallState.COMPLETED, last_sequence=0, event=_event(ProviderEventType.ANSWERED, 2)
    )
    assert not decision.applies


def test_normal_progression_applies():
    decision = call_fsm.decide_event(
        CallState.RINGING, last_sequence=2, event=_event(ProviderEventType.ANSWERED, 3)
    )
    assert decision.applies
    assert decision.target_state is CallState.ANSWERED
