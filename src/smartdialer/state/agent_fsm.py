"""Agent lifecycle state machine.

Transitions are data, not scattered ``if`` statements, so the legal graph can
be asserted in tests and rendered into documentation.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..models.enums import AgentState


class IllegalTransition(Exception):
    """Raised when code attempts a transition that the FSM forbids."""


# reason -> why this edge exists. Documented in docs/state-machines.md.
AGENT_TRANSITIONS: dict[AgentState, frozenset[AgentState]] = {
    AgentState.OFFLINE: frozenset({AgentState.AVAILABLE}),
    AgentState.AVAILABLE: frozenset(
        {AgentState.RESERVED, AgentState.PAUSED, AgentState.OFFLINE}
    ),
    # RESERVED -> CONNECTED is the predictive bridge: the borrower has already
    # answered, so the agent skips DIALING and is joined straight onto the call.
    AgentState.RESERVED: frozenset(
        {AgentState.DIALING, AgentState.CONNECTED, AgentState.AVAILABLE,
         AgentState.OFFLINE}
    ),
    AgentState.DIALING: frozenset(
        {AgentState.CONNECTED, AgentState.AVAILABLE, AgentState.WRAP_UP,
         AgentState.OFFLINE}
    ),
    AgentState.CONNECTED: frozenset(
        {AgentState.WRAP_UP, AgentState.AVAILABLE, AgentState.OFFLINE}
    ),
    AgentState.WRAP_UP: frozenset(
        {AgentState.AVAILABLE, AgentState.PAUSED, AgentState.OFFLINE}
    ),
    AgentState.PAUSED: frozenset({AgentState.AVAILABLE, AgentState.OFFLINE}),
}

# Going OFFLINE is always allowed: an agent can close their browser at any
# moment. It is the one "exceptional" edge, and it always requires cleanup of
# whatever call the agent was holding (see AgentRegistry.force_offline).
EXCEPTIONAL_TARGETS = frozenset({AgentState.OFFLINE})


@dataclass(frozen=True)
class AgentTransition:
    source: AgentState
    target: AgentState
    reason: str


def can_transition(source: AgentState, target: AgentState) -> bool:
    if source == target:
        return False
    if target in EXCEPTIONAL_TARGETS:
        return True
    return target in AGENT_TRANSITIONS.get(source, frozenset())


def validate_transition(
    source: AgentState, target: AgentState, reason: str
) -> AgentTransition:
    """Return the transition or raise. Every caller must supply a reason."""
    if not reason:
        raise ValueError("agent transitions require a reason")
    if not can_transition(source, target):
        raise IllegalTransition(
            f"illegal agent transition {source.value} -> {target.value} ({reason})"
        )
    return AgentTransition(source=source, target=target, reason=reason)


def legal_targets(source: AgentState) -> frozenset[AgentState]:
    return AGENT_TRANSITIONS.get(source, frozenset()) | EXCEPTIONAL_TARGETS
