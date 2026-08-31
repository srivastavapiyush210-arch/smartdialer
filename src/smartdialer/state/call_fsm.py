"""Call lifecycle state machine and the event-acceptance policy.

The provider is untrusted: it may repeat events, reorder them, or deliver them
after the worker that started the call has died. The policy below is applied to
*every* provider event, in this order:

1. duplicate  -- the ``event_id`` has already been processed (handled by the
                 event repository's primary key, not here);
2. stale      -- ``sequence`` is not newer than the newest event already
                 applied to this call. Out-of-order delivery lands here;
3. terminal   -- the call already ended; nothing may resurrect it;
4. no-op      -- the event's target state is the state we are already in
                 (e.g. ANSWERED, ANSWERED, ANSWERED);
5. legality   -- the target must be reachable in the transition graph.

Why sequence numbers rather than "state ordering": call states are not totally
ordered (COMPLETED and FAILED are siblings), so comparing states is not a safe
way to decide what is newer. The provider's per-call monotonic ``sequence`` is
assigned when the event is *emitted*, so it reflects real causal order even
when the network delivers events in a different order. Where a provider offers
no sequence we fall back to the event timestamp and then to graph legality
alone; both fallbacks are strictly weaker, which is why the graph check is kept
as an independent last line of defence.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..models.domain import ProviderEvent
from ..models.enums import CallState, EventOutcome, ProviderEventType


class IllegalTransition(Exception):
    pass


CALL_TRANSITIONS: dict[CallState, frozenset[CallState]] = {
    CallState.QUEUED: frozenset(
        {CallState.RESERVED, CallState.CANCELLED, CallState.FAILED}
    ),
    # RINGING/ANSWERED are reachable directly from RESERVED because a carrier
    # webhook can legitimately arrive before our own HTTP response does. The
    # provider identifiers are recorded as facts either way; only the state
    # transition is conditional. See CallRepository.mark_initiated.
    CallState.RESERVED: frozenset(
        {CallState.INITIATED, CallState.RINGING, CallState.ANSWERED,
         CallState.COMPLETED, CallState.CANCELLED, CallState.FAILED}
    ),
    # A terminal event may legitimately arrive from any live state: the call can
    # end before we ever observe RINGING.
    CallState.INITIATED: frozenset(
        {CallState.RINGING, CallState.ANSWERED, CallState.COMPLETED,
         CallState.FAILED, CallState.CANCELLED}
    ),
    CallState.RINGING: frozenset(
        {CallState.ANSWERED, CallState.COMPLETED, CallState.FAILED,
         CallState.CANCELLED}
    ),
    CallState.ANSWERED: frozenset(
        {CallState.CONNECTED, CallState.COMPLETED, CallState.FAILED,
         CallState.CANCELLED}
    ),
    CallState.CONNECTED: frozenset({CallState.COMPLETED, CallState.FAILED}),
    CallState.COMPLETED: frozenset(),
    CallState.FAILED: frozenset(),
    CallState.CANCELLED: frozenset(),
}

EVENT_TARGET_STATE: dict[ProviderEventType, CallState] = {
    ProviderEventType.RINGING: CallState.RINGING,
    ProviderEventType.ANSWERED: CallState.ANSWERED,
    ProviderEventType.COMPLETED: CallState.COMPLETED,
    ProviderEventType.NO_ANSWER: CallState.FAILED,
    ProviderEventType.BUSY: CallState.FAILED,
    ProviderEventType.FAILED: CallState.FAILED,
    ProviderEventType.CANCELLED: CallState.CANCELLED,
}


def can_transition(source: CallState, target: CallState) -> bool:
    return target in CALL_TRANSITIONS.get(source, frozenset())


def validate_transition(source: CallState, target: CallState, reason: str) -> None:
    if not can_transition(source, target):
        raise IllegalTransition(
            f"illegal call transition {source.value} -> {target.value} ({reason})"
        )


@dataclass(frozen=True)
class EventDecision:
    """Pure decision about one event. No I/O, fully unit-testable."""

    outcome: EventOutcome
    target_state: CallState | None
    reason: str

    @property
    def applies(self) -> bool:
        return self.outcome is EventOutcome.APPLIED


def decide_event(
    current_state: CallState, last_sequence: int, event: ProviderEvent
) -> EventDecision:
    """Decide what to do with ``event`` given the call's current state."""
    target = EVENT_TARGET_STATE.get(event.type)
    if target is None:
        return EventDecision(EventOutcome.INVALID, None, f"unknown event {event.type}")

    if event.sequence <= last_sequence:
        return EventDecision(
            EventOutcome.STALE,
            None,
            f"sequence {event.sequence} <= applied {last_sequence}",
        )

    if current_state.is_terminal():
        if target is current_state:
            return EventDecision(
                EventOutcome.NO_OP, None, f"already {current_state.value}"
            )
        return EventDecision(
            EventOutcome.TERMINAL_PROTECTED,
            None,
            f"{current_state.value} is terminal; refusing {target.value}",
        )

    if target is current_state:
        return EventDecision(
            EventOutcome.NO_OP, None, f"already {current_state.value}"
        )

    if not can_transition(current_state, target):
        return EventDecision(
            EventOutcome.INVALID,
            None,
            f"{current_state.value} -> {target.value} not permitted",
        )

    return EventDecision(EventOutcome.APPLIED, target, "ok")
