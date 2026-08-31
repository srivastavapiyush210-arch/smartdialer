"""State and event enumerations.

Strings are used as values so they persist to SQLite and read well in logs.
"""

from __future__ import annotations

from enum import Enum


class AgentState(str, Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"     # held for a specific call, not yet dialling
    DIALING = "DIALING"       # agent-bound outbound call in setup/ringing
    CONNECTED = "CONNECTED"   # talking to a borrower
    WRAP_UP = "WRAP_UP"       # post-call disposition
    PAUSED = "PAUSED"         # logged in but not dialable

    def is_occupied(self) -> bool:
        return self in _OCCUPIED_AGENT_STATES


_OCCUPIED_AGENT_STATES = frozenset(
    {AgentState.RESERVED, AgentState.DIALING, AgentState.CONNECTED}
)


class CallState(str, Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"      # borrower (and maybe agent) held, not yet dialled
    INITIATED = "INITIATED"    # provider accepted the request
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"      # borrower picked up; NOT yet bridged to an agent
    CONNECTED = "CONNECTED"    # bridged to an agent
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    def is_terminal(self) -> bool:
        return self in TERMINAL_CALL_STATES

    def is_in_flight(self) -> bool:
        """States where the borrower's phone may still start ringing/answering."""
        return self in IN_FLIGHT_CALL_STATES


TERMINAL_CALL_STATES = frozenset(
    {CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED}
)
IN_FLIGHT_CALL_STATES = frozenset(
    {CallState.RESERVED, CallState.INITIATED, CallState.RINGING}
)


class BorrowerState(str, Enum):
    PENDING = "PENDING"        # eligible (subject to cooldown/attempts)
    RESERVED = "RESERVED"      # held by a worker for an in-flight call
    CONTACTED = "CONTACTED"    # answered at least once, campaign done for now
    EXHAUSTED = "EXHAUSTED"    # attempt budget spent
    SUPPRESSED = "SUPPRESSED"  # permanently ineligible (bad number, DNC)


class ProviderEventType(str, Enum):
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    COMPLETED = "COMPLETED"
    NO_ANSWER = "NO_ANSWER"
    BUSY = "BUSY"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class SafetyAction(str, Enum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"
    FALLBACK_TO_PROGRESSIVE = "FALLBACK_TO_PROGRESSIVE"


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class FailureClass(str, Enum):
    """Drives the retry policy. Retrying a permanent failure is a bug."""

    TRANSIENT = "TRANSIENT"    # timeout, provider overloaded -> retry
    PERMANENT = "PERMANENT"    # invalid number, rejected -> never retry
    UNKNOWN = "UNKNOWN"        # treated as transient but with a lower budget


class EventOutcome(str, Enum):
    """What the event processor did with a provider event."""

    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"                  # same event_id seen before
    NO_OP = "NO_OP"                          # already in that state
    STALE = "STALE"                          # arrived out of order
    TERMINAL_PROTECTED = "TERMINAL_PROTECTED"  # would regress a terminal call
    INVALID = "INVALID"                      # illegal transition
    UNKNOWN_CALL = "UNKNOWN_CALL"
