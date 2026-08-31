"""Domain records and decision objects.

These are plain dataclasses. Persistence lives in ``repositories/``; nothing
here touches the database.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from ..config import DialMode
from .enums import (
    AgentState,
    BorrowerState,
    CallState,
    FailureClass,
    ProviderEventType,
    SafetyAction,
)


@dataclass(frozen=True)
class Campaign:
    id: str
    name: str
    max_concurrent_calls: int = 1000
    active: bool = True


@dataclass(frozen=True)
class Agent:
    id: str
    name: str
    campaign_id: str
    state: AgentState
    reservation_id: str | None = None
    current_call_id: str | None = None
    reserved_at: float | None = None
    state_changed_at: float = 0.0
    wrap_up_until: float | None = None


@dataclass(frozen=True)
class Borrower:
    """Synthetic collections account. No real personal data is ever used."""

    id: str
    campaign_id: str
    phone: str
    state: BorrowerState
    priority: int = 0
    attempts: int = 0
    max_attempts: int = 3
    not_before: float = 0.0
    reservation_id: str | None = None
    reserved_at: float | None = None
    last_outcome: str | None = None
    created_at: float = 0.0


@dataclass(frozen=True)
class Call:
    id: str
    campaign_id: str
    borrower_id: str
    mode: DialMode
    state: CallState
    agent_bound: bool
    agent_id: str | None = None
    provider: str | None = None
    provider_call_id: str | None = None
    reservation_id: str | None = None
    last_sequence: int = -1
    attempts: int = 0
    created_at: float = 0.0
    updated_at: float = 0.0
    initiated_at: float | None = None
    ringing_at: float | None = None
    answered_at: float | None = None
    connected_at: float | None = None
    ended_at: float | None = None
    abandoned: bool = False
    failure_reason: str | None = None


@dataclass(frozen=True)
class ProviderEvent:
    """An untrusted message from a telecom provider.

    ``event_id`` is the idempotency key. ``sequence`` is the provider's
    per-call monotonic counter assigned at *emission* time; delivery order may
    differ, which is exactly how we detect out-of-order events.
    """

    event_id: str
    call_id: str
    type: ProviderEventType
    sequence: int
    timestamp: float
    provider: str
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderHealth:
    name: str
    healthy: bool
    success_rate: float
    latency_seconds: float
    circuit_state: str

    @property
    def score(self) -> float:
        """0.0 (dead) .. 1.0 (perfect). Used by pacing and safety."""
        if not self.healthy:
            return 0.0
        return max(0.0, min(1.0, self.success_rate))


@dataclass(frozen=True)
class CallRequest:
    call_id: str
    borrower_id: str
    phone: str
    campaign_id: str
    # Carried for a real adapter to send as an Idempotency-Key header. NOT
    # honoured by anything today: the mock ignores it, so this field buys no
    # protection against the ambiguous-timeout case. Deliberately kept rather
    # than deleted, because it is the seam a Twilio/Plivo adapter would use.
    # See docs/engineering-log.md, "Ambiguous initiate timeouts".
    idempotency_key: str


@dataclass(frozen=True)
class CallHandle:
    provider: str
    provider_call_id: str
    accepted_at: float


class ProviderError(Exception):
    """Base class for provider failures. ``failure_class`` drives retries."""

    failure_class: FailureClass = FailureClass.UNKNOWN

    def __init__(self, message: str, *, provider: str = "unknown") -> None:
        super().__init__(message)
        self.provider = provider


class TransientProviderError(ProviderError):
    failure_class = FailureClass.TRANSIENT


class ProviderTimeoutError(TransientProviderError):
    pass


class PermanentProviderError(ProviderError):
    failure_class = FailureClass.PERMANENT


class CircuitOpenError(TransientProviderError):
    """Raised locally; the request never reached the provider."""


@dataclass(frozen=True)
class SystemSnapshot:
    """Immutable view of the world used by pacing and safety.

    Both components read the *same* snapshot so their decisions are comparable
    and reproducible in logs.
    """

    at: float
    campaign_id: str
    available_agents: int
    reserved_agents: int
    dialing_agents: int
    connected_agents: int
    wrap_up_agents: int
    paused_agents: int
    offline_agents: int
    unbound_calls_in_flight: int   # predictive calls with no agent held
    bound_calls_in_flight: int     # progressive calls holding an agent
    ringing_calls: int
    answered_awaiting_agent: int
    connected_calls: int
    eligible_borrowers: int
    provider_health: float
    circuit_state: str
    answers_observed: int
    abandoned_observed: int
    dials_observed: int
    # Rolling window over the most recent completed dials. Cumulative counters
    # are blind to regime changes; these are what the Safety Controller plans
    # against.
    recent_dials: int = 0
    recent_answers: int = 0
    recent_abandoned: int = 0

    @property
    def total_active_calls(self) -> int:
        return (
            self.unbound_calls_in_flight
            + self.bound_calls_in_flight
            + self.answered_awaiting_agent
            + self.connected_calls
        )

    @property
    def total_agents(self) -> int:
        return (
            self.available_agents
            + self.reserved_agents
            + self.dialing_agents
            + self.connected_agents
            + self.wrap_up_agents
            + self.paused_agents
            + self.offline_agents
        )


@dataclass(frozen=True)
class AnswerRateEstimate:
    point: float
    planning_rate: float   # conservative (upper) rate used for capacity maths
    stderr: float
    samples: int
    confidence: float
    recent: float
    historical: float
    volatility: float

    def explain(self) -> dict[str, float | int]:
        return {
            "point": round(self.point, 4),
            "planning_rate": round(self.planning_rate, 4),
            "recent": round(self.recent, 4),
            "historical": round(self.historical, 4),
            "stderr": round(self.stderr, 4),
            "samples": self.samples,
            "confidence": round(self.confidence, 3),
            "volatility": round(self.volatility, 3),
        }


@dataclass(frozen=True)
class PacingRequest:
    """The pacing engine's *opinion*. It carries no authority to dial."""

    requested_calls: int
    estimate: AnswerRateEstimate
    capacity_now: float
    soon_free_credit: float
    expected_incoming_answers: float
    headroom: float
    safety_margin: float
    reason: str

    def explain(self) -> dict[str, Any]:
        return {
            "requested": self.requested_calls,
            "capacity_now": round(self.capacity_now, 2),
            "soon_free_credit": round(self.soon_free_credit, 2),
            "expected_incoming_answers": round(self.expected_incoming_answers, 2),
            "headroom": round(self.headroom, 2),
            "safety_margin": self.safety_margin,
            "reason": self.reason,
            **{f"est_{k}": v for k, v in self.estimate.explain().items()},
        }


@dataclass(frozen=True)
class SafetyDecision:
    """The only object the Call Allocator will act on.

    It cannot be constructed outside ``smartdialer.safety.controller``: the
    constructor requires a module-private issuer token. That makes
    "the predictive engine cannot bypass the Safety Controller" a property the
    test suite can assert rather than a comment in a document.
    """

    action: SafetyAction
    approved_calls: int
    requested_calls: int
    limiting_constraint: str
    caps: Mapping[str, float]
    snapshot: SystemSnapshot
    issuer_token: Any = None

    def __post_init__(self) -> None:
        from ..safety.controller import ISSUER_TOKEN  # local import: avoids cycle

        if self.issuer_token is not ISSUER_TOKEN:
            raise PermissionError(
                "SafetyDecision may only be created by the SafetyController"
            )

    def explain(self) -> dict[str, Any]:
        return {
            "decision": self.action.value,
            "requested": self.requested_calls,
            "approved": self.approved_calls,
            "limiting_constraint": self.limiting_constraint,
            "caps": {k: round(v, 2) for k, v in self.caps.items()},
        }


@dataclass(frozen=True)
class Reservation:
    """Result of an atomic check-and-reserve."""

    ok: bool
    entity_id: str | None
    reservation_id: str | None
    reason: str = ""

    def __bool__(self) -> bool:
        return self.ok
