"""Configuration objects.

All durations are expressed in *simulated seconds* (see ``clock.py``).
Every tunable lives here so there are no magic numbers scattered through the
business logic, and so a demo can override one knob without editing code.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum


class DialMode(str, Enum):
    """How the orchestrator decides to place calls."""

    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE = "PREDICTIVE"


class SafetyMode(str, Enum):
    """How much statistical risk the Safety Controller will tolerate.

    What is deterministic here is the *decision*, not the outcome. The
    controller can guarantee what it authorises at dial time; it cannot
    guarantee what the world does between dialling and the borrower answering.
    Stating it the other way round would be a lie, and it was one this file
    told until the numbers below were measured.

    STRICT   -- unbound calls in flight <= currently available agents. Even a
                100% answer rate is absorbed, so with a stable agent pool no
                call is abandoned. Agents vanishing mid-flight can still cause
                abandonment: a call that already reached ANSWERED cannot be
                cancelled, because hanging up on someone who has said hello is
                the outcome being avoided. Measured 0% on an answer-rate spike,
                1.1% on a mass agent drop, 7.1% on both at once.
    BALANCED -- EXPERIMENTAL. Deliberately over-dials using the estimated
                answer rate. Carries no abandonment-rate guarantee of any kind.
                max_abandon_rate is a reactive throttle, not a bound: it is
                measured after the fact and can only stop the next call.
                Measured 6.1% on an answer-rate spike and 31% on a spike
                combined with a mass agent drop. Opt in knowingly.

    PROGRESSIVE dialling was the only configuration that abandoned nobody in
    every adversarial run, because the agent is reserved before the call
    exists. That is the floor the system falls back to.
    """

    STRICT = "STRICT"
    BALANCED = "BALANCED"


@dataclass(frozen=True)
class PacingConfig:
    """Inputs to the predictive pacing engine and its answer-rate estimator."""

    # Blend of recent (sliding window) and historical (Bayesian-smoothed) rate.
    recent_window: int = 25
    min_recent_samples: int = 8
    recent_weight: float = 0.50
    # How many standard errors of divergence between the recent window and the
    # long-run rate counts as a full-blown regime change.
    regime_change_sigmas: float = 4.0
    # Beta prior: pretend we have already seen ``prior_strength`` dials at
    # ``prior_answer_rate``. Stops 1 answer out of 2 dials meaning "100%".
    prior_answer_rate: float = 0.25
    prior_strength: float = 20.0
    # Conservatism: we plan against answer_rate + z * stderr, i.e. we assume
    # MORE borrowers may answer than expected, because that is the risky side.
    confidence_z: float = 1.0
    # Number of observed dials at which the estimator is considered confident.
    samples_for_confidence: int = 40
    # Horizon used to estimate how many busy agents will free up during setup.
    setup_horizon_seconds: float = 20.0
    default_talk_seconds: float = 90.0
    default_wrap_up_seconds: float = 15.0
    # Fraction of "about to be free" agents we are willing to count as capacity.
    soon_free_discount: float = 0.50
    # Keep this many agents in reserve beyond the arithmetic.
    safety_margin_calls: float = 1.0
    # Never ask for more than this in a single tick (sanity bound).
    max_request_per_tick: int = 200


@dataclass(frozen=True)
class SafetyConfig:
    """Hard limits enforced by the Safety Controller. Never bypassable."""

    mode: SafetyMode = SafetyMode.STRICT
    max_concurrent_calls: int = 2000
    # BALANCED only: standard deviations of head-room kept between expected
    # answers and available capacity. 2.0 leaves roughly a 2% tail.
    overshoot_sigmas: float = 2.0
    soon_free_discount: float = 0.50
    # The controller's own answer-rate prior, kept deliberately pessimistic
    # (high) so that early in a campaign it assumes lots of people will answer.
    controller_prior_answer_rate: float = 0.50
    controller_prior_strength: float = 20.0
    # Abandonment budget. Above this observed rate, predictive dialling stops.
    max_abandon_rate: float = 0.03
    min_answers_for_abandon_rate: int = 20
    # Samples needed before the rolling window is trusted over the cumulative
    # figure, and an absolute trip that does not wait for a rate to converge.
    min_recent_dials: int = 40
    max_abandoned_in_window: int = 5
    # Hard ceiling on how far BALANCED may overshoot available capacity,
    # independent of what the arithmetic would allow. Bounds the worst case
    # when every assumption behind the estimate turns out to be wrong at once.
    max_overshoot_calls: int = 25
    # Answered-with-no-agent calls wait this long for an agent before we give up.
    abandon_grace_seconds: float = 2.0
    # Below this estimator confidence we refuse to dial predictively.
    min_estimator_confidence: float = 0.35
    # Cancel excess ringing unbound calls when agents disappear.
    cancel_excess_ringing: bool = True
    # Provider health below this forces progressive fallback.
    min_provider_health: float = 0.50
    # Defence in depth. A batch approved by the controller is dialled in
    # *waves*; before each wave we rebuild the snapshot and re-check the hard
    # cap, so a batch approved when 40 agents were online stops part-way if
    # they disappear. Revalidating before every single call was measurably
    # worse than useless: it serialised the control loop (see docs/adr.md,
    # ADR-11). 0 disables revalidation entirely.
    revalidate_every_calls: int = 8


@dataclass(frozen=True)
class ReliabilityConfig:
    """Retries, circuit breaker and stale-state recovery."""

    max_retries: int = 2
    base_backoff_seconds: float = 0.5
    max_backoff_seconds: float = 8.0
    backoff_jitter: float = 0.30

    breaker_failure_threshold: int = 5
    breaker_cooldown_seconds: float = 15.0
    breaker_half_open_max_calls: int = 2
    breaker_success_threshold: int = 2

    # Leases. A reservation older than its TTL is presumed orphaned by a crash.
    agent_reservation_ttl_seconds: float = 45.0
    borrower_reservation_ttl_seconds: float = 120.0
    call_setup_ttl_seconds: float = 60.0
    recovery_interval_seconds: float = 10.0


@dataclass(frozen=True)
class DialerConfig:
    """Top-level wiring."""

    campaign_id: str = "CAMP-1"
    mode: DialMode = DialMode.PROGRESSIVE
    tick_seconds: float = 1.0
    wrap_up_seconds: float = 15.0
    db_path: str = "data/runtime/smartdialer.sqlite3"
    # Max agent-bound calls started per progressive tick (throughput smoothing).
    progressive_batch_limit: int = 100
    # Calls are placed concurrently, in bounded waves. Carrier setup is I/O
    # bound and a real dialer issues many requests at once; dialling serially
    # starves the control loop, which is the single biggest throughput bug we
    # found while building this (see docs/adr.md, ADR-11).
    max_parallel_dials: int = 16
    # Shield event handling from caller cancellation. On by default: without it,
    # cancelling a carrier delivery task between the committed state change and
    # the follow-up work strands agents. Exposed as a knob only so the load test
    # can measure what it costs (scripts/run_load_test.py --shield-comparison).
    shield_event_handling: bool = True
    pacing: PacingConfig = field(default_factory=PacingConfig)
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    reliability: ReliabilityConfig = field(default_factory=ReliabilityConfig)

    def with_mode(self, mode: DialMode) -> "DialerConfig":
        return replace(self, mode=mode)

    def with_safety_mode(self, mode: SafetyMode) -> "DialerConfig":
        return replace(self, safety=replace(self.safety, mode=mode))
