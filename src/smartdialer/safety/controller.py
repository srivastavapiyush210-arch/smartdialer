"""Safety Controller -- the non-bypassable boundary.

Everything upstream of this module is allowed to be probabilistic. Everything
downstream is required to be deterministic. The controller sits between them
and answers one question: *given what is true right now, how many new calls may
be started?*

Three properties make it a real boundary rather than a naming convention:

1. It recomputes its own numbers from the snapshot. It never reuses the pacing
   engine's arithmetic, so a broken estimator cannot inflate a cap.
2. It emits :class:`SafetyDecision`, which cannot be constructed anywhere else
   (the constructor demands a module-private token). The Call Allocator accepts
   nothing else. Forging a decision raises ``PermissionError``.
3. The pacing package imports no provider and no allocator, so there is no code
   path from prediction to a telephone. ``tests/unit/test_safety_boundary.py``
   asserts all three of these mechanically.

Caps are combined with ``min``. The smallest one is reported as
``limiting_constraint``, so "why only 8?" always has a one-word answer.
"""

from __future__ import annotations

import math

from ..config import SafetyConfig, SafetyMode
from ..logging_setup import get_logger, kv
from ..metrics.collector import M, MetricsCollector
from ..models.domain import PacingRequest, SafetyDecision, SystemSnapshot
from ..models.enums import SafetyAction

log = get_logger("safety")

# Private capability token. Holding a reference to this object is what makes a
# SafetyDecision authentic; only this module has it at construction time.
ISSUER_TOKEN = object()

_UNBOUNDED = float("inf")


def _solve_max_unbound(capacity: float, p: float, z: float) -> float:
    """Largest U with ``U*p + z*sqrt(U*p*(1-p)) <= capacity``.

    Substituting ``x = sqrt(U)`` turns it into ``p*x^2 + b*x - capacity = 0``
    with ``b = z*sqrt(p(1-p))``, so the positive root is closed form. Pure
    arithmetic: deterministic, unit-testable, and checkable on paper.
    """
    if capacity <= 0:
        return 0.0
    p = min(1.0, max(0.01, p))
    if p >= 0.999:
        return capacity
    b = z * math.sqrt(p * (1.0 - p))
    x = (-b + math.sqrt(b * b + 4.0 * p * capacity)) / (2.0 * p)
    return max(0.0, x * x)


class SafetyController:
    def __init__(
        self,
        config: SafetyConfig,
        metrics: MetricsCollector,
        *,
        campaign_max_concurrent: int = 10_000,
    ) -> None:
        self._config = config
        self._metrics = metrics
        self._campaign_max_concurrent = campaign_max_concurrent

    # ------------------------------------------------------------- evaluation
    def evaluate(
        self, request: PacingRequest, snapshot: SystemSnapshot
    ) -> SafetyDecision:
        cfg = self._config
        caps: dict[str, float] = {}

        caps["agent_capacity"] = self._agent_capacity_cap(snapshot)
        caps["global_concurrency"] = max(
            0.0, cfg.max_concurrent_calls - snapshot.total_active_calls
        )
        caps["campaign_concurrency"] = max(
            0.0, self._campaign_max_concurrent - snapshot.total_active_calls
        )
        caps["provider_health"] = self._provider_cap(snapshot)
        caps["abandon_budget"] = self._abandon_cap(snapshot)
        caps["borrower_supply"] = float(snapshot.eligible_borrowers)

        limiting = min(caps, key=lambda k: caps[k])
        safe_capacity = int(max(0.0, math.floor(min(caps.values()))))

        action, approved = self._choose(request, snapshot, safe_capacity, caps)

        decision = SafetyDecision(
            action=action,
            approved_calls=approved,
            requested_calls=request.requested_calls,
            limiting_constraint=limiting,
            caps=caps,
            snapshot=snapshot,
            issuer_token=ISSUER_TOKEN,
        )
        self._record(decision)
        log.info(kv("SAFETY", **decision.explain(),
                    available=snapshot.available_agents,
                    unbound_in_flight=snapshot.unbound_calls_in_flight,
                    confidence=request.estimate.confidence,
                    provider_health=snapshot.provider_health))
        return decision

    def _choose(
        self,
        request: PacingRequest,
        snapshot: SystemSnapshot,
        safe_capacity: int,
        caps: dict[str, float],
    ) -> tuple[SafetyAction, int]:
        cfg = self._config

        # The estimator has lost the plot (too few samples, or recent behaviour
        # has diverged sharply from history). Predictive dialling depends on the
        # estimate being roughly right, so we stop depending on it and dial the
        # deterministic way instead.
        if request.estimate.confidence < cfg.min_estimator_confidence:
            return SafetyAction.FALLBACK_TO_PROGRESSIVE, 0

        # Abandonment budget blown: fall back rather than merely reduce.
        if caps["abandon_budget"] <= 0:
            return SafetyAction.FALLBACK_TO_PROGRESSIVE, 0

        # Provider is unusable: dialling anything is pointless and would just
        # queue retries. Progressive would not help either, so reject outright.
        if caps["provider_health"] <= 0:
            return SafetyAction.REJECT, 0

        if safe_capacity <= 0:
            return SafetyAction.REJECT, 0
        if request.requested_calls <= 0:
            return SafetyAction.REJECT, 0
        if safe_capacity < request.requested_calls:
            return SafetyAction.REDUCE, safe_capacity
        return SafetyAction.APPROVE, request.requested_calls

    # ------------------------------------------------------------------- caps
    def _agent_capacity_cap(self, snapshot: SystemSnapshot) -> float:
        """The cap that actually prevents abandoned calls."""
        committed = snapshot.unbound_calls_in_flight + snapshot.answered_awaiting_agent
        return max(0.0, self.max_unbound_in_flight(snapshot) - committed)

    def max_unbound_in_flight(self, snapshot: SystemSnapshot) -> float:
        """How many unbound calls may be ringing at once.

        Both safety modes are the *same* formula with a different assumed
        answer rate, which is why STRICT is not a special case bolted on:

            find the largest U such that
                U*p + z*sqrt(U*p*(1-p))  <=  capacity

        i.e. "even at the upper end of the plausible range, the number of
        borrowers who pick up does not exceed the agents available to take
        them". Solving for U has a closed form (quadratic in sqrt(U)).

        STRICT   assumes p = 1.0. Every ringing phone might answer at once, so
                 U <= capacity: at the moment of the decision there is an agent
                 for every call authorised. That is an arithmetic property of
                 the decision. It becomes zero abandonment only if the agent
                 pool does not shrink before those calls are answered -- see
                 SafetyMode for the measured numbers when it does.
        BALANCED assumes p = the controller's own upper-confidence estimate of
                 the answer rate, with z sigma of head-room, and adds a
                 discounted credit for agents about to finish wrapping up.

        Note the pooling effect that falls out of the maths: the safe overshoot
        grows with sqrt(capacity), so a 500-agent pool can be dialled far more
        aggressively than a 5-agent pool. That is the real reason predictive
        dialling pays off at scale, and the formula says so out loud.
        """
        cfg = self._config
        if cfg.mode is SafetyMode.STRICT:
            return float(max(0, snapshot.available_agents))

        capacity = snapshot.available_agents + (
            snapshot.wrap_up_agents * cfg.soon_free_discount
        )
        p = self._independent_answer_rate_upper_bound(snapshot)
        solved = _solve_max_unbound(capacity, p, cfg.overshoot_sigmas)
        # Absolute ceiling on overshoot. The statistics assume the answer rate
        # is roughly stationary over the setup horizon; when that assumption
        # breaks the arithmetic can authorise a very large number. This bounds
        # how wrong a single tick is allowed to be, in calls rather than sigmas.
        return min(solved, capacity + cfg.max_overshoot_calls)

    def _independent_answer_rate_upper_bound(self, snapshot: SystemSnapshot) -> float:
        """The controller's *own* answer-rate estimate.

        Deliberately not the pacing engine's number. If the controller reused
        the estimate it is meant to be policing, a broken estimator could
        inflate its own cap: report p = 0.01 and the formula would happily
        authorise a hundred calls per agent.

        Computed twice -- once over the whole campaign, once over the recent
        window -- and the *higher* of the two upper bounds wins. A cumulative
        mean is blind to a regime change, and blindness here is expensive: a
        campaign that dialled for an hour at 5% keeps reporting 5% long after
        the true rate jumps to 95%, authorises an enormous overshoot, and
        abandons the difference. Measured at 23.75% abandonment before the
        window was added. High answer rates are the dangerous side, so every
        error this function can make is forced upward.
        """
        cumulative = self._upper_bound(
            snapshot.dials_observed, snapshot.answers_observed
        )
        if snapshot.recent_dials < self._config.min_recent_dials:
            return cumulative
        recent = self._upper_bound(snapshot.recent_dials, snapshot.recent_answers)
        return max(cumulative, recent)

    def _upper_bound(self, dials: int, answers: int) -> float:
        cfg = self._config
        k = cfg.controller_prior_strength
        dials = max(0, dials)
        answers = max(0, answers)
        p_hat = (answers + k * cfg.controller_prior_answer_rate) / (dials + k)
        p_hat = min(1.0, max(0.01, p_hat))
        stderr = math.sqrt(max(1e-9, p_hat * (1 - p_hat) / (dials + k)))
        return min(1.0, p_hat + 2.0 * stderr)

    def _provider_cap(self, snapshot: SystemSnapshot) -> float:
        if snapshot.provider_health < self._config.min_provider_health:
            return 0.0
        # Degraded-but-usable provider: scale capacity down with health so we
        # reduce load rather than hammering it.
        return _UNBOUNDED if snapshot.provider_health >= 0.99 else math.floor(
            max(0.0, snapshot.available_agents * snapshot.provider_health)
        )

    def _abandon_cap(self, snapshot: SystemSnapshot) -> float:
        """Reactive throttle on observed abandonment. Not a guarantee.

        This is worth being blunt about, because the name invites the wrong
        reading. It is measured *after* the fact and it can only stop the next
        call, never the one that already went wrong. A cumulative rate is also
        self-healing in the worst way: abandon 19 calls early on and a long
        campaign dilutes them below the threshold while nothing has actually
        improved. So the recent window governs once it has enough samples, and
        an absolute count of abandoned calls in that window is an independent
        trip -- a short campaign cannot hide behind "too few answers to judge".

        The deterministic bound lives in ``max_unbound_in_flight``. This is a
        second line, not the line.
        """
        cfg = self._config
        if snapshot.recent_abandoned >= cfg.max_abandoned_in_window:
            return 0.0
        if snapshot.recent_dials >= cfg.min_recent_dials:
            recent_answers = max(1, snapshot.recent_answers)
            if snapshot.recent_abandoned / recent_answers > cfg.max_abandon_rate:
                return 0.0
        if snapshot.answers_observed < cfg.min_answers_for_abandon_rate:
            return _UNBOUNDED
        rate = snapshot.abandoned_observed / max(1, snapshot.answers_observed)
        return 0.0 if rate > cfg.max_abandon_rate else _UNBOUNDED

    # -------------------------------------------------- continuous invariant
    def excess_unbound_calls(self, snapshot: SystemSnapshot) -> int:
        """How many in-flight unbound calls exceed current safe capacity.

        Evaluated every tick, not only when dialling. This is what reacts when
        40 agents vanish: those calls were safe when they were placed and are
        not safe now, and the cheapest remedy is to cancel the ones that are
        still ringing, before anybody picks up.
        """
        if not self._config.cancel_excess_ringing:
            return 0
        committed = snapshot.unbound_calls_in_flight + snapshot.answered_awaiting_agent
        allowed = self.max_unbound_in_flight(snapshot)
        return int(max(0, math.ceil(committed - allowed)))

    # ---------------------------------------------------------------- metrics
    def _record(self, decision: SafetyDecision) -> None:
        if decision.action is SafetyAction.APPROVE:
            self._metrics.incr(M.SAFETY_APPROVALS)
        elif decision.action is SafetyAction.REDUCE:
            self._metrics.incr(M.SAFETY_REDUCTIONS)
        elif decision.action is SafetyAction.REJECT:
            self._metrics.incr(M.SAFETY_REJECTIONS)
        else:
            self._metrics.incr(M.SAFETY_FALLBACKS)
        self._metrics.incr(M.SAFETY_APPROVED_CALLS, decision.approved_calls)
        self._metrics.incr(M.PACING_REQUESTED_CALLS, decision.requested_calls)
