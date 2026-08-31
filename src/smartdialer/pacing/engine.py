"""Predictive pacing engine.

This module is *pure*. It has no database handle, no provider, no allocator --
grep the imports. All it can do is return a number and an explanation of how
that number was produced. Executing anything is somebody else's job, and there
is no code path from here to a telephone.

The calculation, in words:

    capacity        = available agents
                      + (agents likely to finish during setup) x discount
    incoming        = unbound calls already ringing x planning answer rate
    headroom        = capacity - incoming - safety margin
    requested       = floor(headroom / planning answer rate)

"Agents likely to finish during setup" uses a memoryless approximation of call
duration: with mean handle time ``T`` and a setup horizon ``H``, a call in
progress ends within the horizon with probability ``1 - exp(-H/T)``. Agents in
WRAP_UP are counted with the same treatment against the wrap-up time. The
result is multiplied by ``soon_free_discount`` (default 0.5) because being
wrong about this is exactly how borrowers get abandoned.

Worked example (the "why 17 and not 10" question):

    available=20, wrap_up=6, connected=30, unbound_ringing=10
    planning_rate=0.55, AHT=90s, horizon=20s
    soon_free = 6*(1-e^(-20/15))*0.5 + 30*(1-e^(-20/90))*0.5  = 2.34 + 2.99 = 5.33
    capacity  = 20 + 5.33 = 25.33
    incoming  = 10 * 0.55 = 5.5
    headroom  = 25.33 - 5.5 - 1 (margin) = 18.83
    requested = floor(18.83 / 0.55) = 34

...and the Safety Controller will then cut that to 10 in STRICT mode. Every one
of those intermediate numbers is in the returned ``PacingRequest`` and in the
log line, so the arithmetic can be checked by hand.
"""

from __future__ import annotations

import math

from ..config import PacingConfig
from ..logging_setup import get_logger, kv
from ..models.domain import PacingRequest, SystemSnapshot
from .estimator import AnswerRateEstimator

log = get_logger("pacing")


class PacingEngine:
    """Produces a *request*. It carries no authority."""

    def __init__(self, config: PacingConfig, estimator: AnswerRateEstimator) -> None:
        self._config = config
        self._estimator = estimator

    @property
    def estimator(self) -> AnswerRateEstimator:
        return self._estimator

    def compute(self, snapshot: SystemSnapshot) -> PacingRequest:
        cfg = self._config
        estimate = self._estimator.estimate()
        rate = max(0.02, estimate.planning_rate)

        soon_free = self._soon_free_agents(snapshot)
        capacity_now = float(snapshot.available_agents)
        capacity = capacity_now + soon_free

        expected_incoming = snapshot.unbound_calls_in_flight * rate
        # Calls that already answered and are waiting for an agent consume
        # capacity right now, not probabilistically.
        expected_incoming += snapshot.answered_awaiting_agent

        margin = cfg.safety_margin_calls
        headroom = capacity - expected_incoming - margin

        if headroom <= 0:
            requested = 0
            reason = "no_headroom"
        else:
            requested = int(math.floor(headroom / rate))
            reason = "headroom_available"

        # A degraded provider means longer setup times and more failures; asking
        # for more calls in that state only builds a backlog.
        if snapshot.provider_health < 1.0:
            requested = int(math.floor(requested * snapshot.provider_health))
            reason = f"{reason}+provider_health_scaled"

        requested = max(0, min(requested, cfg.max_request_per_tick,
                               snapshot.eligible_borrowers))

        request = PacingRequest(
            requested_calls=requested,
            estimate=estimate,
            capacity_now=capacity_now,
            soon_free_credit=soon_free,
            expected_incoming_answers=expected_incoming,
            headroom=headroom,
            safety_margin=margin,
            reason=reason,
        )
        log.info(kv("PACING", **request.explain()))
        return request

    def _soon_free_agents(self, snapshot: SystemSnapshot) -> float:
        """Expected agents freeing up within the setup horizon, discounted."""
        cfg = self._config
        horizon = cfg.setup_horizon_seconds
        talk = max(1.0, self._estimator.talk_seconds)
        wrap = max(1.0, cfg.default_wrap_up_seconds)

        p_call_ends = 1.0 - math.exp(-horizon / talk)
        p_wrap_ends = 1.0 - math.exp(-horizon / wrap)
        raw = snapshot.connected_agents * p_call_ends + snapshot.wrap_up_agents * p_wrap_ends
        return raw * cfg.soon_free_discount
