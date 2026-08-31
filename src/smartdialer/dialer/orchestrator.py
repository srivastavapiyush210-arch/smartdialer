"""Dialer orchestrator: the control loop.

One tick, in order:

1. refresh provider health;
2. build the snapshot both decision-makers will read;
3. enforce the continuous safety invariant (cancel ringing unbound calls if
   capacity shrank underneath them);
4. decide how to dial:
      PROGRESSIVE -> one available agent, one call, no prediction involved;
      PREDICTIVE  -> pacing engine proposes, Safety Controller disposes,
                     allocator executes only what was approved.

Step 3 runs before step 4 on purpose: give capacity back before spending it.

Note the shape of the predictive branch. The pacing engine is called, its
answer is passed to the Safety Controller, and the *controller's* object is
what reaches the allocator. There is no branch in which ``pacing.compute``'s
number is used directly.
"""

from __future__ import annotations

import asyncio

from ..clock import Clock
from ..config import DialerConfig, DialMode
from ..logging_setup import get_logger, kv
from ..metrics.collector import M, MetricsCollector
from ..models.enums import SafetyAction
from ..pacing.engine import PacingEngine
from ..reliability.resilient_provider import ProviderRouter
from ..safety.controller import SafetyController
from .snapshot import SnapshotBuilder

log = get_logger("dialer")


class DialerOrchestrator:
    def __init__(
        self,
        config: DialerConfig,
        snapshots: SnapshotBuilder,
        pacing: PacingEngine,
        safety: SafetyController,
        allocator,
        router: ProviderRouter,
        metrics: MetricsCollector,
        clock: Clock,
    ) -> None:
        self._config = config
        self._snapshots = snapshots
        self._pacing = pacing
        self._safety = safety
        self._allocator = allocator
        self._router = router
        self._metrics = metrics
        self._clock = clock
        self.mode = config.mode
        self.ticks = 0

    async def tick(self) -> int:
        """Run one control cycle. Returns the number of calls started."""
        self.ticks += 1
        await self._router.refresh_health()
        snapshot = await self._snapshots.build(
            self._config.campaign_id,
            provider_health=self._router.health_score(),
            circuit_state=self._router.circuit_state(),
        )

        await self._allocator.cancel_excess_unbound(snapshot)

        if self.mode is DialMode.PROGRESSIVE:
            started = await self._allocator.run_progressive()
            if started:
                log.debug(kv("PROGRESSIVE", started=started,
                             available=snapshot.available_agents))
            return started

        request = self._pacing.compute(snapshot)
        self._metrics.incr(M.PACING_REQUESTS)
        decision = self._safety.evaluate(request, snapshot)

        if decision.action is SafetyAction.FALLBACK_TO_PROGRESSIVE:
            # Deterministic behaviour is always available as a floor: we lose
            # utilisation, we do not lose safety.
            # Only dial into agents that are not already spoken for by unbound
            # calls still in flight, otherwise we would immediately have to
            # cancel our own ringing calls.
            limit = max(
                0,
                snapshot.available_agents
                - snapshot.unbound_calls_in_flight
                - snapshot.answered_awaiting_agent,
            )
            started = await self._allocator.run_progressive(limit=limit)
            log.warning(kv("FALLBACK", mode="progressive", started=started,
                           reason=decision.limiting_constraint,
                           confidence=request.estimate.confidence))
            return started

        if decision.action is SafetyAction.REJECT:
            return 0

        started = await self._allocator.execute(decision)
        log.debug(kv("ALLOCATION", approved=decision.approved_calls, started=started))
        return started

    async def run_forever(self) -> None:
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                log.exception("dialer tick failed")
            await self._clock.sleep(self._config.tick_seconds)
