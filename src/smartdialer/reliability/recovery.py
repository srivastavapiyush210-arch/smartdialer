"""Reconciliation / crash recovery.

The scenario the assignment asks about:

    agent reserved -> borrower reserved -> call initiated -> worker dies

Nothing in memory survives, so recovery has to be driven from persisted state.
Every reservation carries a timestamp, which makes it a *lease*: past its TTL we
assume the owner is gone.

The important rule is that we do **not** reset everything to AVAILABLE. A call
may still be live at the carrier, and freeing the agent while a borrower is
mid-conversation would be worse than the crash. So for each stale record we ask
the provider what is actually true and act on the answer:

    provider says live      -> renew the lease, leave the call alone
    provider says terminal  -> finish the call locally, release agent+borrower
    provider unreachable    -> leave it; try again next sweep (bounded by the
                               call TTL, after which we fail it closed)
    no provider id at all   -> the call never left our process: fail it and
                               return the borrower without burning an attempt

The sweep is also the safety net for the cross-aggregate work the event
processor does outside its transaction.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from ..clock import Clock
from ..config import DialerConfig
from ..logging_setup import get_logger, kv
from ..metrics.collector import M, MetricsCollector
from ..models.domain import ProviderError
from ..models.enums import AgentState, CallState
from ..repositories.agents import AgentRepository
from ..repositories.borrowers import BorrowerRepository
from ..repositories.calls import CallRepository
from .resilient_provider import ProviderRouter

log = get_logger("recovery")


@dataclass
class RecoveryReport:
    agents_released: int = 0
    calls_failed: int = 0
    calls_left_alone: int = 0
    borrowers_released: int = 0
    abandoned_orphans: int = 0
    stranded_agents_released: int = 0
    calls_reaped_from_offline_agents: int = 0

    def total(self) -> int:
        return (
            self.agents_released + self.calls_failed + self.borrowers_released
            + self.abandoned_orphans + self.stranded_agents_released
            + self.calls_reaped_from_offline_agents
        )


class RecoveryService:
    def __init__(
        self,
        config: DialerConfig,
        agents: AgentRepository,
        borrowers: BorrowerRepository,
        calls: CallRepository,
        router: ProviderRouter,
        metrics: MetricsCollector,
        clock: Clock,
    ) -> None:
        self._config = config
        self._agents = agents
        self._borrowers = borrowers
        self._calls = calls
        self._router = router
        self._metrics = metrics
        self._clock = clock

    # ------------------------------------------------------------------ sweep
    async def reconcile(self) -> RecoveryReport:
        report = RecoveryReport()
        self._metrics.incr(M.RECOVERY_RUNS)
        rel = self._config.reliability

        await self._recover_stale_calls(rel.call_setup_ttl_seconds, report)
        await self._recover_stale_agents(rel.agent_reservation_ttl_seconds, report)
        await self._recover_orphaned_answered(report)
        await self._recover_stale_borrowers(rel.borrower_reservation_ttl_seconds, report)
        await self._recover_stranded_agents(report)
        await self._reap_calls_of_offline_agents(report)

        if report.total():
            log.warning(kv("RECOVERY", agents_released=report.agents_released,
                           calls_failed=report.calls_failed,
                           calls_left_alone=report.calls_left_alone,
                           borrowers_released=report.borrowers_released,
                           abandoned_orphans=report.abandoned_orphans,
                           stranded_agents=report.stranded_agents_released,
                           reaped_offline=report.calls_reaped_from_offline_agents))
        return report

    async def _reap_calls_of_offline_agents(self, report: RecoveryReport) -> None:
        """End calls whose agent disappeared, and hang up at the carrier.

        A connected call counts as abandoned: the borrower was talking to
        somebody and now is not. Recording it any other way would flatter the
        abandonment numbers, which is the opposite of what they are for.
        """
        for call in await self._calls.find_with_offline_agent(
            self._config.campaign_id
        ):
            was_connected = call.state is CallState.CONNECTED
            if call.provider_call_id:
                for provider in self._router.providers:
                    if provider.name == call.provider:
                        await provider.cancel_call(call.provider_call_id)
                        break
            ended = (
                await self._calls.mark_abandoned(call.id, "agent_went_offline")
                if was_connected
                else await self._calls.force_terminal(
                    call.id, CallState.CANCELLED, "agent_went_offline"
                )
            )
            if not ended:
                continue
            report.calls_reaped_from_offline_agents += 1
            self._metrics.incr(M.CALLS_REAPED_OFFLINE_AGENT)
            if was_connected:
                self._metrics.incr(M.CALLS_ABANDONED)
            await self._borrowers.settle(
                call.borrower_id, answered=was_connected,
                outcome="agent_went_offline", cooldown_seconds=60.0,
            )

    async def _recover_stranded_agents(self, report: RecoveryReport) -> None:
        """Free agents left busy against a call that has already ended.

        The ordering bug that made this necessary is fixed, but a crash between
        the bridge's two writes can still produce the same state, and an agent
        that is silently unusable is the most expensive kind of leak: nobody
        notices until utilisation is quietly wrong.
        """
        for agent in await self._agents.find_stranded_busy():
            if await self._agents.release(agent.id, "stranded_without_live_call"):
                report.stranded_agents_released += 1
                self._metrics.incr(M.STRANDED_AGENTS_RECOVERED)

    async def _recover_stale_calls(self, ttl: float, report: RecoveryReport) -> None:
        for call in await self._calls.find_stuck_in_setup(ttl):
            state = await self._provider_state(call.provider, call.provider_call_id)
            if state is not None and not state.is_terminal():
                report.calls_left_alone += 1
                continue  # genuinely live: do not touch it
            if state is None and call.provider_call_id and await self._provider_unreachable(
                call.provider
            ):
                report.calls_left_alone += 1
                continue  # cannot tell; retry next sweep
            if await self._calls.force_terminal(
                call.id, CallState.FAILED, "recovered_stale_setup"
            ):
                report.calls_failed += 1
                self._metrics.incr(M.STALE_CALLS_RECOVERED)
                if call.agent_id:
                    if await self._agents.release(call.agent_id, "recovered_stale_call"):
                        report.agents_released += 1
                if call.provider_call_id is None:
                    # Never reached the carrier: no attempt should be charged.
                    await self._borrowers.release_reservation(
                        call.borrower_id, "recovered_never_dialled"
                    )
                else:
                    await self._borrowers.settle(
                        call.borrower_id, answered=False,
                        outcome="recovered_stale_setup", cooldown_seconds=60.0,
                    )
                report.borrowers_released += 1

    async def _recover_stale_agents(self, ttl: float, report: RecoveryReport) -> None:
        for agent in await self._agents.find_stale_reservations(ttl):
            call = await self._calls.active_call_for_agent(agent.id)
            if call is not None:
                state = await self._provider_state(call.provider, call.provider_call_id)
                if state is not None and not state.is_terminal():
                    report.calls_left_alone += 1
                    continue
                await self._calls.force_terminal(
                    call.id, CallState.FAILED, "recovered_orphaned_call"
                )
                await self._borrowers.settle(
                    call.borrower_id, answered=False,
                    outcome="recovered_orphaned_call", cooldown_seconds=60.0,
                )
                report.calls_failed += 1
            if await self._agents.release(agent.id, "recovered_stale_reservation"):
                report.agents_released += 1
                self._metrics.incr(M.STALE_AGENTS_RECOVERED)

    async def _recover_orphaned_answered(self, report: RecoveryReport) -> None:
        """Answered calls that nobody bridged (the worker died mid-bridge)."""
        grace = max(1.0, self._config.safety.abandon_grace_seconds * 4)
        for call in await self._calls.find_orphaned_answered(grace):
            if await self._calls.mark_abandoned(call.id, "recovered_orphaned_answer"):
                report.abandoned_orphans += 1
                self._metrics.incr(M.CALLS_ABANDONED)
                await self._borrowers.settle(
                    call.borrower_id, answered=True, outcome="abandoned_orphan",
                    cooldown_seconds=60.0,
                )

    async def _recover_stale_borrowers(self, ttl: float, report: RecoveryReport) -> None:
        for borrower in await self._borrowers.find_stale_reservations(ttl):
            active = await self._calls.get_active_for_borrower(borrower.id)
            if active is not None:
                continue  # a live call still owns this reservation
            if await self._borrowers.release_reservation(
                borrower.id, "recovered_stale_reservation"
            ):
                report.borrowers_released += 1
                self._metrics.incr(M.STALE_BORROWERS_RECOVERED)

    # -------------------------------------------------------------- provider
    async def _provider_state(self, name: str | None, provider_call_id: str | None):
        if not name or not provider_call_id:
            return None
        for provider in self._router.providers:
            if provider.name == name:
                try:
                    return await provider.get_status(provider_call_id)
                except ProviderError:
                    return None
        return None

    async def _provider_unreachable(self, name: str | None) -> bool:
        for provider in self._router.providers:
            if provider.name == name:
                health = await provider.health_check()
                return not health.healthy
        return False

    # ----------------------------------------------------------------- loop
    async def run_forever(self) -> None:
        interval = self._config.reliability.recovery_interval_seconds
        while True:
            try:
                await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                log.exception("recovery sweep failed: %s", exc)
            await self._clock.sleep(interval)


class WrapUpService:
    """Moves agents out of WRAP_UP once their disposition time has elapsed."""

    def __init__(
        self, config: DialerConfig, agents: AgentRepository, clock: Clock
    ) -> None:
        self._config = config
        self._agents = agents
        self._clock = clock

    async def tick(self) -> int:
        released = 0
        for agent in await self._agents.find_expired_wrap_up():
            if await self._agents.transition(
                agent.id, AgentState.AVAILABLE, "wrap up complete",
                expected=AgentState.WRAP_UP, clear_call=True,
            ):
                released += 1
        return released

    async def run_forever(self, interval: float = 1.0) -> None:
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                log.exception("wrap-up sweep failed")
            await self._clock.sleep(interval)
