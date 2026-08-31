"""Call Allocator -- the only component that talks to a telecom provider.

Two entry points:

``run_progressive(limit)``
    Strictly one available agent -> one outbound call. The agent is reserved
    *before* the call exists, so the invariant
    ``agent-bound calls in flight <= agents reserved for dialling`` holds by
    construction: a call cannot be created without first winning an agent.

``execute(decision)``
    Predictive. Accepts a :class:`SafetyDecision` and nothing else; a forged or
    hand-built decision raises ``PermissionError`` before any I/O happens. It
    starts *unbound* calls: no agent is held while the phone rings, which is
    where predictive dialling's utilisation win comes from. An agent is only
    grabbed at the instant the borrower answers.

Failure handling is uniform: whatever happens, the agent and the borrower are
released, and the call is driven to a terminal state. A failed setup never
leaves an agent stranded.
"""

from __future__ import annotations

import asyncio
import uuid

from ..clock import Clock
from ..config import DialerConfig, DialMode
from ..logging_setup import get_logger, kv
from ..metrics.collector import M, MetricsCollector
from ..models.domain import (
    Borrower,
    CallRequest,
    CircuitOpenError,
    PermanentProviderError,
    Reservation,
    SafetyDecision,
    SystemSnapshot,
)
from ..models.enums import AgentState, CallState, SafetyAction
from ..reliability.resilient_provider import ProviderRouter
from ..repositories.agents import AgentRepository
from ..repositories.borrowers import BorrowerRepository
from ..repositories.calls import CallRepository, DuplicateActiveCall
from ..safety.controller import ISSUER_TOKEN, SafetyController

log = get_logger("allocator")


class CallAllocator:
    def __init__(
        self,
        config: DialerConfig,
        agents: AgentRepository,
        borrowers: BorrowerRepository,
        calls: CallRepository,
        router: ProviderRouter,
        safety: SafetyController,
        metrics: MetricsCollector,
        clock: Clock,
        snapshots=None,
    ) -> None:
        self._config = config
        self._agents = agents
        self._borrowers = borrowers
        self._calls = calls
        self._router = router
        self._safety = safety
        self._metrics = metrics
        self._clock = clock
        self._snapshots = snapshots
        self._dial_tasks: set[asyncio.Task] = set()

    # ------------------------------------------------------------ progressive
    async def run_progressive(self, limit: int | None = None) -> int:
        """Start up to ``limit`` agent-bound calls. Returns how many started.

        Dialled in bounded concurrent waves rather than one at a time. Carrier
        setup is I/O bound, so serialising it makes the control loop late and
        agents sit idle waiting for a decision that has not been taken yet.
        Concurrency here is safe precisely because agent reservation is a
        compare-and-swap: N racing workers produce exactly one winner per agent
        and the losers simply see ``lost_race``.
        """
        budget = limit if limit is not None else self._config.progressive_batch_limit
        remaining = max(0, budget)
        started = 0
        while remaining > 0:
            wave = min(remaining, self._config.max_parallel_dials)
            results = await self._run_wave(
                self._progressive_one() for _ in range(wave)
            )
            started += sum(1 for r in results if r is True)
            for result in results:
                if isinstance(result, BaseException):
                    log.warning(kv("ALLOCATION", event="dial_failed",
                                   error=type(result).__name__))
            # ``None`` means "no agent was free", which is the natural end of
            # progressive dialling: there is nobody left to dial for.
            if any(r is None for r in results):
                break
            remaining -= wave
        return started

    async def _run_wave(self, coros) -> list:
        """Run one wave of dials, shielded from the caller's cancellation.

        A dial is reserve-then-release: win an agent, try to find a borrower,
        give the agent back if anything fails. Cancelling the orchestrator
        between those steps leaves the agent RESERVED until its lease expires,
        which is a real agent sitting idle for up to
        ``agent_reservation_ttl_seconds``. Same reasoning as the event path:
        once the work has started it finishes, and ``drain`` waits for it.
        """
        tasks = []
        for coro in coros:
            task = asyncio.create_task(coro)
            self._dial_tasks.add(task)
            task.add_done_callback(self._dial_tasks.discard)
            tasks.append(task)
        return await asyncio.shield(
            asyncio.gather(*tasks, return_exceptions=True)
        )

    async def drain(self) -> None:
        """Wait for in-flight dials, including any they spawn."""
        for _ in range(100):
            pending = [t for t in self._dial_tasks if not t.done()]
            if not pending:
                return
            await asyncio.gather(*pending, return_exceptions=True)
        log.warning(kv("ALLOCATION", event="drain_incomplete",
                       pending=len(self._dial_tasks)))

    async def _progressive_one(self) -> bool | None:
        reservation = await self._agents.reserve_any_available(self._config.campaign_id)
        if not reservation.ok:
            if reservation.reason == "lost_race":
                self._metrics.incr(M.AGENT_RESERVE_CONTENTION)
            return None
        self._metrics.incr(M.AGENT_RESERVE_SUCCESS)
        return await self._dial_one(
            agent_id=reservation.entity_id,
            reservation_id=reservation.reservation_id,
            mode=DialMode.PROGRESSIVE,
        )

    # ------------------------------------------------------------- predictive
    async def execute(self, decision: SafetyDecision) -> int:
        """Start exactly as many unbound calls as the Safety Controller allowed.

        The approved number is an upper bound, never a target to hit at any
        cost: running out of eligible borrowers or a provider refusing calls
        both end the batch early, and so does the hard cap being reached
        part-way through.
        """
        if decision.issuer_token is not ISSUER_TOKEN:
            raise PermissionError("allocator only accepts authentic SafetyDecisions")
        if decision.action in (SafetyAction.REJECT, SafetyAction.FALLBACK_TO_PROGRESSIVE):
            return 0

        checkpoint = self._config.safety.revalidate_every_calls
        remaining = decision.approved_calls
        started = 0
        first_wave = True
        while remaining > 0:
            # The first wave needs no revalidation: the orchestrator built the
            # snapshot this decision rests on microseconds ago, and rebuilding
            # it here would only buy a stale answer at the price of three more
            # queries. Revalidation earns its keep *between* waves, where real
            # time has passed and agents may have gone offline underneath us.
            if checkpoint > 0 and self._snapshots is not None and not first_wave:
                if await self._hard_cap_reached():
                    log.warning(kv("ALLOCATION", event="stopped_early",
                                   started=started,
                                   approved=decision.approved_calls,
                                   reason="hard_cap_reached_midway"))
                    self._metrics.incr(M.ALLOCATION_STOPPED_EARLY)
                    break
            wave = min(remaining, self._config.max_parallel_dials,
                       checkpoint or remaining)
            results = await self._run_wave(
                self._dial_one(agent_id=None, reservation_id=None,
                               mode=DialMode.PREDICTIVE)
                for _ in range(wave)
            )
            succeeded = sum(
                1 for r in results if r is True and not isinstance(r, BaseException)
            )
            started += succeeded
            if succeeded == 0:
                # Nothing in the whole wave worked: out of borrowers, or the
                # provider is refusing. Grinding through the rest of the batch
                # would only manufacture failures.
                break
            remaining -= wave
            first_wave = False
        return started

    async def _hard_cap_reached(self) -> bool:
        snapshot = await self._snapshots.build(
            self._config.campaign_id,
            provider_health=self._router.health_score(),
            circuit_state=self._router.circuit_state(),
        )
        return self._safety.excess_unbound_calls(snapshot) > 0

    # --------------------------------------------------------------- one call
    async def _dial_one(
        self, *, agent_id: str | None, reservation_id: str | None, mode: DialMode
    ) -> bool:
        borrower_reservation, borrower = await self._borrowers.reserve_next(
            self._config.campaign_id
        )
        if not borrower_reservation.ok or borrower is None:
            if borrower_reservation.reason == "lost_race":
                self._metrics.incr(M.BORROWER_RESERVE_CONTENTION)
            if agent_id:
                await self._agents.release(agent_id, "no_eligible_borrower")
            return False

        call_id = f"C{uuid.uuid4().hex[:14]}"
        try:
            await self._calls.create(
                call_id,
                self._config.campaign_id,
                borrower.id,
                mode,
                agent_id=agent_id,
                reservation_id=reservation_id or borrower_reservation.reservation_id,
            )
        except DuplicateActiveCall:
            # Database-level invariant fired: this borrower already has a live
            # call. Give both resources back, do not burn an attempt.
            await self._borrowers.release_reservation(borrower.id, "duplicate_active_call")
            if agent_id:
                await self._agents.release(agent_id, "duplicate_active_call")
            return False

        if agent_id:
            await self._agents.transition(
                agent_id, AgentState.DIALING, "call created",
                expected=AgentState.RESERVED, call_id=call_id,
            )

        return await self._initiate(call_id, borrower, agent_id)

    async def _initiate(
        self, call_id: str, borrower: Borrower, agent_id: str | None
    ) -> bool:
        provider = self._router.select()
        request = CallRequest(
            call_id=call_id,
            borrower_id=borrower.id,
            phone=borrower.phone,
            campaign_id=self._config.campaign_id,
            # Sent, but honoured by no adapter in this prototype. Retrying a
            # timed-out initiate CAN create a second physical call today.
            idempotency_key=f"{call_id}",
        )
        try:
            handle = await provider.initiate_call(request)
        except BaseException as exc:  # noqa: BLE001 - classified below
            await self._fail_setup(call_id, borrower, agent_id, exc)
            return False

        await self._calls.mark_initiated(call_id, handle.provider, handle.provider_call_id)
        self._metrics.incr(M.CALLS_INITIATED)
        log.debug(kv("ALLOCATION", call=call_id, borrower=borrower.id,
                     agent=agent_id or "-", provider=handle.provider,
                     bound=bool(agent_id)))
        return True

    async def _fail_setup(
        self, call_id: str, borrower: Borrower, agent_id: str | None, exc: BaseException
    ) -> None:
        permanent = isinstance(exc, PermanentProviderError)
        circuit_open = isinstance(exc, CircuitOpenError)
        reason = type(exc).__name__

        self._metrics.incr(M.SETUP_FAILURES)
        await self._calls.force_terminal(call_id, CallState.FAILED, reason)

        if circuit_open:
            # The call never reached the carrier, so it would be unfair to the
            # borrower (and wrong for retry accounting) to consume an attempt.
            await self._borrowers.release_reservation(borrower.id, reason)
        else:
            await self._borrowers.settle(
                borrower.id,
                answered=False,
                outcome=reason,
                cooldown_seconds=self._retry_cooldown(),
                permanent_failure=permanent,
            )
        if agent_id:
            # Invariant 12: a failed setup must never permanently consume an agent.
            await self._agents.release(agent_id, f"setup_failed:{reason}")
        log.debug(kv("ALLOCATION", event="setup_failed", call=call_id,
                     agent=agent_id or "-", error=reason, permanent=permanent))

    def _retry_cooldown(self) -> float:
        return max(30.0, self._config.reliability.base_backoff_seconds * 60)

    # ------------------------------------------------------- answering bridge
    async def bridge_to_agent(self, call_id: str) -> str | None:
        """Attach an agent to a call the borrower has just answered.

        This is the moment predictive dialling can go wrong. We try once
        immediately; if no agent is free we keep trying for
        ``abandon_grace_seconds`` (real dialers hold a very short grace window
        for exactly this reason) before declaring the call abandoned.
        """
        deadline = self._clock.now() + self._config.safety.abandon_grace_seconds
        attempts = 0
        while True:
            attempts += 1
            reservation: Reservation = await self._agents.reserve_any_available(
                self._config.campaign_id
            )
            if reservation.ok and reservation.entity_id:
                agent_id = reservation.entity_id
                connected = await self._calls.mark_connected(call_id, agent_id)
                if not connected:
                    # The call ended (or was cancelled) while we were looking
                    # for an agent. Give the agent straight back.
                    await self._agents.release(agent_id, "call_ended_before_bridge")
                    return None
                moved = await self._agents.transition(
                    agent_id, AgentState.CONNECTED, "bridged to answered call",
                    expected=AgentState.RESERVED,
                    expected_reservation_id=reservation.reservation_id,
                    call_id=call_id,
                )
                if not moved:
                    # The finaliser released this agent while we were bridging
                    # and somebody else has already claimed it. Not ours.
                    return None
                # The call can also end *after* we take the agent. Then the
                # finaliser ran before agent_id was visible to it, so nobody
                # else will clean this up. Pinned release, so we cannot free an
                # agent that has since moved on to different work.
                latest = await self._calls.get(call_id)
                if latest is None or latest.state.is_terminal():
                    await self._agents.release(
                        agent_id, "call_ended_mid_bridge", expected_call_id=call_id
                    )
                    return None
                self._metrics.incr(M.CALLS_CONNECTED)
                if attempts > 1:
                    self._metrics.incr(M.CALLS_RESCUED_BY_GRACE)
                return agent_id

            if self._clock.now() >= deadline:
                return None
            await self._clock.sleep(0.25)

    # --------------------------------------------------------- safety actions
    async def cancel_excess_unbound(self, snapshot: SystemSnapshot) -> int:
        """Cancel the youngest ringing unbound calls to restore the hard cap.

        Called when capacity shrinks underneath calls that were safe when they
        were placed -- most obviously when a block of agents goes offline.
        Answered calls are never touched; hanging up on a borrower who has
        already said "hello" is precisely the outcome we are avoiding.
        """
        excess = self._safety.excess_unbound_calls(snapshot)
        if excess <= 0:
            return 0
        victims = await self._calls.newest_unbound_ringing(
            self._config.campaign_id, excess
        )
        cancelled = 0
        for call in victims:
            if call.provider_call_id:
                for provider in self._router.providers:
                    if provider.name == call.provider:
                        await provider.cancel_call(call.provider_call_id)
                        break
            if await self._calls.force_terminal(
                call.id, CallState.CANCELLED, "safety_capacity_shrank"
            ):
                await self._borrowers.release_reservation(
                    call.borrower_id, "safety_cancelled"
                )
                cancelled += 1
        if cancelled:
            self._metrics.incr(M.SAFETY_CANCELLATIONS, cancelled)
            self._metrics.incr(M.CALLS_CANCELLED, cancelled)
            log.warning(kv("SAFETY", event="cancelled_ringing_calls",
                           count=cancelled, available=snapshot.available_agents,
                           unbound_in_flight=snapshot.unbound_calls_in_flight))
        return cancelled
