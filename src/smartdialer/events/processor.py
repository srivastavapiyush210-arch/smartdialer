"""Call Event Processor.

Receives untrusted provider events and turns them into state changes, metrics
and follow-up work. The hard parts (idempotency, staleness, terminal
protection) live in :mod:`repositories.calls` and :mod:`state.call_fsm`; this
module is about what happens *after* an event is accepted.

Follow-up work deliberately happens outside the event's transaction:

* the ledger write guarantees **at most once** side effects;
* the reconciler guarantees they **eventually** happen.

That is the standard trade: one small transaction per aggregate plus a repair
loop, instead of one giant transaction spanning agents, borrowers and calls.
"""

from __future__ import annotations

import asyncio

from ..clock import Clock
from ..config import DialerConfig
from ..logging_setup import get_logger, kv
from ..metrics.collector import M, MetricsCollector
from ..models.domain import Call, ProviderEvent
from ..models.enums import AgentState, CallState, EventOutcome
from ..pacing.estimator import AnswerRateEstimator
from ..repositories.agents import AgentRepository
from ..repositories.borrowers import BorrowerRepository
from ..repositories.calls import CallRepository, EventApplication

log = get_logger("events")

_OUTCOME_METRIC = {
    EventOutcome.DUPLICATE: M.DUPLICATE_EVENTS,
    EventOutcome.STALE: M.OUT_OF_ORDER_EVENTS,
    EventOutcome.TERMINAL_PROTECTED: M.TERMINAL_PROTECTED,
    EventOutcome.INVALID: M.INVALID_TRANSITIONS,
    EventOutcome.NO_OP: M.NOOP_EVENTS,
}


class CallEventProcessor:
    def __init__(
        self,
        config: DialerConfig,
        agents: AgentRepository,
        borrowers: BorrowerRepository,
        calls: CallRepository,
        metrics: MetricsCollector,
        estimator: AnswerRateEstimator,
        clock: Clock,
        allocator=None,
    ) -> None:
        self._config = config
        self._agents = agents
        self._borrowers = borrowers
        self._calls = calls
        self._metrics = metrics
        self._estimator = estimator
        self._clock = clock
        self._allocator = allocator
        self._bridge_tasks: set[asyncio.Task[None]] = set()

    def set_allocator(self, allocator) -> None:
        self._allocator = allocator

    # ------------------------------------------------------------------ entry
    async def handle(self, event: ProviderEvent) -> EventApplication:
        """Apply one provider event, cancellation-safe as a whole.

        Accepting an event and acting on it must not come apart. The commit
        happens in a worker thread, so a cancellation arriving while it is in
        flight would otherwise leave the state change durable and the follow-up
        work unstarted -- the agent stays CONNECTED against a completed call.

        So the entire unit runs as a tracked task and the caller merely waits on
        a shield. Cancelling the caller (the carrier's delivery task, on
        shutdown) detaches the wait; the work itself continues and ``drain``
        waits for it. A hard crash still needs the reconciler, which is the
        point of having one.
        """
        if not self._config.shield_event_handling:
            return await self._handle(event)
        task = asyncio.create_task(self._handle(event))
        self._bridge_tasks.add(task)
        task.add_done_callback(self._bridge_tasks.discard)
        return await asyncio.shield(task)

    async def _handle(self, event: ProviderEvent) -> EventApplication:
        started = self._clock.now()
        self._metrics.incr(M.EVENTS_RECEIVED)
        application = await self._calls.apply_provider_event(event)

        metric = _OUTCOME_METRIC.get(application.outcome)
        if metric:
            self._metrics.incr(metric)
        if application.late_answer_merged:
            self._metrics.incr(M.LATE_ANSWER_MERGED)

        if application.outcome is not EventOutcome.APPLIED:
            log.debug(kv("EVENT", call=event.call_id, type=event.type.value,
                         seq=event.sequence, outcome=application.outcome.value,
                         reason=application.reason))
            self._metrics.observe(
                M.H_EVENT_LATENCY, (self._clock.now() - started) * 1000
            )
            return application

        self._metrics.incr(M.EVENTS_APPLIED)
        await self._apply_side_effects(event, application)
        self._metrics.observe(M.H_EVENT_LATENCY, (self._clock.now() - started) * 1000)
        return application

    # ---------------------------------------------------------- side effects
    async def _apply_side_effects(
        self, event: ProviderEvent, application: EventApplication
    ) -> None:
        call = application.call
        assert call is not None
        new_state = application.new_state

        if new_state is CallState.ANSWERED:
            self._metrics.incr(M.CALLS_ANSWERED)
            if call.agent_id:
                await self._connect_bound_call(call)
            else:
                self._spawn(self._bridge_unbound_call(call))
            return

        if new_state is not None and new_state.is_terminal():
            await self._finalise(call, new_state)

    async def _connect_bound_call(self, call: Call) -> None:
        """Progressive path: the agent is already held, so bridging cannot fail."""
        if await self._calls.mark_connected(call.id, call.agent_id or ""):
            # Pinned to this call: if the call ended while we were bridging and
            # the agent has already been released and re-dialled for something
            # else, this must not drag it back onto a dead call.
            moved = await self._agents.transition(
                call.agent_id or "", AgentState.CONNECTED, "borrower answered",
                expected=(AgentState.DIALING, AgentState.RESERVED),
                expected_call_id=call.id, call_id=call.id,
            )
            if moved:
                self._metrics.incr(M.CALLS_CONNECTED)

    async def _bridge_unbound_call(self, call: Call) -> None:
        """Predictive path: find an agent now, or record an abandoned call."""
        if self._allocator is None:  # pragma: no cover - wiring guard
            return
        agent_id = await self._allocator.bridge_to_agent(call.id)
        if agent_id is not None:
            return
        current = await self._calls.get(call.id)
        if current is None or current.state is not CallState.ANSWERED:
            return  # the call already ended by itself; nothing was abandoned
        if await self._calls.mark_abandoned(call.id, "no_agent_available"):
            self._metrics.incr(M.CALLS_ABANDONED)
            log.error(kv("SAFETY_VIOLATION", event="abandoned_call", call=call.id,
                         reason="answered_with_no_available_agent"))
            await self._borrowers.settle(
                call.borrower_id, answered=True, outcome="abandoned",
                cooldown_seconds=self._config.wrap_up_seconds,
            )

    async def _finalise(self, call: Call, new_state: CallState) -> None:
        """Release resources when a call reaches a terminal state.

        ``call`` is the row as it looked *inside* the event transaction. That
        copy is fine for deciding what happened, but not for deciding whose
        agent to release: a predictive bridge running concurrently may have
        written ``agent_id`` in the meantime, and finalising against the stale
        NULL leaves that agent stuck CONNECTED against a dead call. So the
        agent linkage is re-read from durable state. The two writes serialise
        in SQLite, so by the time this runs the bridge has either committed
        (we see its agent_id) or it has not and its own mark_connected will
        fail against the now-terminal call.
        """
        current = await self._calls.get(call.id) or call
        answered = current.answered_at is not None or call.state in (
            CallState.ANSWERED, CallState.CONNECTED
        )
        was_connected = current.connected_at is not None

        if new_state is CallState.COMPLETED:
            self._metrics.incr(M.CALLS_COMPLETED)
        elif new_state is CallState.FAILED:
            self._metrics.incr(M.CALLS_FAILED)
        else:
            self._metrics.incr(M.CALLS_CANCELLED)

        # The estimator learns exactly once per dial attempt, at the end.
        self._estimator.observe_outcome(answered)
        self._metrics.record_dial_outcome(
            answered=answered, abandoned=bool(current.abandoned)
        )
        self._metrics.gauge(
            M.G_ESTIMATED_ANSWER_RATE, self._estimator.estimate().point
        )
        self._metrics.gauge(
            M.G_ESTIMATOR_CONFIDENCE, self._estimator.estimate().confidence
        )

        if was_connected and current.connected_at is not None:
            duration = max(0.0, self._clock.now() - current.connected_at)
            self._metrics.observe(M.H_CALL_DURATION, duration)
            self._estimator.observe_talk_time(duration)

        agent_id = current.agent_id or call.agent_id
        if agent_id:
            moved = False
            if was_connected:
                moved = await self._agents.transition(
                    agent_id, AgentState.WRAP_UP, "call ended",
                    expected=AgentState.CONNECTED, expected_call_id=call.id,
                    wrap_up_until=self._clock.now() + self._config.wrap_up_seconds,
                    call_id=call.id,
                )
            if not moved:
                # Either the call never connected, or the bridge had not yet
                # moved the agent out of RESERVED when we looked. Releasing
                # covers both; the bridge re-checks the call afterwards and
                # will not resurrect a CONNECTED agent on a dead call.
                await self._agents.release(
                    agent_id, f"call_{new_state.value.lower()}",
                    expected_call_id=call.id,
                )

        await self._borrowers.settle(
            call.borrower_id,
            answered=answered,
            outcome=new_state.value,
            cooldown_seconds=self._retry_cooldown(),
        )

    def _retry_cooldown(self) -> float:
        return max(30.0, self._config.reliability.base_backoff_seconds * 60)

    # -------------------------------------------------------------- internals
    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._bridge_tasks.add(task)
        task.add_done_callback(self._bridge_tasks.discard)

    async def drain(self) -> None:
        """Wait for all in-flight event work, including work it spawns.

        An event handler can start a bridge task, so a single snapshot of the
        set is not enough -- loop until it stays empty. Bounded so a task that
        keeps spawning work cannot hang shutdown forever.
        """
        for _ in range(100):
            pending = [t for t in self._bridge_tasks if not t.done()]
            if not pending:
                return
            await asyncio.gather(*pending, return_exceptions=True)
        log.warning(kv("EVENTS", event="drain_incomplete",
                       pending=len(self._bridge_tasks)))
