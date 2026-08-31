"""Configurable mock telecom providers.

One class, two profiles:

* **Provider A** -- fast, reliable, well-ordered events, few duplicates.
* **Provider B** -- slow, times out, duplicates events, and delivers them out
  of order (both adjacent transpositions and full reversals such as
  ``COMPLETED, ANSWERED, RINGING``).

Everything is driven by a seeded ``random.Random`` so a simulation with the
same seed replays identically.

The mock also plays the part of the borrower: it decides whether a call is
answered (using ``answer_rate``) and how long the conversation lasts. That is
the *ground truth* the pacing engine's estimator has to learn.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from dataclasses import dataclass, field, replace

from ..clock import Clock
from ..logging_setup import get_logger, kv
from ..models.domain import (
    CallHandle,
    CallRequest,
    PermanentProviderError,
    ProviderEvent,
    ProviderHealth,
    ProviderTimeoutError,
)
from ..models.enums import CallState, ProviderEventType
from .base import EventSink

log = get_logger("provider")


@dataclass(frozen=True)
class ProviderProfile:
    """Every knob a simulation may vary."""

    name: str
    setup_latency_seconds: float = 0.4
    setup_latency_jitter: float = 0.2
    ring_seconds: float = 8.0
    answer_rate: float = 0.30
    talk_seconds_mean: float = 90.0
    timeout_probability: float = 0.0
    permanent_failure_probability: float = 0.0
    duplicate_event_probability: float = 0.0
    out_of_order_probability: float = 0.0
    full_reversal_share: float = 0.35   # of out-of-order calls, how many reverse
    health_window: int = 50

    @staticmethod
    def provider_a(**overrides: object) -> "ProviderProfile":
        base = ProviderProfile(
            name="provider-a",
            setup_latency_seconds=0.3,
            setup_latency_jitter=0.15,
            ring_seconds=8.0,
            timeout_probability=0.005,
            permanent_failure_probability=0.01,
            duplicate_event_probability=0.01,
            out_of_order_probability=0.0,
        )
        return replace(base, **overrides)  # type: ignore[arg-type]

    @staticmethod
    def provider_b(**overrides: object) -> "ProviderProfile":
        base = ProviderProfile(
            name="provider-b",
            setup_latency_seconds=1.2,
            setup_latency_jitter=0.8,
            ring_seconds=10.0,
            timeout_probability=0.08,
            permanent_failure_probability=0.04,
            duplicate_event_probability=0.15,
            out_of_order_probability=0.25,
        )
        return replace(base, **overrides)  # type: ignore[arg-type]


@dataclass
class _CallSim:
    call_id: str
    provider_call_id: str
    sequence: int = 0
    state: CallState = CallState.INITIATED
    cancelled: bool = False
    reorder: str = "none"           # none | swap | reverse
    held: list[ProviderEvent] = field(default_factory=list)


class MockTelecomProvider:
    """Implements :class:`TelecomProvider`."""

    def __init__(
        self,
        profile: ProviderProfile,
        clock: Clock,
        *,
        seed: int = 1234,
        event_sink: EventSink | None = None,
    ) -> None:
        self.name = profile.name
        self.profile = profile
        self._clock = clock
        self._rng = random.Random(seed)
        self._sink: EventSink | None = event_sink
        self._calls: dict[str, _CallSim] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._recent: list[bool] = []
        self._outage = False
        self._latency_ewma = profile.setup_latency_seconds

    # ------------------------------------------------------------------ wiring
    def set_event_sink(self, sink: EventSink) -> None:
        self._sink = sink

    def set_outage(self, outage: bool) -> None:
        """Failure injection: flip the carrier off (or back on)."""
        self._outage = outage
        log.warning(kv("PROVIDER", provider=self.name,
                       event="outage_started" if outage else "outage_cleared"))

    def override_profile(self, **overrides: object) -> None:
        """Change behaviour mid-run (e.g. answer rate collapses)."""
        self.profile = replace(self.profile, **overrides)  # type: ignore[arg-type]

    # ------------------------------------------------------------- interface
    async def initiate_call(self, request: CallRequest) -> CallHandle:
        latency = max(
            0.0,
            self.profile.setup_latency_seconds
            + self._rng.uniform(
                -self.profile.setup_latency_jitter, self.profile.setup_latency_jitter
            ),
        )
        await self._clock.sleep(latency)
        self._latency_ewma = 0.8 * self._latency_ewma + 0.2 * latency

        if self._outage or self._rng.random() < self.profile.timeout_probability:
            self._record(False)
            raise ProviderTimeoutError(
                f"{self.name}: timeout placing call", provider=self.name
            )
        if self._rng.random() < self.profile.permanent_failure_probability:
            self._record(False)
            raise PermanentProviderError(
                f"{self.name}: invalid destination number", provider=self.name
            )

        self._record(True)
        provider_call_id = f"{self.name}:{uuid.uuid4().hex[:10]}"
        sim = _CallSim(request.call_id, provider_call_id)
        if self._rng.random() < self.profile.out_of_order_probability:
            sim.reorder = (
                "reverse"
                if self._rng.random() < self.profile.full_reversal_share
                else "swap"
            )
        self._calls[provider_call_id] = sim
        self._spawn(self._run_call(sim))
        return CallHandle(self.name, provider_call_id, self._clock.now())

    async def cancel_call(self, provider_call_id: str) -> bool:
        sim = self._calls.get(provider_call_id)
        if sim is None or sim.state.is_terminal():
            return False
        sim.cancelled = True
        return True

    async def get_status(self, provider_call_id: str) -> CallState | None:
        if self._outage:
            raise ProviderTimeoutError(
                f"{self.name}: status unavailable", provider=self.name
            )
        sim = self._calls.get(provider_call_id)
        return sim.state if sim else None

    async def health_check(self) -> ProviderHealth:
        if not self._recent:
            success_rate = 1.0
        else:
            success_rate = sum(self._recent) / len(self._recent)
        healthy = (not self._outage) and success_rate >= 0.5
        return ProviderHealth(
            name=self.name,
            healthy=healthy,
            success_rate=success_rate,
            latency_seconds=self._latency_ewma,
            circuit_state="N/A",
        )

    # ------------------------------------------------------------- simulation
    async def _run_call(self, sim: _CallSim) -> None:
        """Drive one call's lifecycle and emit provider events."""
        try:
            await self._emit(sim, ProviderEventType.RINGING)
            await self._clock.sleep(self.profile.ring_seconds)
            if sim.cancelled:
                sim.state = CallState.CANCELLED
                await self._emit(sim, ProviderEventType.CANCELLED, final=True)
                return
            if self._rng.random() >= self.profile.answer_rate:
                sim.state = CallState.FAILED
                await self._emit(sim, ProviderEventType.NO_ANSWER, final=True)
                return

            sim.state = CallState.ANSWERED
            await self._emit(sim, ProviderEventType.ANSWERED)
            talk = self._rng.expovariate(1.0 / max(1e-6, self.profile.talk_seconds_mean))
            talk = min(talk, self.profile.talk_seconds_mean * 6)
            await self._clock.sleep(talk)
            sim.state = CallState.COMPLETED
            await self._emit(sim, ProviderEventType.COMPLETED, final=True)
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            raise

    async def _emit(
        self, sim: _CallSim, event_type: ProviderEventType, *, final: bool = False
    ) -> None:
        sim.sequence += 1
        event = ProviderEvent(
            event_id=f"E{uuid.uuid4().hex[:14]}",
            call_id=sim.call_id,
            type=event_type,
            sequence=sim.sequence,
            timestamp=self._clock.now(),
            provider=self.name,
        )

        if sim.reorder == "reverse":
            # Hold every event; deliver the whole call backwards at the end.
            sim.held.append(event)
            if final:
                for held in reversed(sim.held):
                    await self._deliver(held)
                sim.held.clear()
            return

        if sim.reorder == "swap" and not final:
            # Adjacent transposition: hold one event, release it after the next.
            if sim.held:
                previous = sim.held.pop()
                await self._deliver(event)
                await self._deliver(previous)
            else:
                sim.held.append(event)
            return

        if sim.held:  # flush anything still held before a final event
            await self._deliver(event)
            for held in sim.held:
                await self._deliver(held)
            sim.held.clear()
            return

        await self._deliver(event)

    async def _deliver(self, event: ProviderEvent) -> None:
        if self._sink is None:
            return
        await self._sink(event)
        if self._rng.random() < self.profile.duplicate_event_probability:
            # Exactly the same event id: the receiver must ignore it entirely.
            await self._sink(event)

    # ------------------------------------------------------------- internals
    def _record(self, success: bool) -> None:
        self._recent.append(success)
        if len(self._recent) > self.profile.health_window:
            self._recent.pop(0)

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def shutdown(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        self._tasks.clear()
