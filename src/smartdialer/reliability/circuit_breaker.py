"""Circuit breaker.

Purpose: when a provider starts failing, stop *asking* it. Without this, every
tick would fire a fresh batch of doomed requests, each one burning retries --
the classic retry storm that turns a provider blip into an outage of our own.

    CLOSED    normal. Count consecutive failures.
    OPEN      reject immediately for ``cooldown_seconds``. Nothing reaches the
              provider, so it gets room to recover and we stop wasting agents.
    HALF_OPEN after the cooldown, let a couple of probes through. Enough
              successes -> CLOSED. Any failure -> OPEN again (cooldown restarts).
"""

from __future__ import annotations

from dataclasses import dataclass

from ..clock import Clock
from ..config import ReliabilityConfig
from ..logging_setup import get_logger, kv
from ..models.enums import CircuitState

log = get_logger("breaker")


@dataclass
class BreakerStats:
    state: CircuitState
    consecutive_failures: int
    successes_in_half_open: int
    opened_at: float | None
    total_rejections: int


class CircuitBreaker:
    def __init__(self, name: str, config: ReliabilityConfig, clock: Clock) -> None:
        self.name = name
        self._config = config
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._half_open_successes = 0
        self._half_open_inflight = 0
        self._opened_at: float | None = None
        self._rejections = 0
        self.on_open = None  # optional callback for metrics
        self.on_close = None

    @property
    def state(self) -> CircuitState:
        self._maybe_half_open()
        return self._state

    def stats(self) -> BreakerStats:
        return BreakerStats(
            self.state, self._failures, self._half_open_successes,
            self._opened_at, self._rejections,
        )

    def allows_request(self) -> bool:
        """Should this request be sent to the provider at all?"""
        state = self.state
        if state is CircuitState.CLOSED:
            return True
        if state is CircuitState.OPEN:
            self._rejections += 1
            return False
        if self._half_open_inflight < self._config.breaker_half_open_max_calls:
            self._half_open_inflight += 1
            return True
        self._rejections += 1
        return False

    def record_success(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._half_open_inflight = max(0, self._half_open_inflight - 1)
            self._half_open_successes += 1
            if self._half_open_successes >= self._config.breaker_success_threshold:
                self._close()
            return
        self._failures = 0

    def record_failure(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._half_open_inflight = max(0, self._half_open_inflight - 1)
            self._open()
            return
        self._failures += 1
        if self._failures >= self._config.breaker_failure_threshold:
            self._open()

    # ------------------------------------------------------------- internals
    def _open(self) -> None:
        was = self._state
        self._state = CircuitState.OPEN
        self._opened_at = self._clock.now()
        self._half_open_successes = 0
        self._half_open_inflight = 0
        if was is not CircuitState.OPEN:
            log.warning(kv("BREAKER", provider=self.name, state="OPEN",
                           consecutive_failures=self._failures))
            if self.on_open:
                self.on_open()

    def _close(self) -> None:
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._half_open_successes = 0
        self._half_open_inflight = 0
        self._opened_at = None
        log.info(kv("BREAKER", provider=self.name, state="CLOSED"))
        if self.on_close:
            self.on_close()

    def _maybe_half_open(self) -> None:
        if self._state is not CircuitState.OPEN or self._opened_at is None:
            return
        if self._clock.now() - self._opened_at >= self._config.breaker_cooldown_seconds:
            self._state = CircuitState.HALF_OPEN
            self._half_open_successes = 0
            self._half_open_inflight = 0
            log.info(kv("BREAKER", provider=self.name, state="HALF_OPEN"))
