"""Provider decorators: circuit breaker + bounded retry, then routing.

``ResilientProvider`` wraps any :class:`TelecomProvider` and satisfies the same
interface, so the allocator is unaware of it. ``ProviderRouter`` picks between
several resilient providers and fails over to a healthy one.
"""

from __future__ import annotations

from ..clock import Clock
from ..config import ReliabilityConfig
from ..logging_setup import get_logger, kv
from ..metrics.collector import M, MetricsCollector
from ..models.domain import (
    CallHandle,
    CallRequest,
    CircuitOpenError,
    ProviderError,
    ProviderHealth,
    ProviderTimeoutError,
)
from ..models.enums import CallState, CircuitState
from .circuit_breaker import CircuitBreaker
from .retry import RetryPolicy

log = get_logger("provider.resilient")


class ResilientProvider:
    def __init__(
        self,
        inner,
        config: ReliabilityConfig,
        clock: Clock,
        metrics: MetricsCollector,
    ) -> None:
        self.name = inner.name
        self.inner = inner
        self._clock = clock
        self._metrics = metrics
        self.breaker = CircuitBreaker(inner.name, config, clock)
        self.breaker.on_open = lambda: metrics.incr(M.CIRCUIT_OPENED)
        self.breaker.on_close = lambda: metrics.incr(M.CIRCUIT_CLOSED)
        self._retry = RetryPolicy(config)

    async def initiate_call(self, request: CallRequest) -> CallHandle:
        attempt = 0
        while True:
            attempt += 1
            if not self.breaker.allows_request():
                self._metrics.incr(M.CIRCUIT_REJECTED)
                raise CircuitOpenError(
                    f"{self.name}: circuit {self.breaker.state.value}",
                    provider=self.name,
                )
            started = self._clock.now()
            try:
                handle = await self.inner.initiate_call(request)
            except BaseException as exc:  # noqa: BLE001 - re-raised below
                self.breaker.record_failure()
                self._metrics.incr(M.PROVIDER_FAILURES)
                if isinstance(exc, ProviderTimeoutError):
                    self._metrics.incr(M.PROVIDER_TIMEOUTS)
                decision = self._retry.decide(attempt, exc)
                log.debug(kv("PROVIDER_ERR", provider=self.name, attempt=attempt,
                             error=type(exc).__name__, retry=decision.should_retry,
                             reason=decision.reason))
                if not decision.should_retry:
                    if decision.reason == "permanent_failure":
                        self._metrics.incr(M.PROVIDER_PERMANENT)
                    raise
                self._metrics.incr(M.PROVIDER_RETRIES)
                await self._clock.sleep(decision.delay_seconds)
                continue
            self.breaker.record_success()
            self._metrics.observe(
                M.H_SETUP_LATENCY, max(0.0, self._clock.now() - started)
            )
            return handle

    async def cancel_call(self, provider_call_id: str) -> bool:
        try:
            return await self.inner.cancel_call(provider_call_id)
        except ProviderError:
            return False

    async def get_status(self, provider_call_id: str) -> CallState | None:
        return await self.inner.get_status(provider_call_id)

    async def health_check(self) -> ProviderHealth:
        health = await self.inner.health_check()
        state = self.breaker.state
        healthy = health.healthy and state is not CircuitState.OPEN
        return ProviderHealth(
            name=health.name,
            healthy=healthy,
            success_rate=0.0 if state is CircuitState.OPEN else health.success_rate,
            latency_seconds=health.latency_seconds,
            circuit_state=state.value,
        )


class ProviderRouter:
    """Chooses a provider per call and fails over when the primary is unwell.

    Deliberately simple: ordered preference list, first healthy provider wins.
    That is enough to demonstrate isolation of provider failure without adding
    a load-balancing subsystem nobody asked for.
    """

    def __init__(self, providers: list[ResilientProvider], metrics: MetricsCollector) -> None:
        if not providers:
            raise ValueError("at least one provider is required")
        self.providers = providers
        self._metrics = metrics
        self._health: dict[str, ProviderHealth] = {}

    @property
    def primary(self) -> ResilientProvider:
        return self.providers[0]

    async def refresh_health(self) -> dict[str, ProviderHealth]:
        for provider in self.providers:
            self._health[provider.name] = await provider.health_check()
        return dict(self._health)

    def health_score(self) -> float:
        """Best available provider's score: the pacing input."""
        if not self._health:
            return 1.0
        return max(h.score for h in self._health.values())

    def circuit_state(self) -> str:
        states = {p.name: p.breaker.state.value for p in self.providers}
        return ",".join(f"{k}:{v}" for k, v in states.items())

    def select(self) -> ResilientProvider:
        for index, provider in enumerate(self.providers):
            health = self._health.get(provider.name)
            usable = provider.breaker.state is not CircuitState.OPEN and (
                health is None or health.healthy
            )
            if usable:
                if index > 0:
                    self._metrics.incr(M.PROVIDER_FAILOVER)
                return provider
        # Everything is unhealthy: return the primary so the caller gets a
        # CircuitOpenError rather than silently doing nothing.
        return self.providers[0]

    async def shutdown(self) -> None:
        for provider in self.providers:
            inner = getattr(provider, "inner", None)
            if inner is not None and hasattr(inner, "shutdown"):
                await inner.shutdown()
