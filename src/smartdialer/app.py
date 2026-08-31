"""Application wiring.

A modular monolith: one process, one SQLite file, explicit components joined by
constructor injection. Everything a distributed deployment would put behind a
network hop is a method call here, and the seams are exactly where the network
would go later (see docs/architecture.md).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from .allocation.allocator import CallAllocator
from .clock import Clock, RealClock
from .config import DialerConfig, DialMode
from .dialer.orchestrator import DialerOrchestrator
from .dialer.snapshot import SnapshotBuilder
from .events.processor import CallEventProcessor
from .logging_setup import get_logger
from .metrics.collector import MetricsCollector
from .pacing.engine import PacingEngine
from .pacing.estimator import AnswerRateEstimator
from .persistence.db import Database
from .providers.mock import MockTelecomProvider, ProviderProfile
from .reliability.recovery import RecoveryService, WrapUpService
from .reliability.resilient_provider import ProviderRouter, ResilientProvider
from .repositories.agents import AgentRepository
from .repositories.borrowers import BorrowerRepository
from .repositories.calls import CallRepository
from .safety.controller import SafetyController

log = get_logger("app")


@dataclass
class SmartDialerApp:
    config: DialerConfig
    clock: Clock
    db: Database
    metrics: MetricsCollector
    agents: AgentRepository
    borrowers: BorrowerRepository
    calls: CallRepository
    router: ProviderRouter
    estimator: AnswerRateEstimator
    pacing: PacingEngine
    safety: SafetyController
    allocator: CallAllocator
    events: CallEventProcessor
    recovery: RecoveryService
    wrap_up: WrapUpService
    orchestrator: DialerOrchestrator
    providers: list[MockTelecomProvider] = field(default_factory=list)
    _tasks: list[asyncio.Task] = field(default_factory=list)

    # ------------------------------------------------------------------ build
    @classmethod
    def build(
        cls,
        config: DialerConfig,
        *,
        clock: Clock | None = None,
        db: Database | None = None,
        profiles: list[ProviderProfile] | None = None,
        metrics: MetricsCollector | None = None,
        seed: int = 20240,
    ) -> "SmartDialerApp":
        clock = clock or RealClock()
        db = db or Database(config.db_path)
        db.initialise()
        metrics = metrics or MetricsCollector()

        agents = AgentRepository(db, clock)
        borrowers = BorrowerRepository(db, clock)
        calls = CallRepository(db, clock)

        profiles = profiles or [ProviderProfile.provider_a(), ProviderProfile.provider_b()]
        mocks = [
            MockTelecomProvider(profile, clock, seed=seed + i)
            for i, profile in enumerate(profiles)
        ]
        resilient = [
            ResilientProvider(mock, config.reliability, clock, metrics) for mock in mocks
        ]
        router = ProviderRouter(resilient, metrics)

        estimator = AnswerRateEstimator(config.pacing)
        pacing = PacingEngine(config.pacing, estimator)
        safety = SafetyController(config.safety, metrics)
        snapshots = SnapshotBuilder(agents, borrowers, calls, metrics, clock)
        allocator = CallAllocator(
            config, agents, borrowers, calls, router, safety, metrics, clock,
            snapshots=snapshots,
        )
        events = CallEventProcessor(
            config, agents, borrowers, calls, metrics, estimator, clock,
            allocator=allocator,
        )
        for mock in mocks:
            mock.set_event_sink(events.handle)

        recovery = RecoveryService(config, agents, borrowers, calls, router, metrics, clock)
        wrap_up = WrapUpService(config, agents, clock)
        orchestrator = DialerOrchestrator(
            config, snapshots, pacing, safety, allocator, router, metrics, clock
        )

        return cls(
            config=config, clock=clock, db=db, metrics=metrics, agents=agents,
            borrowers=borrowers, calls=calls, router=router, estimator=estimator,
            pacing=pacing, safety=safety, allocator=allocator, events=events,
            recovery=recovery, wrap_up=wrap_up, orchestrator=orchestrator,
            providers=mocks,
        )

    # ------------------------------------------------------------------ setup
    async def seed_campaign(self, agents: int, borrowers: int, *, max_attempts: int = 3):
        await self.db.run_in_transaction(
            lambda c: c.execute(
                "INSERT OR REPLACE INTO campaigns (id, name, max_concurrent_calls, active) "
                "VALUES (?,?,?,?)",
                (self.config.campaign_id, "collections", 100000, 1),
            )
        )
        await self.agents.bulk_create(self.config.campaign_id, agents)
        await self.borrowers.bulk_create(
            self.config.campaign_id, borrowers, max_attempts=max_attempts
        )

    # ---------------------------------------------------------------- runtime
    async def start(self, *, run_recovery: bool = True) -> None:
        """Startup reconciliation first, then the background loops."""
        if run_recovery:
            await self.recovery.reconcile()
        self._tasks = [
            asyncio.create_task(self.orchestrator.run_forever(), name="dialer"),
            asyncio.create_task(self.wrap_up.run_forever(), name="wrap-up"),
            asyncio.create_task(self.recovery.run_forever(), name="recovery"),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        await self.router.shutdown()
        await self.allocator.drain()
        await self.events.drain()

    async def crash(self) -> None:
        """Simulate a hard worker crash: stop everything, persist nothing extra.

        No cleanup, no draining, no releasing reservations -- exactly what a
        SIGKILL looks like to the database.
        """
        for task in self._tasks:
            task.cancel()
        self._tasks = []
        for provider in self.providers:
            await provider.shutdown()

    def set_mode(self, mode: DialMode) -> None:
        self.orchestrator.mode = mode

    def close(self) -> None:
        self.db.close()
