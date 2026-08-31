"""Simulation engine.

Runs the real system -- real repositories, real SQLite, real event processor,
real Safety Controller -- against mock carriers on a time-compressed clock.
Nothing is stubbed out for the sake of nicer numbers.

Agent utilisation is measured by sampling agent states on a fixed interval and
integrating: ``utilisation = connected agent-seconds / online agent-seconds``.
That is the number a collections operations team actually cares about, and it
is the one predictive dialling is supposed to improve.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from ..app import SmartDialerApp
from ..clock import ScaledClock
from ..config import DialerConfig, DialMode
from ..logging_setup import get_logger, kv
from ..metrics.collector import M, MetricsCollector
from ..models.enums import AgentState
from ..persistence.db import Database
from ..providers.mock import ProviderProfile
from .scenarios import Injection, Scenario

log = get_logger("simulation")


@dataclass
class SimulationResult:
    scenario: str
    mode: str
    safety_mode: str
    duration_seconds: float
    agents: int
    metrics: dict[str, Any]
    agent_utilisation: float
    average_idle_seconds_per_agent: float
    observed_answer_rate: float
    timeline: list[tuple[float, str, str]] = field(default_factory=list)

    # ------------------------------------------------------------- accessors
    def counter(self, name: str) -> float:
        return self.metrics["counters"].get(name, 0.0)

    @property
    def abandoned(self) -> float:
        return self.counter(M.CALLS_ABANDONED)

    def summary_row(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "mode": self.mode,
            "safety": self.safety_mode,
            "initiated": int(self.counter(M.CALLS_INITIATED)),
            "answered": int(self.counter(M.CALLS_ANSWERED)),
            "connected": int(self.counter(M.CALLS_CONNECTED)),
            "completed": int(self.counter(M.CALLS_COMPLETED)),
            "failed": int(self.counter(M.CALLS_FAILED)),
            "utilisation": round(self.agent_utilisation, 4),
            "unproductive_s/agent": round(self.average_idle_seconds_per_agent, 1),
            "answer_rate": round(self.observed_answer_rate, 3),
            "abandoned": int(self.abandoned),
            "approvals": int(self.counter(M.SAFETY_APPROVALS)),
            "reductions": int(self.counter(M.SAFETY_REDUCTIONS)),
            "rejections": int(self.counter(M.SAFETY_REJECTIONS)),
            "fallbacks": int(self.counter(M.SAFETY_FALLBACKS)),
            "cancelled_by_safety": int(self.counter(M.SAFETY_CANCELLATIONS)),
            "dupes": int(self.counter(M.DUPLICATE_EVENTS)),
            "out_of_order": int(self.counter(M.OUT_OF_ORDER_EVENTS)),
            "provider_failures": int(self.counter(M.PROVIDER_FAILURES)),
            "retries": int(self.counter(M.PROVIDER_RETRIES)),
        }

    def render(self) -> str:
        row = self.summary_row()
        width = max(len(k) for k in row)
        return "\n".join(f"  {k:<{width}} : {v}" for k, v in row.items())


class SimulationRunner:
    def __init__(self, scenario: Scenario, *, verbose: bool = False) -> None:
        self.scenario = scenario
        self.verbose = verbose
        self._utilisation_samples: list[tuple[int, int, int]] = []

    async def run(self) -> SimulationResult:
        sc = self.scenario
        clock = ScaledClock(scale=sc.time_scale)
        metrics = MetricsCollector()

        config = DialerConfig(
            mode=sc.mode,
            tick_seconds=sc.tick_seconds,
            wrap_up_seconds=sc.wrap_up_seconds,
        )
        config = config.with_safety_mode(sc.safety_mode)

        profiles = [
            ProviderProfile.provider_a(
                answer_rate=sc.answer_rate,
                talk_seconds_mean=sc.talk_seconds,
                ring_seconds=sc.ring_seconds,
            )
        ]
        if sc.use_provider_b:
            b = ProviderProfile.provider_b(
                answer_rate=sc.answer_rate,
                talk_seconds_mean=sc.talk_seconds,
                ring_seconds=sc.ring_seconds,
            )
            if sc.provider_b_share_of_traffic:
                profiles.insert(0, b)   # B becomes primary: hostile conditions
            else:
                profiles.append(b)      # B is failover only

        app = SmartDialerApp.build(
            config, clock=clock, db=Database.temporary(), profiles=profiles,
            metrics=metrics, seed=sc.seed,
        )
        await app.seed_campaign(agents=sc.agents, borrowers=sc.borrowers)
        await app.start()

        sampler = asyncio.create_task(self._sample_utilisation(app, clock))
        injector = asyncio.create_task(self._run_injections(app, clock))

        await clock.sleep(sc.duration_seconds)

        sampler.cancel()
        injector.cancel()
        await asyncio.gather(sampler, injector, return_exceptions=True)
        await app.stop()

        result = self._build_result(app, metrics)
        app.close()
        return result

    # ------------------------------------------------------------- sampling
    async def _sample_utilisation(self, app: SmartDialerApp, clock) -> None:
        interval = self.scenario.tick_seconds
        while True:
            counts = await app.agents.counts_by_state(app.config.campaign_id)
            online = sum(
                counts[s] for s in AgentState if s is not AgentState.OFFLINE
            )
            connected = counts[AgentState.CONNECTED]
            # "Productive" deliberately excludes RESERVED/DIALING: an agent
            # listening to a ring tone is occupied but not productive, and
            # counting it as busy would flatter progressive dialling.
            occupied = connected + counts[AgentState.WRAP_UP]
            self._utilisation_samples.append((connected, occupied, online))
            await clock.sleep(interval)

    async def _run_injections(self, app: SmartDialerApp, clock) -> None:
        pending = sorted(self.scenario.injections, key=lambda i: i.at_seconds)
        start = clock.now()
        for injection in pending:
            delay = injection.at_seconds - (clock.now() - start)
            if delay > 0:
                await clock.sleep(delay)
            await self._apply(app, injection, clock)

    async def _apply(self, app: SmartDialerApp, injection: Injection, clock) -> None:
        at = clock.now()
        app.metrics.note(at, injection.action, injection.description)
        log.warning(kv("INJECT", action=injection.action,
                       detail=injection.description, **injection.params))
        params = injection.params
        action = injection.action

        if action == "provider_outage":
            for provider in app.providers:
                if provider.name == params.get("provider", provider.name):
                    provider.set_outage(True)
        elif action == "provider_recover":
            for provider in app.providers:
                if provider.name == params.get("provider", provider.name):
                    provider.set_outage(False)
        elif action == "agents_offline":
            await app.agents.force_offline(
                app.config.campaign_id, int(params["count"]),
                only_available=params.get("only_available", True),
            )
        elif action == "agents_online":
            await app.agents.bring_online(app.config.campaign_id, int(params["count"]))
        elif action == "set_answer_rate":
            for provider in app.providers:
                provider.override_profile(answer_rate=float(params["value"]))
        elif action == "set_talk_time":
            for provider in app.providers:
                provider.override_profile(talk_seconds_mean=float(params["value"]))
        elif action == "worker_crash":
            await app.crash()
            await clock.sleep(float(params.get("downtime", 30.0)))
            await app.start(run_recovery=True)
        else:  # pragma: no cover - guarded by scenario definitions
            raise ValueError(f"unknown injection action: {action}")

    # --------------------------------------------------------------- results
    def _build_result(
        self, app: SmartDialerApp, metrics: MetricsCollector
    ) -> SimulationResult:
        samples = self._utilisation_samples or [(0, 0, 1)]
        connected_seconds = sum(s[0] for s in samples) * self.scenario.tick_seconds
        online_seconds = sum(s[2] for s in samples) * self.scenario.tick_seconds
        occupied_seconds = sum(s[1] for s in samples) * self.scenario.tick_seconds
        utilisation = connected_seconds / online_seconds if online_seconds else 0.0
        idle_seconds = (online_seconds - occupied_seconds) / max(1, self.scenario.agents)

        initiated = metrics.counter(M.CALLS_INITIATED)
        answered = metrics.counter(M.CALLS_ANSWERED)

        return SimulationResult(
            scenario=self.scenario.name,
            mode=self.scenario.mode.value,
            safety_mode=self.scenario.safety_mode.value,
            duration_seconds=self.scenario.duration_seconds,
            agents=self.scenario.agents,
            metrics=metrics.snapshot(),
            agent_utilisation=utilisation,
            average_idle_seconds_per_agent=idle_seconds,
            observed_answer_rate=(answered / initiated) if initiated else 0.0,
            timeline=metrics.timeline(),
        )


async def compare(
    scenario: Scenario, modes: list[tuple[DialMode, Any]] | None = None
) -> list[SimulationResult]:
    """Run the same scenario under several dialling strategies."""
    from ..config import SafetyMode

    modes = modes or [
        (DialMode.PROGRESSIVE, SafetyMode.STRICT),
        (DialMode.PREDICTIVE, SafetyMode.STRICT),
        (DialMode.PREDICTIVE, SafetyMode.BALANCED),
    ]
    results = []
    for mode, safety in modes:
        variant = scenario.with_mode(mode, safety)
        results.append(await SimulationRunner(variant).run())
    return results


def render_comparison(results: list[SimulationResult]) -> str:
    """Fixed-width comparison table."""
    rows = [r.summary_row() for r in results]
    columns = [
        "mode", "safety", "initiated", "answered", "connected", "utilisation",
        "unproductive_s/agent", "answer_rate", "abandoned", "approvals", "reductions",
        "rejections", "fallbacks",
    ]
    widths = {
        c: max(len(c), *(len(str(row[c])) for row in rows)) for c in columns
    }
    header = " | ".join(c.ljust(widths[c]) for c in columns)
    sep = "-+-".join("-" * widths[c] for c in columns)
    body = "\n".join(
        " | ".join(str(row[c]).ljust(widths[c]) for c in columns) for row in rows
    )
    return f"{header}\n{sep}\n{body}"
