#!/usr/bin/env python3
"""Load test.

Measures the operations the dialer actually performs under contention, at
100 / 1,000 / 10,000 agents, and reports where the time goes. The point is to
find the real bottleneck in *this* implementation, not to produce a large
number.

    python3 scripts/run_load_test.py                    # full sweep
    python3 scripts/run_load_test.py --scale 1000
    python3 scripts/run_load_test.py --shield-comparison
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import sqlite3
import statistics
import sys
import time
import tracemalloc
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smartdialer.clock import RealClock  # noqa: E402
from smartdialer.config import DialerConfig, DialMode  # noqa: E402
from smartdialer.metrics.collector import MetricsCollector  # noqa: E402
from smartdialer.models.domain import ProviderEvent, SystemSnapshot  # noqa: E402
from smartdialer.models.enums import AgentState, ProviderEventType  # noqa: E402
from smartdialer.pacing.engine import PacingEngine  # noqa: E402
from smartdialer.pacing.estimator import AnswerRateEstimator  # noqa: E402
from smartdialer.persistence.db import Database  # noqa: E402
from smartdialer.repositories.agents import AgentRepository  # noqa: E402
from smartdialer.repositories.borrowers import BorrowerRepository  # noqa: E402
from smartdialer.repositories.calls import CallRepository  # noqa: E402
from smartdialer.safety.controller import SafetyController  # noqa: E402

CAMPAIGN = "LOAD"


class Timer:
    """Times operations and counts the ones the store refused.

    Bounded by a semaphore because that is how the dialer actually issues work
    (allocator waves of ``max_parallel_dials``). Firing 10,000 concurrent
    BEGIN IMMEDIATE transactions at one SQLite file is not a load test of the
    dialer, it is a load test of the busy timeout -- and it loses.
    """

    def __init__(self, concurrency: int = 32) -> None:
        self.samples: list[float] = []
        self.locked = 0
        self.gate = asyncio.Semaphore(concurrency)

    async def run(self, factory):
        async with self.gate:
            started = time.perf_counter()
            try:
                result = await factory()
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) and "busy" not in str(exc):
                    raise
                self.locked += 1
                return None
            self.samples.append((time.perf_counter() - started) * 1000)
            return result

    def report(self) -> dict[str, float]:
        if not self.samples:
            return {"n": 0, "locked": self.locked}
        ordered = sorted(self.samples)
        return {
            "n": len(ordered),
            "mean_ms": round(statistics.fmean(ordered), 3),
            "p50_ms": round(ordered[len(ordered) // 2], 3),
            "p95_ms": round(ordered[int(len(ordered) * 0.95)], 3),
            "p99_ms": round(ordered[int(len(ordered) * 0.99)], 3),
            "max_ms": round(ordered[-1], 3),
            "locked": self.locked,
        }


def _row(label: str, ops: int, seconds: float, extra: str = "") -> str:
    rate = ops / seconds if seconds else 0.0
    return f"  {label:<34} {ops:>7} ops  {seconds:>7.2f}s  {rate:>9.0f}/s  {extra}"


async def _seed(tmp: Path, agents: int, borrowers: int):
    db = Database(str(tmp / "load.sqlite3"))
    db.initialise()
    clock = RealClock()
    agent_repo = AgentRepository(db, clock)
    borrower_repo = BorrowerRepository(db, clock)
    call_repo = CallRepository(db, clock)
    with db.transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO campaigns (id, name, max_concurrent_calls, active) "
            "VALUES (?,?,?,?)",
            (CAMPAIGN, "load", 1_000_000, 1),
        )
    await agent_repo.bulk_create(CAMPAIGN, agents)
    await borrower_repo.bulk_create(CAMPAIGN, borrowers, max_attempts=3)
    return db, agent_repo, borrower_repo, call_repo


async def scale_test(scale: int, *, workers: int = 32) -> dict:
    tmp = Path(f"/tmp/loadtest-{scale}")
    tmp.mkdir(exist_ok=True)
    for stale in tmp.glob("load.sqlite3*"):
        stale.unlink()
    db, agents, borrowers, calls = await _seed(tmp, scale, scale * 4)
    print(f"\n=== {scale} agents, {scale * 4} borrowers, {workers} workers ===")
    results: dict = {"scale": scale}

    # 1. Agent reservation under contention -------------------------------
    reserve_timer = Timer(workers)
    started = time.perf_counter()
    reservations = await asyncio.gather(
        *(reserve_timer.run(lambda: agents.reserve_any_available(CAMPAIGN))
          for _ in range(scale))
    )
    elapsed = time.perf_counter() - started
    won = sum(1 for r in reservations if r.ok)
    lost = sum(1 for r in reservations if not r.ok and r.reason == "lost_race")
    print(_row("agent reservation (CAS)", scale, elapsed,
               f"won={won} lost_race={lost}"))
    print(f"       latency {reserve_timer.report()}")
    results["agent_reservation"] = reserve_timer.report()
    results["agent_reservation"]["ops_per_s"] = round(scale / elapsed)
    results["lost_race_ratio"] = round(lost / max(1, scale), 4)

    # 2. Borrower selection ------------------------------------------------
    borrower_timer = Timer(workers)
    started = time.perf_counter()
    picked = await asyncio.gather(
        *(borrower_timer.run(lambda: borrowers.reserve_next(CAMPAIGN)) for _ in range(scale))
    )
    elapsed = time.perf_counter() - started
    picked = [p for p in picked if p is not None]
    got = sum(1 for reservation, b in picked if reservation.ok and b)
    print(_row("borrower selection (indexed queue)", scale, elapsed, f"got={got}"))
    print(f"       latency {borrower_timer.report()}")
    results["borrower_selection"] = borrower_timer.report()
    results["borrower_selection"]["ops_per_s"] = round(scale / elapsed)

    # 3. Call creation -----------------------------------------------------
    winners = [r for r in reservations if r is not None and r.ok]
    available_borrowers = [b for reservation, b in picked if reservation.ok and b]
    pairs = [
        (agent.entity_id, borrower.id)
        for agent, borrower in zip(winners, available_borrowers)
    ]
    create_timer = Timer(workers)
    started = time.perf_counter()
    await asyncio.gather(*(
        create_timer.run(
            lambda i=i, a=agent_id, b=borrower_id: calls.create(
                f"C{i}", CAMPAIGN, b, DialMode.PROGRESSIVE, agent_id=a)
        )
        for i, (agent_id, borrower_id) in enumerate(pairs)
    ))
    elapsed = time.perf_counter() - started
    print(_row("call creation (unique-index guarded)", len(pairs), elapsed))
    print(f"       latency {create_timer.report()}")
    results["call_creation"] = create_timer.report()
    results["call_creation"]["ops_per_s"] = round(len(pairs) / max(1e-9, elapsed))

    # 4. State transitions -------------------------------------------------
    transition_timer = Timer(workers)
    started = time.perf_counter()
    await asyncio.gather(*(
        transition_timer.run(
            lambda i=i, a=agent_id: agents.transition(
                a, AgentState.DIALING, "load",
                expected=AgentState.RESERVED, call_id=f"C{i}")
        )
        for i, (agent_id, _) in enumerate(pairs)
    ))
    elapsed = time.perf_counter() - started
    print(_row("agent state transition", len(pairs), elapsed))
    results["transition"] = transition_timer.report()
    results["transition"]["ops_per_s"] = round(len(pairs) / max(1e-9, elapsed))

    # 5. Snapshot + pacing + safety (the control loop's own cost) ----------
    metrics = MetricsCollector()
    config = DialerConfig(campaign_id=CAMPAIGN)
    engine = PacingEngine(config.pacing, AnswerRateEstimator(config.pacing))
    controller = SafetyController(config.safety, metrics)

    snapshot_timer = Timer(workers)
    started = time.perf_counter()
    for _ in range(20):
        await snapshot_timer.run(lambda: agents.counts_by_state(CAMPAIGN))
        await calls.live_counts(CAMPAIGN)
        await borrowers.count_eligible(CAMPAIGN)
    elapsed = time.perf_counter() - started
    print(_row("snapshot build (3 aggregate queries)", 20, elapsed))
    print(f"       latency {snapshot_timer.report()}")
    results["snapshot"] = snapshot_timer.report()

    snap = SystemSnapshot(
        at=0.0, campaign_id=CAMPAIGN, available_agents=scale // 2,
        reserved_agents=0, dialing_agents=0, connected_agents=scale // 4,
        wrap_up_agents=0, paused_agents=0, offline_agents=0,
        unbound_calls_in_flight=scale // 8, bound_calls_in_flight=0,
        ringing_calls=0, answered_awaiting_agent=0, connected_calls=scale // 4,
        eligible_borrowers=scale * 4, provider_health=1.0, circuit_state="CLOSED",
        answers_observed=1000, abandoned_observed=2, dials_observed=2000,
    )
    started = time.perf_counter()
    for _ in range(10_000):
        controller.evaluate(engine.compute(snap), snap)
    elapsed = time.perf_counter() - started
    print(_row("pacing + safety decision (pure CPU)", 10_000, elapsed))
    results["decision_ops_per_s"] = round(10_000 / elapsed)

    db.close()
    return results


async def event_throughput(*, shielded: bool, events: int = 3_000) -> dict:
    """Event processing throughput with the shield on and off."""
    from smartdialer.app import SmartDialerApp
    from smartdialer.providers.mock import ProviderProfile

    tmp = Path(f"/tmp/loadtest-events-{shielded}")
    tmp.mkdir(exist_ok=True)
    for stale in tmp.glob("*.sqlite3*"):
        stale.unlink()

    import dataclasses
    config = DialerConfig(
        campaign_id=CAMPAIGN, db_path=str(tmp / "ev.sqlite3"),
        mode=DialMode.PROGRESSIVE,
    )
    config = dataclasses.replace(config, shield_event_handling=shielded)
    app = SmartDialerApp.build(
        config, clock=RealClock(),
        profiles=[ProviderProfile.provider_a(answer_rate=0.5)],
    )
    calls_n = events // 3
    await app.seed_campaign(agents=calls_n, borrowers=calls_n * 2)

    call_ids = []
    for i in range(calls_n):
        _, borrower = await app.borrowers.reserve_next(CAMPAIGN)
        call_id = f"C{i}"
        await app.calls.create(call_id, CAMPAIGN, borrower.id, DialMode.PREDICTIVE)
        await app.calls.mark_initiated(call_id, "provider-a", f"P{i}")
        call_ids.append(call_id)

    payload = []
    for call_id in call_ids:
        for seq, kind in enumerate(
            (ProviderEventType.RINGING, ProviderEventType.ANSWERED,
             ProviderEventType.COMPLETED), start=1
        ):
            payload.append(ProviderEvent(
                event_id=f"{call_id}-{seq}", call_id=call_id, type=kind,
                sequence=seq, timestamp=float(seq), provider="provider-a",
                payload={},
            ))

    gc.collect()
    tracemalloc.start()
    before = tracemalloc.get_traced_memory()[0]
    peak_tasks = 0

    started = time.perf_counter()
    pending = []
    for event in payload:
        pending.append(asyncio.create_task(app.events.handle(event)))
        if len(pending) >= 64:
            peak_tasks = max(peak_tasks, len(asyncio.all_tasks()))
            await asyncio.gather(*pending, return_exceptions=True)
            pending = []
    if pending:
        peak_tasks = max(peak_tasks, len(asyncio.all_tasks()))
        await asyncio.gather(*pending, return_exceptions=True)
    apply_elapsed = time.perf_counter() - started

    drain_started = time.perf_counter()
    await app.events.drain()
    drain_elapsed = time.perf_counter() - drain_started

    after, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    from smartdialer.metrics.collector import M
    summary = app.metrics.histogram(M.H_EVENT_LATENCY).summary()
    latency = {k: round(v, 3) for k, v in summary.items()}
    app.close()

    return {
        "shielded": shielded,
        "events": len(payload),
        "apply_s": round(apply_elapsed, 3),
        "events_per_s": round(len(payload) / apply_elapsed),
        "drain_s": round(drain_elapsed, 4),
        "peak_tasks": peak_tasks,
        "mem_growth_kb": round((after - before) / 1024, 1),
        "mem_peak_kb": round(peak / 1024, 1),
        "latency": latency,
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, action="append")
    parser.add_argument("--shield-comparison", action="store_true")
    parser.add_argument("--events", type=int, default=3_000)
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--workers", type=int, default=32,
                        help="concurrent in-flight operations")
    args = parser.parse_args()

    if args.shield_comparison:
        # Alternate the two configurations rather than running all of one and
        # then all of the other: a single pair of runs put the "cost" anywhere
        # between -1% and +15%, which is machine noise, not signal.
        print(f"=== shielded event handling: {args.reps} alternating reps ===")
        collected: dict[bool, list[dict]] = {False: [], True: []}
        for rep in range(args.reps):
            for shielded in (False, True):
                row = await event_throughput(shielded=shielded, events=args.events)
                collected[shielded].append(row)
                print(f"  rep{rep} shield={'on ' if shielded else 'off'} "
                      f"throughput={row['events_per_s']}/s "
                      f"drain={row['drain_s']}s tasks={row['peak_tasks']} "
                      f"mem_growth={row['mem_growth_kb']}KB "
                      f"p99_latency={row['latency'].get('p99')}ms", flush=True)

        def med(shielded: bool, key: str) -> float:
            return statistics.median(r[key] for r in collected[shielded])

        print("\n  metric                    shield=off   shield=on    delta")
        for key, label, unit in (
            ("events_per_s", "throughput", "/s"),
            ("drain_s", "drain time", "s"),
            ("peak_tasks", "peak outstanding tasks", ""),
            ("mem_growth_kb", "memory growth", "KB"),
            ("mem_peak_kb", "peak traced memory", "KB"),
        ):
            off_v, on_v = med(False, key), med(True, key)
            delta = ((on_v - off_v) / off_v * 100) if off_v else 0.0
            print(f"  {label:<25} {off_v:>10.4g}{unit:<3} {on_v:>8.4g}{unit:<3} "
                  f"{delta:>+7.1f}%")

        off_p99 = statistics.median(
            r["latency"].get("p99", 0) for r in collected[False])
        on_p99 = statistics.median(
            r["latency"].get("p99", 0) for r in collected[True])
        print(f"  {'event latency p99':<25} {off_p99:>10.4g}ms  {on_p99:>8.4g}ms")
        spread = [r["events_per_s"] for r in collected[False]]
        print(f"\n  run-to-run spread with shield OFF: "
              f"{min(spread)}-{max(spread)}/s -- interpret small deltas with care")
        return

    for scale in args.scale or (100, 1_000, 10_000):
        await scale_test(scale, workers=args.workers)


if __name__ == "__main__":
    asyncio.run(main())
