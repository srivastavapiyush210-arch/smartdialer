#!/usr/bin/env python3
"""Run one scenario, or compare progressive vs predictive under identical conditions.

    python scripts/run_simulation.py --list
    python scripts/run_simulation.py --scenario B-medium-answer-rate --compare
    python scripts/run_simulation.py --scenario F-agent-availability-drop -v
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from smartdialer.config import DialMode, SafetyMode  # noqa: E402
from smartdialer.logging_setup import configure  # noqa: E402
from smartdialer.simulation.runner import (  # noqa: E402
    SimulationRunner, compare, render_comparison,
)
from smartdialer.simulation.scenarios import ALL_SCENARIOS  # noqa: E402


async def main() -> int:
    parser = argparse.ArgumentParser(description="SmartDialer simulation")
    parser.add_argument("--scenario", default="B-medium-answer-rate")
    parser.add_argument("--list", action="store_true", help="list scenarios and exit")
    parser.add_argument("--compare", action="store_true",
                        help="run progressive vs predictive(strict) vs predictive(balanced)")
    parser.add_argument("--mode", choices=[m.value.lower() for m in DialMode],
                        default="predictive")
    parser.add_argument("--safety", choices=[s.value.lower() for s in SafetyMode],
                        default="strict")
    parser.add_argument("--duration", type=float, default=None,
                        help="override scenario duration (simulated seconds)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.list:
        for name, scenario in ALL_SCENARIOS.items():
            print(f"{name:<32} agents={scenario.agents:<5} "
                  f"answer_rate={scenario.answer_rate:<5} "
                  f"talk={scenario.talk_seconds:<6.0f} "
                  f"duration={scenario.duration_seconds:.0f}s "
                  f"injections={len(scenario.injections)}")
        return 0

    configure(logging.DEBUG if args.verbose else logging.WARNING)
    scenario = ALL_SCENARIOS.get(args.scenario)
    if scenario is None:
        print(f"unknown scenario: {args.scenario}", file=sys.stderr)
        return 2
    if args.duration:
        from dataclasses import replace
        scenario = replace(scenario, duration_seconds=args.duration)

    if args.compare:
        print(f"\n=== {scenario.name}: identical conditions, three strategies ===")
        print(f"agents={scenario.agents} answer_rate={scenario.answer_rate} "
              f"talk={scenario.talk_seconds}s duration={scenario.duration_seconds}s\n")
        results = await compare(scenario)
        print(render_comparison(results))
        print()
        return 0

    variant = scenario.with_mode(
        DialMode(args.mode.upper()), SafetyMode(args.safety.upper())
    )
    result = await SimulationRunner(variant).run()
    print(f"\n=== {result.scenario} ===")
    print(result.render())
    if result.timeline:
        print("\ntimeline:")
        for at, kind, detail in result.timeline:
            print(f"  t={at:8.1f}s  {kind:<18} {detail}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
