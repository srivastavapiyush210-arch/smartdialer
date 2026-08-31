"""Object factories for tests.

Kept separate from ``conftest.py`` so they can be imported explicitly:
a test that builds a snapshot should *say* it builds a snapshot.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from smartdialer.app import SmartDialerApp  # noqa: E402
from smartdialer.clock import ScaledClock  # noqa: E402
from smartdialer.config import DialerConfig, DialMode, SafetyMode  # noqa: E402
from smartdialer.models.domain import (  # noqa: E402
    AnswerRateEstimate,
    PacingRequest,
    SystemSnapshot,
)
from smartdialer.providers.mock import ProviderProfile  # noqa: E402

CAMPAIGN = "CAMP-TEST"


def snapshot(**overrides) -> SystemSnapshot:
    """A snapshot with sane defaults; override only what the test is about."""
    base = dict(
        at=1_000.0,
        campaign_id=CAMPAIGN,
        available_agents=10,
        reserved_agents=0,
        dialing_agents=0,
        connected_agents=0,
        wrap_up_agents=0,
        paused_agents=0,
        offline_agents=0,
        unbound_calls_in_flight=0,
        bound_calls_in_flight=0,
        ringing_calls=0,
        answered_awaiting_agent=0,
        connected_calls=0,
        eligible_borrowers=1_000,
        provider_health=1.0,
        circuit_state="CLOSED",
        answers_observed=500,
        abandoned_observed=0,
        dials_observed=1_000,
    )
    base.update(overrides)
    return SystemSnapshot(**base)


def estimate(**overrides) -> AnswerRateEstimate:
    base = dict(
        point=0.50,
        planning_rate=0.55,
        stderr=0.02,
        samples=500,
        confidence=0.90,
        recent=0.50,
        historical=0.50,
        volatility=0.05,
    )
    base.update(overrides)
    return AnswerRateEstimate(**base)


def pacing_request(requested: int = 20, **overrides) -> PacingRequest:
    base = dict(
        requested_calls=requested,
        estimate=estimate(),
        capacity_now=10.0,
        soon_free_credit=0.0,
        expected_incoming_answers=0.0,
        headroom=10.0,
        safety_margin=1.0,
        reason="test",
    )
    base.update(overrides)
    return PacingRequest(**base)


def build_app(
    tmp_path,
    *,
    mode: DialMode = DialMode.PROGRESSIVE,
    safety: SafetyMode = SafetyMode.STRICT,
    answer_rate: float = 0.5,
    talk_seconds: float = 30.0,
    ring_seconds: float = 4.0,
    time_scale: float = 0.005,
    profiles=None,
    seed: int = 11,
    **config_overrides,
) -> SmartDialerApp:
    """A fully wired app on a compressed clock, for integration tests."""
    import dataclasses

    config = DialerConfig(
        campaign_id=CAMPAIGN,
        mode=mode,
        tick_seconds=0.5,
        wrap_up_seconds=5.0,
        db_path=str(tmp_path / "app.sqlite3"),
    )
    config = config.with_safety_mode(safety)
    if config_overrides:
        config = dataclasses.replace(config, **config_overrides)

    if profiles is None:
        profiles = [
            ProviderProfile.provider_a(
                answer_rate=answer_rate,
                talk_seconds_mean=talk_seconds,
                ring_seconds=ring_seconds,
            )
        ]
    return SmartDialerApp.build(
        config,
        clock=ScaledClock(scale=time_scale),
        profiles=profiles,
        seed=seed,
    )
