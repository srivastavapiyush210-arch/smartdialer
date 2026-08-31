"""Scenario definitions and failure injection.

A scenario is a plain dataclass: everything that varies between runs lives in
one object, so a comparison is "same object, different mode" and nobody can
accuse the numbers of being cherry-picked.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from ..config import DialMode, SafetyMode


@dataclass(frozen=True)
class Injection:
    """A fault (or a change of world conditions) scheduled at a point in time."""

    at_seconds: float
    action: str
    params: dict = field(default_factory=dict)
    description: str = ""


@dataclass(frozen=True)
class Scenario:
    name: str
    agents: int = 50
    borrowers: int = 5000
    duration_seconds: float = 600.0
    mode: DialMode = DialMode.PREDICTIVE
    safety_mode: SafetyMode = SafetyMode.STRICT
    answer_rate: float = 0.30
    talk_seconds: float = 90.0
    ring_seconds: float = 8.0
    wrap_up_seconds: float = 15.0
    tick_seconds: float = 1.0
    use_provider_b: bool = True
    provider_b_share_of_traffic: bool = False  # B is failover only by default
    time_scale: float = 0.01
    seed: int = 7
    injections: tuple[Injection, ...] = ()

    def with_mode(self, mode: DialMode, safety: SafetyMode | None = None) -> "Scenario":
        return replace(
            self, mode=mode, safety_mode=safety or self.safety_mode,
            name=f"{self.name}[{mode.value.lower()}"
                 f"{'' if safety is None else '/' + safety.value.lower()}]",
        )


# --------------------------------------------------------------------- library
SCENARIO_A_LOW_ANSWER = Scenario(
    name="A-low-answer-rate", answer_rate=0.20, talk_seconds=120.0, agents=50,
)
SCENARIO_B_MEDIUM = Scenario(
    name="B-medium-answer-rate", answer_rate=0.50, talk_seconds=90.0, agents=50,
)
SCENARIO_C_HIGH = Scenario(
    name="C-high-answer-rate", answer_rate=0.70, talk_seconds=180.0, agents=50,
    duration_seconds=900.0,
)
SCENARIO_D_SHIFTING = Scenario(
    name="D-shifting-conditions", answer_rate=0.60, talk_seconds=90.0, agents=50,
    duration_seconds=900.0,
    injections=(
        Injection(300, "set_answer_rate", {"value": 0.10},
                  "answer rate collapses 60% -> 10%"),
        Injection(600, "set_answer_rate", {"value": 0.55},
                  "answer rate recovers to 55%"),
    ),
)
SCENARIO_E_PROVIDER_OUTAGE = Scenario(
    name="E-provider-outage", answer_rate=0.45, talk_seconds=90.0, agents=40,
    duration_seconds=600.0,
    injections=(
        Injection(150, "provider_outage", {"provider": "provider-a"},
                  "primary carrier starts timing out"),
        Injection(350, "provider_recover", {"provider": "provider-a"},
                  "primary carrier recovers"),
    ),
)
SCENARIO_F_AGENT_DROP = Scenario(
    name="F-agent-availability-drop", answer_rate=0.55, talk_seconds=90.0,
    agents=100, duration_seconds=600.0,
    injections=(
        Injection(240, "agents_offline", {"count": 40},
                  "40 of 100 agents disappear"),
    ),
)
SCENARIO_G_CRASH = Scenario(
    name="G-worker-crash", answer_rate=0.45, talk_seconds=90.0, agents=30,
    duration_seconds=600.0,
    injections=(
        Injection(200, "worker_crash", {"downtime": 30.0},
                  "worker process dies mid-flight and restarts"),
    ),
)
SCENARIO_H_HOSTILE_PROVIDER = Scenario(
    name="H-hostile-provider", answer_rate=0.45, talk_seconds=90.0, agents=40,
    duration_seconds=600.0, provider_b_share_of_traffic=True,
    injections=(),
)

ALL_SCENARIOS = {
    s.name: s
    for s in (
        SCENARIO_A_LOW_ANSWER,
        SCENARIO_B_MEDIUM,
        SCENARIO_C_HIGH,
        SCENARIO_D_SHIFTING,
        SCENARIO_E_PROVIDER_OUTAGE,
        SCENARIO_F_AGENT_DROP,
        SCENARIO_G_CRASH,
        SCENARIO_H_HOSTILE_PROVIDER,
    )
}
