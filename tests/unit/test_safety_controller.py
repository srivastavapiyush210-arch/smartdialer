"""Safety Controller tests.

The controller is the one component where "it usually works" is not a passing
grade, so these tests are written as properties rather than examples wherever
possible: for a wide sweep of inputs, the approved number must never exceed
what the arithmetic permits.
"""

from __future__ import annotations

import math

import pytest

from smartdialer.config import SafetyConfig, SafetyMode
from smartdialer.metrics.collector import MetricsCollector
from smartdialer.models.enums import SafetyAction
from smartdialer.safety.controller import SafetyController, _solve_max_unbound

from tests.factories import estimate, pacing_request, snapshot


def controller(mode: SafetyMode = SafetyMode.STRICT, **cfg) -> SafetyController:
    return SafetyController(SafetyConfig(mode=mode, **cfg), MetricsCollector())


# ------------------------------------------------------------------ arithmetic
def test_strict_capacity_equals_available_agents():
    """p = 1.0 collapses the formula to U <= capacity.

    The provable part of STRICT: if every ringing phone answered at the same
    instant, there would be an agent for each. It is a statement about the
    decision, not about the eventual outcome, which also depends on the agent
    pool not shrinking in the meantime.
    """
    assert _solve_max_unbound(capacity=25, p=1.0, z=2.0) == pytest.approx(25.0)
    assert _solve_max_unbound(capacity=0, p=1.0, z=2.0) == 0.0


def test_solved_bound_satisfies_its_own_inequality():
    """Whatever U we return must satisfy U*p + z*sqrt(U*p(1-p)) <= capacity."""
    for capacity in (1, 5, 25, 100, 500):
        for p in (0.05, 0.2, 0.5, 0.8, 0.95):
            for z in (1.0, 2.0, 3.0):
                u = _solve_max_unbound(capacity, p, z)
                expected = u * p + z * math.sqrt(max(0.0, u * p * (1 - p)))
                assert expected <= capacity + 1e-6, (capacity, p, z, u)


def test_overshoot_grows_with_pool_size():
    """The pooling effect, asserted rather than asserted-in-a-comment.

    A five-agent pool can barely overdial; a five-hundred-agent pool can nearly
    double. This is the real reason predictive dialling pays off at scale, and
    it falls out of the sqrt term rather than being configured.
    """
    small = _solve_max_unbound(5, p=0.5, z=2.0) / 5
    large = _solve_max_unbound(500, p=0.5, z=2.0) / 500
    assert small < large
    assert small == pytest.approx(1.07, abs=0.05)
    assert large == pytest.approx(1.88, abs=0.05)


def test_lower_answer_rate_permits_more_dialling():
    aggressive = _solve_max_unbound(20, p=0.2, z=2.0)
    conservative = _solve_max_unbound(20, p=0.8, z=2.0)
    assert aggressive > conservative


# ----------------------------------------------------------------- decisions
def test_strict_never_approves_more_than_available_agents():
    """The core safety property, swept over the whole small-input space.

    Already being over the cap (agents went offline underneath calls that are
    still ringing) approves nothing; it does not approve a negative number and
    it does not wrap around.
    """
    ctrl = controller(SafetyMode.STRICT)
    for available in range(0, 40):
        for in_flight in range(0, 40):
            snap = snapshot(available_agents=available,
                            unbound_calls_in_flight=in_flight)
            approved = ctrl.evaluate(pacing_request(500), snap).approved_calls
            assert 0 <= approved <= max(0, available - in_flight)


def test_balanced_never_exceeds_its_own_stated_cap():
    ctrl = controller(SafetyMode.BALANCED)
    for available in (0, 1, 5, 20, 100):
        for wrap_up in (0, 5, 20):
            snap = snapshot(available_agents=available, wrap_up_agents=wrap_up)
            decision = ctrl.evaluate(pacing_request(1000), snap)
            assert decision.approved_calls <= ctrl.max_unbound_in_flight(snap) + 1e-9


def test_answered_calls_awaiting_an_agent_consume_capacity():
    """Someone who has already said 'hello' is not a probability any more."""
    ctrl = controller(SafetyMode.STRICT)
    snap = snapshot(available_agents=10, answered_awaiting_agent=10)
    assert ctrl.evaluate(pacing_request(50), snap).approved_calls == 0


def test_zero_capacity_rejects():
    ctrl = controller(SafetyMode.STRICT)
    decision = ctrl.evaluate(pacing_request(20), snapshot(available_agents=0))
    assert decision.action is SafetyAction.REJECT
    assert decision.approved_calls == 0


def test_low_confidence_falls_back_to_progressive():
    """A predictive dialer that cannot predict must stop being predictive."""
    ctrl = controller(SafetyMode.BALANCED, min_estimator_confidence=0.35)
    request = pacing_request(20, estimate=estimate(confidence=0.10))
    decision = ctrl.evaluate(request, snapshot())
    assert decision.action is SafetyAction.FALLBACK_TO_PROGRESSIVE
    assert decision.approved_calls == 0


def test_blown_abandon_budget_stops_predictive_dialling():
    ctrl = controller(SafetyMode.BALANCED, max_abandon_rate=0.03,
                      min_answers_for_abandon_rate=20)
    snap = snapshot(answers_observed=100, abandoned_observed=9)  # 9% >> 3%
    decision = ctrl.evaluate(pacing_request(20), snap)
    assert decision.action is SafetyAction.FALLBACK_TO_PROGRESSIVE


def test_abandon_budget_needs_a_minimum_sample():
    """One abandoned call out of three must not shut the campaign down."""
    ctrl = controller(SafetyMode.BALANCED, min_answers_for_abandon_rate=20)
    snap = snapshot(answers_observed=3, abandoned_observed=1)
    assert ctrl.evaluate(pacing_request(5), snap).action is not \
        SafetyAction.FALLBACK_TO_PROGRESSIVE


def test_dead_provider_is_rejected_not_fallen_back():
    """Progressive dialling would not help if the carrier is unreachable."""
    ctrl = controller(SafetyMode.BALANCED, min_provider_health=0.5)
    decision = ctrl.evaluate(pacing_request(20), snapshot(provider_health=0.1))
    assert decision.action is SafetyAction.REJECT


def test_degraded_provider_reduces_rather_than_stops():
    ctrl = controller(SafetyMode.BALANCED, min_provider_health=0.5)
    healthy = ctrl.evaluate(pacing_request(50), snapshot(provider_health=1.0))
    degraded = ctrl.evaluate(pacing_request(50), snapshot(provider_health=0.6))
    assert degraded.approved_calls < healthy.approved_calls


def test_borrower_supply_is_a_cap():
    ctrl = controller(SafetyMode.BALANCED)
    snap = snapshot(available_agents=50, eligible_borrowers=3)
    decision = ctrl.evaluate(pacing_request(50), snap)
    assert decision.approved_calls <= 3
    assert decision.limiting_constraint == "borrower_supply"


def test_limiting_constraint_names_the_binding_cap():
    """'Why only eight?' must always have a one-word answer."""
    ctrl = controller(SafetyMode.STRICT)
    snap = snapshot(available_agents=8, eligible_borrowers=1000)
    decision = ctrl.evaluate(pacing_request(100), snap)
    assert decision.approved_calls == 8
    assert decision.limiting_constraint == "agent_capacity"


def test_controller_ignores_the_pacing_engines_answer_rate():
    """A broken estimator must not be able to inflate the controller's cap.

    We hand the controller a pacing request claiming a 1% answer rate -- the
    number that would justify dialling a hundred calls per agent -- and check
    the approval is unchanged, because the controller recomputes p from raw
    observed counters instead of trusting the component it is policing.
    """
    ctrl = controller(SafetyMode.BALANCED)
    snap = snapshot(available_agents=10)
    honest = ctrl.evaluate(pacing_request(1000, estimate=estimate()), snap)
    lying = ctrl.evaluate(
        pacing_request(1000, estimate=estimate(point=0.01, planning_rate=0.01)), snap
    )
    assert honest.approved_calls == lying.approved_calls


# ------------------------------------------------- continuous invariant repair
def test_excess_is_detected_when_agents_disappear():
    """Calls that were safe when placed are not safe once agents vanish."""
    ctrl = controller(SafetyMode.STRICT)
    before = snapshot(available_agents=20, unbound_calls_in_flight=20)
    assert ctrl.excess_unbound_calls(before) == 0

    after = snapshot(available_agents=5, unbound_calls_in_flight=20)
    assert ctrl.excess_unbound_calls(after) == 15


def test_excess_is_never_negative():
    ctrl = controller(SafetyMode.STRICT)
    snap = snapshot(available_agents=50, unbound_calls_in_flight=2)
    assert ctrl.excess_unbound_calls(snap) == 0
