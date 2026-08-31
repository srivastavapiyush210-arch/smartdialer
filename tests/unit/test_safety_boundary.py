"""The safety boundary, asserted mechanically.

"The predictive engine cannot bypass the Safety Controller" is easy to write in
a README and hard to keep true through six months of feature work. These tests
turn it into something CI can fail on. Three independent mechanisms, three
tests -- defeating one of them still leaves the other two standing.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from smartdialer.config import SafetyConfig, SafetyMode
from smartdialer.metrics.collector import MetricsCollector
from smartdialer.models.domain import SafetyDecision
from smartdialer.models.enums import SafetyAction
from smartdialer.safety.controller import ISSUER_TOKEN, SafetyController

from tests.factories import pacing_request, snapshot

SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "smartdialer"


# --------------------------------------------- mechanism 1: unforgeable object
def test_forging_a_safety_decision_raises():
    """Anyone can *type* SafetyDecision(...). Nobody outside the controller can
    construct one, because the constructor demands a module-private token."""
    with pytest.raises(PermissionError):
        SafetyDecision(
            action=SafetyAction.APPROVE,
            approved_calls=10_000,
            requested_calls=10_000,
            limiting_constraint="none",
            caps={},
            snapshot=snapshot(),
            issuer_token=object(),   # a plausible-looking impostor
        )


def test_forging_with_no_token_at_all_raises():
    with pytest.raises((PermissionError, TypeError)):
        SafetyDecision(
            action=SafetyAction.APPROVE,
            approved_calls=10_000,
            requested_calls=10_000,
            limiting_constraint="none",
            caps={},
            snapshot=snapshot(),
            issuer_token=None,
        )


def test_the_controller_can_issue_one():
    """The positive case, so the test above is proving scarcity and not just
    that the constructor is broken."""
    ctrl = SafetyController(SafetyConfig(mode=SafetyMode.STRICT), MetricsCollector())
    decision = ctrl.evaluate(pacing_request(5), snapshot(available_agents=5))
    assert decision.issuer_token is ISSUER_TOKEN
    assert decision.approved_calls == 5


async def test_allocator_refuses_a_decision_it_did_not_receive_from_the_controller(
    tmp_path,
):
    """Belt and braces: even if a decision object were somehow obtained, the
    allocator re-checks the token before performing any I/O."""
    from smartdialer.allocation.allocator import CallAllocator

    class _Impostor:
        action = SafetyAction.APPROVE
        approved_calls = 500
        issuer_token = object()

    allocator = CallAllocator.__new__(CallAllocator)  # no wiring needed
    with pytest.raises(PermissionError):
        await allocator.execute(_Impostor())


# ------------------------------------- mechanism 2: no code path to a telephone
def _imported_modules(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text())
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            # Relative imports: ".." + "providers.base" etc.
            found.add("." * (node.level or 0) + (node.module or ""))
    return found


@pytest.mark.parametrize(
    "module", sorted(p.name for p in (SRC / "pacing").glob("*.py"))
)
def test_pacing_package_cannot_reach_a_provider_or_allocator(module):
    """The pacing engine is *architecturally* unable to place a call.

    Not "does not"; *cannot*. It imports no provider and no allocator, so there
    is no expression anywhere in the package that ends in a phone ringing. This
    scan is what stops a well-meaning future change from adding one.
    """
    imports = _imported_modules(SRC / "pacing" / module)
    forbidden = ("provider", "allocat", "telecom")
    offenders = [i for i in imports if any(word in i.lower() for word in forbidden)]
    assert offenders == [], f"pacing/{module} imports {offenders}"


def test_pacing_engine_has_no_io_handles():
    """A second angle on the same property: no database, no HTTP, no sockets."""
    imports = set()
    for path in (SRC / "pacing").glob("*.py"):
        imports |= _imported_modules(path)
    for banned in ("sqlite3", "requests", "httpx", "socket", "aiohttp"):
        assert not any(banned in i for i in imports), banned


def test_only_the_allocator_talks_to_providers():
    """Exactly one module is allowed to originate calls.

    Anything else importing a provider would be a second door into the carrier
    that the Safety Controller does not guard. Recovery and the router are the
    listed exceptions: recovery *asks* about existing calls rather than placing
    new ones, and the router is the provider abstraction itself.
    """
    allowed = {
        "allocation/allocator.py",
        "reliability/resilient_provider.py",
        "reliability/recovery.py",
        "app.py",
        "simulation/runner.py",
    }
    offenders = []
    for path in SRC.rglob("*.py"):
        rel = path.relative_to(SRC).as_posix()
        if rel in allowed or rel.startswith("providers/"):
            continue
        imports = _imported_modules(path)
        if any("providers" in i for i in imports):
            offenders.append(rel)
    assert offenders == [], f"unexpected provider importers: {offenders}"


# ------------------------------------ mechanism 3: independent recomputation
def test_controller_recomputes_rather_than_trusting_the_request():
    """The controller's approval must not be a function of what it was asked for.

    We ask for one call and for a thousand under identical conditions; the cap
    it computes is the same either way, so an inflated request cannot inflate
    an approval.
    """
    ctrl = SafetyController(SafetyConfig(mode=SafetyMode.BALANCED), MetricsCollector())
    snap = snapshot(available_agents=12)
    cap = ctrl.max_unbound_in_flight(snap)
    for requested in (1, 10, 1_000, 10_000):
        decision = ctrl.evaluate(pacing_request(requested), snap)
        assert decision.approved_calls <= cap
        assert ctrl.max_unbound_in_flight(snap) == cap
