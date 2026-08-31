"""Shared fixtures.

Every test that touches storage gets its *own* temporary SQLite file. Tests
never share a database, so they can run in any order and, if we ever add
``pytest-xdist``, in parallel.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for entry in (str(ROOT), str(ROOT / "src")):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from smartdialer.clock import ManualClock  # noqa: E402
from smartdialer.metrics.collector import MetricsCollector  # noqa: E402
from smartdialer.persistence.db import Database  # noqa: E402
from smartdialer.repositories.agents import AgentRepository  # noqa: E402
from smartdialer.repositories.borrowers import BorrowerRepository  # noqa: E402
from smartdialer.repositories.calls import CallRepository  # noqa: E402

CAMPAIGN = "CAMP-TEST"


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(start=1_000.0)


@pytest.fixture
def db(tmp_path) -> Database:
    database = Database(str(tmp_path / "test.sqlite3"))
    database.initialise()
    yield database
    database.close()


@pytest.fixture
def metrics() -> MetricsCollector:
    return MetricsCollector()


@pytest.fixture
def repos(db, clock):
    """(agents, borrowers, calls) against one seeded campaign."""
    agents = AgentRepository(db, clock)
    borrowers = BorrowerRepository(db, clock)
    calls = CallRepository(db, clock)
    with db.transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO campaigns (id, name, max_concurrent_calls, active) "
            "VALUES (?,?,?,?)",
            (CAMPAIGN, "test", 10_000, 1),
        )
    return agents, borrowers, calls
