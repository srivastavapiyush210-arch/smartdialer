"""SQLite access layer.

Design notes
------------
* **One source of truth.** Every piece of reservation and lifecycle state lives
  in SQLite. In-memory objects (metrics, the answer-rate estimator) are derived
  and may be rebuilt at any time. If memory and the database disagree, the
  database wins -- see docs/architecture.md, "Source of truth".
* **Real concurrency.** Each worker thread gets its own connection to the same
  file, WAL is enabled and ``BEGIN IMMEDIATE`` is used for writes, so SQLite's
  own writer lock arbitrates races. We are not faking concurrency with a single
  guarded connection.
* **Async without blocking the loop.** All SQLite work is dispatched to a thread
  via ``asyncio.to_thread``. The event loop never blocks on disk I/O.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

T = TypeVar("T")

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")


class Database:
    """Thread-safe SQLite handle with an async facade."""

    def __init__(self, path: str, *, busy_timeout_ms: int = 10_000) -> None:
        self.path = path
        self._busy_timeout_ms = busy_timeout_ms
        self._local = threading.local()
        self._connections: list[sqlite3.Connection] = []
        self._connections_lock = threading.Lock()
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- factory
    @classmethod
    def temporary(cls) -> "Database":
        """A throwaway file database (used by tests and simulations)."""
        directory = tempfile.mkdtemp(prefix="smartdialer-")
        return cls(os.path.join(directory, f"{uuid.uuid4().hex}.sqlite3"))

    # ------------------------------------------------------------ connections
    def connection(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=self._busy_timeout_ms / 1000)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
            with self._connections_lock:
                self._connections.append(conn)
        return conn

    def close(self) -> None:
        with self._connections_lock:
            for conn in self._connections:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            self._connections.clear()
        self._local = threading.local()

    # ------------------------------------------------------------ transactions
    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Write transaction.

        ``BEGIN IMMEDIATE`` takes SQLite's write lock at the start rather than
        on first write, which removes the "upgrade deadlock" failure mode where
        two readers both try to become writers. Combined with ``busy_timeout``
        this means concurrent writers queue rather than fail.
        """
        conn = self.connection()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()

    def initialise(self) -> None:
        conn = self.connection()
        conn.executescript(_SCHEMA_PATH.read_text())
        conn.commit()

    # ------------------------------------------------------------------ async
    async def run(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        """Run ``fn(conn)`` on a worker thread (read path, autocommit)."""
        return await asyncio.to_thread(self._run_sync, fn)

    async def run_in_transaction(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        """Run ``fn(conn)`` inside a single ``BEGIN IMMEDIATE`` transaction."""
        return await asyncio.to_thread(self._run_txn_sync, fn)

    def _run_sync(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self.connection())

    def _run_txn_sync(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        with self.transaction() as conn:
            return fn(conn)

    # ------------------------------------------------------------- convenience
    async def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        return await self.run(lambda c: c.execute(sql, params).fetchone())

    async def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        return await self.run(lambda c: c.execute(sql, params).fetchall())

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        def _op(conn: sqlite3.Connection) -> int:
            return conn.execute(sql, params).rowcount

        return await self.run_in_transaction(_op)
