"""Borrower queue.

Selection policy (deterministic, and easy to extend):

    eligible = state == PENDING
               AND not_before <= now          (retry cooldown elapsed)
               AND attempts   <  max_attempts (attempt budget remains)
    order by priority DESC, not_before ASC, created_at ASC, id ASC

The ordering is total (``id`` breaks ties), so two runs over the same data pick
the same borrower -- which is what makes simulations reproducible.

Reservation uses the same compare-and-swap technique as agents: the UPDATE is
guarded on ``state='PENDING'``, so two workers racing for the last eligible
borrower produce exactly one winner.
"""

from __future__ import annotations

import sqlite3
import uuid

from ..clock import Clock
from ..models.domain import Borrower, Reservation
from ..models.enums import BorrowerState
from ..persistence.db import Database

_COLUMNS = (
    "id, campaign_id, phone, state, priority, attempts, max_attempts, "
    "not_before, reservation_id, reserved_at, last_outcome, created_at"
)

_ELIGIBLE_PREDICATE = (
    "campaign_id=? AND state=? AND not_before<=? AND attempts<max_attempts"
)
_ORDER = "priority DESC, not_before ASC, created_at ASC, id ASC"


def _row_to_borrower(row: sqlite3.Row) -> Borrower:
    return Borrower(
        id=row["id"],
        campaign_id=row["campaign_id"],
        phone=row["phone"],
        state=BorrowerState(row["state"]),
        priority=row["priority"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        not_before=row["not_before"],
        reservation_id=row["reservation_id"],
        reserved_at=row["reserved_at"],
        last_outcome=row["last_outcome"],
        created_at=row["created_at"],
    )


class BorrowerRepository:
    def __init__(self, db: Database, clock: Clock) -> None:
        self._db = db
        self._clock = clock

    async def bulk_create(
        self,
        campaign_id: str,
        count: int,
        *,
        max_attempts: int = 3,
        prefix: str = "B",
        priority_cycle: int = 3,
    ) -> list[str]:
        """Create synthetic borrower records. No real personal data is used."""
        now = self._clock.now()
        ids = [f"{prefix}{i:06d}" for i in range(count)]
        rows = [
            (
                bid,
                campaign_id,
                f"+1555{i:07d}",
                BorrowerState.PENDING.value,
                i % priority_cycle,
                max_attempts,
                0.0,
                now + i * 1e-6,  # stable, distinct creation order
            )
            for i, bid in enumerate(ids)
        ]

        def _op(conn: sqlite3.Connection) -> None:
            conn.executemany(
                "INSERT OR REPLACE INTO borrowers (id, campaign_id, phone, state, "
                "priority, max_attempts, not_before, created_at) VALUES (?,?,?,?,?,?,?,?)",
                rows,
            )

        await self._db.run_in_transaction(_op)
        return ids

    # ------------------------------------------------------------ reservation
    async def reserve_next(
        self, campaign_id: str, reservation_id: str | None = None
    ) -> tuple[Reservation, Borrower | None]:
        """Atomically reserve the highest-priority eligible borrower."""
        reservation_id = reservation_id or f"RB{uuid.uuid4().hex[:12]}"
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> tuple[Reservation, Borrower | None]:
            row = conn.execute(
                f"SELECT {_COLUMNS} FROM borrowers WHERE {_ELIGIBLE_PREDICATE} "
                f"ORDER BY {_ORDER} LIMIT 1",
                (campaign_id, BorrowerState.PENDING.value, now),
            ).fetchone()
            if row is None:
                return Reservation(False, None, None, "no_eligible_borrower"), None
            cur = conn.execute(
                "UPDATE borrowers SET state=?, reservation_id=?, reserved_at=? "
                "WHERE id=? AND state=?",
                (
                    BorrowerState.RESERVED.value,
                    reservation_id,
                    now,
                    row["id"],
                    BorrowerState.PENDING.value,
                ),
            )
            if cur.rowcount != 1:
                return Reservation(False, None, None, "lost_race"), None
            return (
                Reservation(True, row["id"], reservation_id, "reserved"),
                _row_to_borrower(row),
            )

        return await self._db.run_in_transaction(_op)

    async def reserve_borrower(
        self, borrower_id: str, reservation_id: str
    ) -> Reservation:
        """Reserve one specific borrower (used by concurrency tests)."""
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> Reservation:
            cur = conn.execute(
                "UPDATE borrowers SET state=?, reservation_id=?, reserved_at=? "
                "WHERE id=? AND state=?",
                (
                    BorrowerState.RESERVED.value,
                    reservation_id,
                    now,
                    borrower_id,
                    BorrowerState.PENDING.value,
                ),
            )
            if cur.rowcount == 1:
                return Reservation(True, borrower_id, reservation_id, "reserved")
            row = conn.execute(
                "SELECT state FROM borrowers WHERE id=?", (borrower_id,)
            ).fetchone()
            reason = "borrower_not_found" if row is None else f"borrower_is_{row['state']}"
            return Reservation(False, borrower_id, None, reason)

        return await self._db.run_in_transaction(_op)

    # --------------------------------------------------------------- outcomes
    async def settle(
        self,
        borrower_id: str,
        *,
        answered: bool,
        outcome: str,
        cooldown_seconds: float,
        count_attempt: bool = True,
        permanent_failure: bool = False,
    ) -> BorrowerState:
        """Release a borrower after a call ends and decide their next state.

        * answered            -> CONTACTED (campaign objective met for now)
        * permanent failure   -> SUPPRESSED (invalid number / DNC: never retry)
        * attempts exhausted  -> EXHAUSTED
        * otherwise           -> PENDING with a cooldown before the next attempt
        """
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> BorrowerState:
            row = conn.execute(
                "SELECT attempts, max_attempts FROM borrowers WHERE id=?", (borrower_id,)
            ).fetchone()
            if row is None:
                return BorrowerState.SUPPRESSED
            attempts = row["attempts"] + (1 if count_attempt else 0)
            if answered:
                state = BorrowerState.CONTACTED
            elif permanent_failure:
                state = BorrowerState.SUPPRESSED
            elif attempts >= row["max_attempts"]:
                state = BorrowerState.EXHAUSTED
            else:
                state = BorrowerState.PENDING
            conn.execute(
                "UPDATE borrowers SET state=?, attempts=?, not_before=?, "
                "reservation_id=NULL, reserved_at=NULL, last_outcome=? WHERE id=?",
                (state.value, attempts, now + cooldown_seconds, outcome, borrower_id),
            )
            return state

        return await self._db.run_in_transaction(_op)

    async def release_reservation(self, borrower_id: str, reason: str) -> bool:
        """Put a reserved borrower straight back in the queue (no attempt used).

        Used when the call was never actually placed (e.g. circuit open), so it
        would be wrong to burn one of the borrower's attempts.
        """

        def _op(conn: sqlite3.Connection) -> bool:
            cur = conn.execute(
                "UPDATE borrowers SET state=?, reservation_id=NULL, reserved_at=NULL, "
                "last_outcome=? WHERE id=? AND state=?",
                (
                    BorrowerState.PENDING.value,
                    reason,
                    borrower_id,
                    BorrowerState.RESERVED.value,
                ),
            )
            return cur.rowcount == 1

        return await self._db.run_in_transaction(_op)

    # --------------------------------------------------------------- queries
    async def get(self, borrower_id: str) -> Borrower | None:
        row = await self._db.fetch_one(
            f"SELECT {_COLUMNS} FROM borrowers WHERE id=?", (borrower_id,)
        )
        return _row_to_borrower(row) if row else None

    async def count_eligible(self, campaign_id: str) -> int:
        row = await self._db.fetch_one(
            f"SELECT COUNT(*) AS n FROM borrowers WHERE {_ELIGIBLE_PREDICATE}",
            (campaign_id, BorrowerState.PENDING.value, self._clock.now()),
        )
        return int(row["n"]) if row else 0

    async def counts_by_state(self, campaign_id: str) -> dict[BorrowerState, int]:
        rows = await self._db.fetch_all(
            "SELECT state, COUNT(*) AS n FROM borrowers WHERE campaign_id=? GROUP BY state",
            (campaign_id,),
        )
        counts = {state: 0 for state in BorrowerState}
        for row in rows:
            counts[BorrowerState(row["state"])] = row["n"]
        return counts

    async def find_stale_reservations(self, ttl_seconds: float) -> list[Borrower]:
        cutoff = self._clock.now() - ttl_seconds
        rows = await self._db.fetch_all(
            f"SELECT {_COLUMNS} FROM borrowers WHERE state=? AND reserved_at IS NOT NULL "
            "AND reserved_at < ?",
            (BorrowerState.RESERVED.value, cutoff),
        )
        return [_row_to_borrower(r) for r in rows]
