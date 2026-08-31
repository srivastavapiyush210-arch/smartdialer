"""Agent repository: atomic reservation and guarded state transitions.

The single most important operation in this system is ``reserve_agent``. It is
implemented as a *conditional update* -- a compare-and-swap:

    UPDATE agents SET state='RESERVED', ...
    WHERE id = ? AND state = 'AVAILABLE'

SQLite executes that statement atomically and reports how many rows it changed.
Exactly one of N concurrent callers can see ``rowcount == 1``; every other
caller sees ``0`` and fails safely with a reason. There is no read-then-write
window for a competitor to slip into, which is precisely the bug that

    if agent.available:            # <-- read
        agent.state = RESERVED     # <-- write, too late

would have.
"""

from __future__ import annotations

import sqlite3
import uuid
from typing import Sequence

from ..clock import Clock
from ..models.domain import Agent, Reservation
from ..models.enums import AgentState
from ..state.agent_fsm import IllegalTransition, validate_transition
from ..persistence.db import Database

_COLUMNS = (
    "id, name, campaign_id, state, reservation_id, current_call_id, "
    "reserved_at, state_changed_at, wrap_up_until"
)


def _row_to_agent(row: sqlite3.Row) -> Agent:
    return Agent(
        id=row["id"],
        name=row["name"],
        campaign_id=row["campaign_id"],
        state=AgentState(row["state"]),
        reservation_id=row["reservation_id"],
        current_call_id=row["current_call_id"],
        reserved_at=row["reserved_at"],
        state_changed_at=row["state_changed_at"],
        wrap_up_until=row["wrap_up_until"],
    )


class AgentRepository:
    def __init__(self, db: Database, clock: Clock) -> None:
        self._db = db
        self._clock = clock

    # ------------------------------------------------------------------ setup
    async def bulk_create(
        self, campaign_id: str, count: int, *, state: AgentState = AgentState.AVAILABLE,
        prefix: str = "A",
    ) -> list[str]:
        now = self._clock.now()
        ids = [f"{prefix}{i:05d}" for i in range(count)]
        rows = [(i, f"agent-{i}", campaign_id, state.value, now) for i in ids]

        def _op(conn: sqlite3.Connection) -> None:
            conn.executemany(
                "INSERT OR REPLACE INTO agents "
                "(id, name, campaign_id, state, state_changed_at) VALUES (?,?,?,?,?)",
                rows,
            )

        await self._db.run_in_transaction(_op)
        return ids

    # ------------------------------------------------------------ reservation
    async def reserve_agent(self, agent_id: str, reservation_id: str) -> Reservation:
        """Atomically move one specific agent AVAILABLE -> RESERVED.

        Succeeds for exactly one concurrent caller.
        """
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> Reservation:
            cur = conn.execute(
                "UPDATE agents SET state=?, reservation_id=?, reserved_at=?, "
                "state_changed_at=? WHERE id=? AND state=?",
                (
                    AgentState.RESERVED.value,
                    reservation_id,
                    now,
                    now,
                    agent_id,
                    AgentState.AVAILABLE.value,
                ),
            )
            if cur.rowcount == 1:
                return Reservation(True, agent_id, reservation_id, "reserved")
            row = conn.execute(
                "SELECT state FROM agents WHERE id=?", (agent_id,)
            ).fetchone()
            reason = "agent_not_found" if row is None else f"agent_is_{row['state']}"
            return Reservation(False, agent_id, None, reason)

        return await self._db.run_in_transaction(_op)

    async def reserve_any_available(
        self, campaign_id: str, reservation_id: str | None = None
    ) -> Reservation:
        """Atomically reserve the longest-idle available agent, if any.

        The SELECT and the UPDATE run inside one ``BEGIN IMMEDIATE``
        transaction, and the UPDATE still carries the ``state='AVAILABLE'``
        guard, so the operation is safe even if the isolation level were
        weaker than it is.
        """
        reservation_id = reservation_id or f"R{uuid.uuid4().hex[:12]}"
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> Reservation:
            row = conn.execute(
                "SELECT id FROM agents WHERE campaign_id=? AND state=? "
                "ORDER BY state_changed_at ASC LIMIT 1",
                (campaign_id, AgentState.AVAILABLE.value),
            ).fetchone()
            if row is None:
                return Reservation(False, None, None, "no_available_agent")
            cur = conn.execute(
                "UPDATE agents SET state=?, reservation_id=?, reserved_at=?, "
                "state_changed_at=? WHERE id=? AND state=?",
                (
                    AgentState.RESERVED.value,
                    reservation_id,
                    now,
                    now,
                    row["id"],
                    AgentState.AVAILABLE.value,
                ),
            )
            if cur.rowcount == 1:
                return Reservation(True, row["id"], reservation_id, "reserved")
            return Reservation(False, None, None, "lost_race")

        return await self._db.run_in_transaction(_op)

    async def reserve_many_available(
        self, campaign_id: str, limit: int
    ) -> list[Reservation]:
        """Reserve up to ``limit`` agents, one atomic CAS per agent."""
        results: list[Reservation] = []
        for _ in range(max(0, limit)):
            res = await self.reserve_any_available(campaign_id)
            if not res.ok:
                break
            results.append(res)
        return results

    # ------------------------------------------------------------ transitions
    async def transition(
        self,
        agent_id: str,
        target: AgentState,
        reason: str,
        *,
        expected: AgentState | Sequence[AgentState] | None = None,
        expected_call_id: str | None = None,
        expected_reservation_id: str | None = None,
        call_id: str | None = None,
        clear_call: bool = False,
        wrap_up_until: float | None = None,
    ) -> bool:
        """Move an agent to ``target``, validating against the FSM.

        The UPDATE is guarded on the state we read, so a concurrent transition
        makes this call return ``False`` rather than clobbering the winner.

        Guarding on state alone is not enough for the bridge, though. An agent
        can be released and immediately re-reserved for a *different* call, and
        a slow continuation from the old call would then find the state it
        expected and hijack the agent onto a call that has already ended.
        ``expected_call_id`` / ``expected_reservation_id`` pin the transition to
        the specific piece of work it was issued for.
        """
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> bool:
            row = conn.execute(
                "SELECT state, current_call_id, reservation_id FROM agents WHERE id=?",
                (agent_id,),
            ).fetchone()
            if row is None:
                return False
            current = AgentState(row["state"])
            if expected is not None:
                allowed = (
                    {expected} if isinstance(expected, AgentState) else set(expected)
                )
                if current not in allowed:
                    return False
            if expected_call_id is not None and row["current_call_id"] != expected_call_id:
                return False
            if (
                expected_reservation_id is not None
                and row["reservation_id"] != expected_reservation_id
            ):
                return False
            if current is target:
                return False
            validate_transition(current, target, reason)

            fields = ["state=?", "state_changed_at=?"]
            params: list[object] = [target.value, now]
            if target in (AgentState.AVAILABLE, AgentState.OFFLINE, AgentState.PAUSED):
                fields += ["reservation_id=NULL", "reserved_at=NULL"]
            if clear_call or target in (AgentState.AVAILABLE, AgentState.OFFLINE):
                fields.append("current_call_id=NULL")
            elif call_id is not None:
                fields.append("current_call_id=?")
                params.append(call_id)
            fields.append("wrap_up_until=?")
            params.append(wrap_up_until)
            params += [agent_id, current.value]

            cur = conn.execute(
                f"UPDATE agents SET {', '.join(fields)} WHERE id=? AND state=?",
                tuple(params),
            )
            return cur.rowcount == 1

        try:
            return await self._db.run_in_transaction(_op)
        except IllegalTransition:
            raise

    async def release(
        self, agent_id: str, reason: str, *, expected_call_id: str | None = None
    ) -> bool:
        """Return an agent to AVAILABLE from any live working state."""
        return await self.transition(
            agent_id,
            AgentState.AVAILABLE,
            reason,
            expected=(
                AgentState.RESERVED,
                AgentState.DIALING,
                AgentState.CONNECTED,
                AgentState.WRAP_UP,
            ),
            expected_call_id=expected_call_id,
            clear_call=True,
        )

    # -------------------------------------------------------------- queries
    async def get(self, agent_id: str) -> Agent | None:
        row = await self._db.fetch_one(
            f"SELECT {_COLUMNS} FROM agents WHERE id=?", (agent_id,)
        )
        return _row_to_agent(row) if row else None

    async def counts_by_state(self, campaign_id: str) -> dict[AgentState, int]:
        rows = await self._db.fetch_all(
            "SELECT state, COUNT(*) AS n FROM agents WHERE campaign_id=? GROUP BY state",
            (campaign_id,),
        )
        counts = {state: 0 for state in AgentState}
        for row in rows:
            counts[AgentState(row["state"])] = row["n"]
        return counts

    async def list_by_state(
        self, campaign_id: str, state: AgentState, limit: int = 1000
    ) -> list[Agent]:
        rows = await self._db.fetch_all(
            f"SELECT {_COLUMNS} FROM agents WHERE campaign_id=? AND state=? LIMIT ?",
            (campaign_id, state.value, limit),
        )
        return [_row_to_agent(r) for r in rows]

    # ------------------------------------------------------- fault injection
    async def force_offline(
        self, campaign_id: str, count: int, *, only_available: bool = True
    ) -> list[str]:
        """Simulate agents vanishing (browser closed, network drop).

        Returns the ids that went offline so the caller can clean up any calls
        those agents were holding.
        """
        now = self._clock.now()
        states = (
            [AgentState.AVAILABLE.value]
            if only_available
            else [
                AgentState.AVAILABLE.value,
                AgentState.RESERVED.value,
                AgentState.DIALING.value,
                AgentState.CONNECTED.value,
                AgentState.WRAP_UP.value,
            ]
        )
        placeholders = ",".join("?" for _ in states)

        def _op(conn: sqlite3.Connection) -> list[str]:
            rows = conn.execute(
                f"SELECT id FROM agents WHERE campaign_id=? AND state IN ({placeholders}) "
                "LIMIT ?",
                (campaign_id, *states, count),
            ).fetchall()
            ids = [r["id"] for r in rows]
            for agent_id in ids:
                conn.execute(
                    "UPDATE agents SET state=?, reservation_id=NULL, reserved_at=NULL, "
                    "current_call_id=NULL, state_changed_at=? WHERE id=?",
                    (AgentState.OFFLINE.value, now, agent_id),
                )
            return ids

        return await self._db.run_in_transaction(_op)

    async def bring_online(self, campaign_id: str, count: int) -> int:
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> int:
            rows = conn.execute(
                "SELECT id FROM agents WHERE campaign_id=? AND state=? LIMIT ?",
                (campaign_id, AgentState.OFFLINE.value, count),
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE agents SET state=?, state_changed_at=? WHERE id=? AND state=?",
                    (AgentState.AVAILABLE.value, now, row["id"], AgentState.OFFLINE.value),
                )
            return len(rows)

        return await self._db.run_in_transaction(_op)

    # ---------------------------------------------------------------- leases
    async def find_stale_reservations(self, ttl_seconds: float) -> list[Agent]:
        """Agents holding a reservation older than the lease TTL."""
        cutoff = self._clock.now() - ttl_seconds
        rows = await self._db.fetch_all(
            f"SELECT {_COLUMNS} FROM agents WHERE state IN (?,?) AND reserved_at IS NOT NULL "
            "AND reserved_at < ?",
            (AgentState.RESERVED.value, AgentState.DIALING.value, cutoff),
        )
        return [_row_to_agent(r) for r in rows]

    async def find_stranded_busy(self) -> list[Agent]:
        """Agents in a working state whose call is gone or already terminal.

        A crash between "call is connected" and "agent is connected" leaves an
        agent occupied against nothing. This is the query the reconciler uses
        to find them; WRAP_UP is excluded because a wrapping agent is supposed
        to outlive its call, and RESERVED is handled by the lease sweep.
        """
        rows = await self._db.fetch_all(
            f"SELECT {_COLUMNS} FROM agents WHERE state IN (?,?) AND ("
            "  current_call_id IS NULL"
            "  OR NOT EXISTS ("
            "    SELECT 1 FROM calls c WHERE c.id = agents.current_call_id"
            "      AND c.state NOT IN ('COMPLETED','FAILED','CANCELLED')"
            "  )"
            ")",
            (AgentState.CONNECTED.value, AgentState.DIALING.value),
        )
        return [_row_to_agent(row) for row in rows]

    async def find_expired_wrap_up(self) -> list[Agent]:
        now = self._clock.now()
        rows = await self._db.fetch_all(
            f"SELECT {_COLUMNS} FROM agents WHERE state=? AND wrap_up_until IS NOT NULL "
            "AND wrap_up_until <= ?",
            (AgentState.WRAP_UP.value, now),
        )
        return [_row_to_agent(r) for r in rows]
