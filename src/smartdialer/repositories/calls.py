"""Call repository, including the transactional provider-event application.

``apply_provider_event`` is the idempotency boundary. In a single
``BEGIN IMMEDIATE`` transaction it:

1. checks the ``processed_events`` ledger for the event id;
2. loads the call and asks the pure policy in ``state/call_fsm.py`` what to do;
3. applies the state change (if any) with a guarded UPDATE;
4. writes the ledger row.

Because (1) and (4) are in the same transaction as (3), replaying an event can
never produce a second side effect: either the whole thing committed, or none
of it did.

Side effects that touch *other* aggregates (releasing the agent, settling the
borrower) are performed by the caller immediately afterwards. They are
individually idempotent (guarded updates) and, if a worker dies in between, the
reconciler repairs them from the lease tables. See docs/architecture.md.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from ..clock import Clock
from ..config import DialMode
from ..models.domain import Call, ProviderEvent
from ..models.enums import CallState, EventOutcome, ProviderEventType
from ..persistence.db import Database
from ..state.call_fsm import EventDecision, decide_event

_COLUMNS = (
    "id, campaign_id, borrower_id, agent_id, agent_bound, mode, state, provider, "
    "provider_call_id, reservation_id, last_sequence, attempts, created_at, "
    "updated_at, initiated_at, ringing_at, answered_at, connected_at, ended_at, "
    "abandoned, failure_reason"
)

_TERMINAL_VALUES = tuple(s.value for s in (
    CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED))


class DuplicateActiveCall(Exception):
    """A live call already exists for this borrower or agent (DB invariant)."""


def _row_to_call(row: sqlite3.Row) -> Call:
    return Call(
        id=row["id"],
        campaign_id=row["campaign_id"],
        borrower_id=row["borrower_id"],
        agent_id=row["agent_id"],
        agent_bound=bool(row["agent_bound"]),
        mode=DialMode(row["mode"]),
        state=CallState(row["state"]),
        provider=row["provider"],
        provider_call_id=row["provider_call_id"],
        reservation_id=row["reservation_id"],
        last_sequence=row["last_sequence"],
        attempts=row["attempts"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        initiated_at=row["initiated_at"],
        ringing_at=row["ringing_at"],
        answered_at=row["answered_at"],
        connected_at=row["connected_at"],
        ended_at=row["ended_at"],
        abandoned=bool(row["abandoned"]),
        failure_reason=row["failure_reason"],
    )


@dataclass(frozen=True)
class EventApplication:
    """Everything the caller needs to know after an event was processed."""

    outcome: EventOutcome
    reason: str
    call: Call | None
    previous_state: CallState | None
    new_state: CallState | None
    became_terminal: bool = False
    needs_agent: bool = False       # ANSWERED on an unbound call -> bridge now
    was_answered: bool = False      # for the answer-rate estimator
    late_answer_merged: bool = False


class CallRepository:
    def __init__(self, db: Database, clock: Clock) -> None:
        self._db = db
        self._clock = clock

    # ----------------------------------------------------------------- create
    async def create(
        self,
        call_id: str,
        campaign_id: str,
        borrower_id: str,
        mode: DialMode,
        *,
        agent_id: str | None = None,
        reservation_id: str | None = None,
    ) -> Call:
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> Call:
            try:
                conn.execute(
                    "INSERT INTO calls (id, campaign_id, borrower_id, agent_id, "
                    "agent_bound, mode, state, reservation_id, created_at, updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        call_id,
                        campaign_id,
                        borrower_id,
                        agent_id,
                        1 if agent_id else 0,
                        mode.value,
                        CallState.RESERVED.value,
                        reservation_id,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise DuplicateActiveCall(str(exc)) from exc
            row = conn.execute(
                f"SELECT {_COLUMNS} FROM calls WHERE id=?", (call_id,)
            ).fetchone()
            return _row_to_call(row)

        return await self._db.run_in_transaction(_op)

    async def mark_initiated(
        self, call_id: str, provider: str, provider_call_id: str
    ) -> bool:
        """Record that the carrier accepted the call.

        Split deliberately in two:

        * the provider identifiers and ``initiated_at`` are **facts** and are
          always written -- we need ``provider_call_id`` to cancel or reconcile
          the call later, even if a webhook has already moved the state;
        * the ``RESERVED -> INITIATED`` transition is **conditional** and only
          happens if a provider event has not already advanced the call.

        This is what makes the "webhook beats the API response" race harmless.
        """
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> bool:
            row = conn.execute(
                "SELECT state FROM calls WHERE id=?", (call_id,)
            ).fetchone()
            if row is None:
                return False
            conn.execute(
                "UPDATE calls SET provider=?, provider_call_id=?, initiated_at=?, "
                "updated_at=?, attempts=attempts+1 WHERE id=?",
                (provider, provider_call_id, now, now, call_id),
            )
            cur = conn.execute(
                "UPDATE calls SET state=? WHERE id=? AND state=?",
                (CallState.INITIATED.value, call_id, CallState.RESERVED.value),
            )
            return cur.rowcount == 1

        return await self._db.run_in_transaction(_op)

    async def force_terminal(
        self, call_id: str, state: CallState, reason: str
    ) -> bool:
        """Terminate a call locally (setup failed, cancelled, abandoned).

        Guarded so it can never overwrite an already-terminal call.
        """
        if not state.is_terminal():
            raise ValueError("force_terminal requires a terminal state")
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> bool:
            cur = conn.execute(
                "UPDATE calls SET state=?, ended_at=?, updated_at=?, failure_reason=? "
                f"WHERE id=? AND state NOT IN {_TERMINAL_VALUES}",
                (state.value, now, now, reason, call_id),
            )
            return cur.rowcount == 1

        return await self._db.run_in_transaction(_op)

    async def mark_connected(self, call_id: str, agent_id: str) -> bool:
        """Bridge an ANSWERED call to a freshly reserved agent."""
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> bool:
            cur = conn.execute(
                "UPDATE calls SET state=?, agent_id=?, connected_at=?, updated_at=? "
                "WHERE id=? AND state=?",
                (
                    CallState.CONNECTED.value,
                    agent_id,
                    now,
                    now,
                    call_id,
                    CallState.ANSWERED.value,
                ),
            )
            return cur.rowcount == 1

        return await self._db.run_in_transaction(_op)

    async def mark_abandoned(self, call_id: str, reason: str) -> bool:
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> bool:
            cur = conn.execute(
                "UPDATE calls SET state=?, abandoned=1, ended_at=?, updated_at=?, "
                f"failure_reason=? WHERE id=? AND state NOT IN {_TERMINAL_VALUES}",
                (CallState.COMPLETED.value, now, now, reason, call_id),
            )
            return cur.rowcount == 1

        return await self._db.run_in_transaction(_op)

    # ------------------------------------------------------- event processing
    async def apply_provider_event(self, event: ProviderEvent) -> EventApplication:
        now = self._clock.now()

        def _op(conn: sqlite3.Connection) -> EventApplication:
            seen = conn.execute(
                "SELECT outcome FROM processed_events WHERE event_id=?",
                (event.event_id,),
            ).fetchone()
            if seen is not None:
                return EventApplication(
                    EventOutcome.DUPLICATE,
                    f"event {event.event_id} already processed ({seen['outcome']})",
                    None,
                    None,
                    None,
                )

            row = conn.execute(
                f"SELECT {_COLUMNS} FROM calls WHERE id=?", (event.call_id,)
            ).fetchone()
            if row is None:
                _record(conn, event, EventOutcome.UNKNOWN_CALL, now)
                return EventApplication(
                    EventOutcome.UNKNOWN_CALL, "no such call", None, None, None
                )

            call = _row_to_call(row)
            decision: EventDecision = decide_event(
                call.state, call.last_sequence, event
            )

            late_answer = False
            if not decision.applies:
                # Monotonic fact merge: we still learn that the borrower picked
                # up, even if the state itself must not move. This never
                # allocates an agent and never resurrects a terminal call.
                if (
                    event.type is ProviderEventType.ANSWERED
                    and call.answered_at is None
                ):
                    conn.execute(
                        "UPDATE calls SET answered_at=?, updated_at=? WHERE id=?",
                        (event.timestamp, now, event.call_id),
                    )
                    late_answer = True
                _record(conn, event, decision.outcome, now)
                return EventApplication(
                    decision.outcome,
                    decision.reason,
                    call,
                    call.state,
                    call.state,
                    late_answer_merged=late_answer,
                )

            target = decision.target_state
            assert target is not None
            fields = ["state=?", "updated_at=?", "last_sequence=?"]
            params: list[object] = [target.value, now, event.sequence]
            if target is CallState.RINGING:
                fields.append("ringing_at=?")
                params.append(event.timestamp)
            elif target is CallState.ANSWERED:
                fields.append("answered_at=?")
                params.append(event.timestamp)
            if target.is_terminal():
                fields.append("ended_at=?")
                params.append(event.timestamp)
                fields.append("failure_reason=?")
                params.append(
                    None if target is CallState.COMPLETED else event.type.value
                )
            params += [event.call_id, call.state.value]

            cur = conn.execute(
                f"UPDATE calls SET {', '.join(fields)} WHERE id=? AND state=?",
                tuple(params),
            )
            if cur.rowcount != 1:
                # Someone else moved the call between our read and write.
                _record(conn, event, EventOutcome.STALE, now)
                return EventApplication(
                    EventOutcome.STALE, "lost write race", call, call.state, call.state
                )

            _record(conn, event, EventOutcome.APPLIED, now)
            was_answered = (
                target is CallState.ANSWERED or call.answered_at is not None
            )
            return EventApplication(
                outcome=EventOutcome.APPLIED,
                reason=decision.reason,
                call=call,
                previous_state=call.state,
                new_state=target,
                became_terminal=target.is_terminal(),
                needs_agent=(target is CallState.ANSWERED and call.agent_id is None),
                was_answered=was_answered,
            )

        return await self._db.run_in_transaction(_op)

    # --------------------------------------------------------------- queries
    async def get(self, call_id: str) -> Call | None:
        row = await self._db.fetch_one(
            f"SELECT {_COLUMNS} FROM calls WHERE id=?", (call_id,)
        )
        return _row_to_call(row) if row else None

    async def counts_by_state(self, campaign_id: str) -> dict[CallState, int]:
        rows = await self._db.fetch_all(
            "SELECT state, COUNT(*) AS n FROM calls WHERE campaign_id=? GROUP BY state",
            (campaign_id,),
        )
        counts = {state: 0 for state in CallState}
        for row in rows:
            counts[CallState(row["state"])] = row["n"]
        return counts

    async def live_counts(self, campaign_id: str) -> dict[str, int]:
        """Counts the pacing engine and Safety Controller both read."""
        rows = await self._db.fetch_all(
            "SELECT state, agent_bound, COUNT(*) AS n FROM calls "
            f"WHERE campaign_id=? AND state NOT IN {_TERMINAL_VALUES} "
            "GROUP BY state, agent_bound",
            (campaign_id,),
        )
        out = {
            "unbound_in_flight": 0,
            "bound_in_flight": 0,
            "ringing": 0,
            "answered_awaiting_agent": 0,
            "connected": 0,
        }
        for row in rows:
            state = CallState(row["state"])
            bound = bool(row["agent_bound"])
            n = row["n"]
            if state is CallState.RINGING:
                out["ringing"] += n
            if state is CallState.ANSWERED:
                out["answered_awaiting_agent"] += n
            elif state is CallState.CONNECTED:
                out["connected"] += n
            if state.is_in_flight():
                out["bound_in_flight" if bound else "unbound_in_flight"] += n
        return out

    async def newest_unbound_ringing(self, campaign_id: str, limit: int) -> list[Call]:
        """Youngest ringing unbound calls -- the ones cheapest to cancel."""
        rows = await self._db.fetch_all(
            f"SELECT {_COLUMNS} FROM calls WHERE campaign_id=? AND agent_bound=0 "
            "AND state IN (?,?) ORDER BY created_at DESC LIMIT ?",
            (campaign_id, CallState.RINGING.value, CallState.INITIATED.value, limit),
        )
        return [_row_to_call(r) for r in rows]

    async def find_stuck_in_setup(self, ttl_seconds: float) -> list[Call]:
        """Calls that have not moved for longer than the setup TTL.

        RINGING is included, not just the pre-acknowledgement states. A carrier
        sending an absurd sequence number makes every later event stale, and
        the call then sits in RINGING forever holding its borrower reservation.
        The sweep is keyed on ``updated_at``, so a call that is genuinely still
        progressing is never selected, and the caller asks the provider before
        ending anything it does select.
        """
        cutoff = self._clock.now() - ttl_seconds
        rows = await self._db.fetch_all(
            f"SELECT {_COLUMNS} FROM calls WHERE state IN (?,?,?) AND updated_at < ?",
            (CallState.RESERVED.value, CallState.INITIATED.value,
             CallState.RINGING.value, cutoff),
        )
        return [_row_to_call(r) for r in rows]

    async def find_with_offline_agent(self, campaign_id: str) -> list[Call]:
        """Live calls whose agent has gone offline.

        The borrower is still on the line and the person who was talking to
        them has vanished -- laptop shut, VPN dropped. Nothing else in the
        system notices: the agent is already OFFLINE so the stranded-agent
        sweep skips it, and no provider event is coming because the call is
        fine as far as the carrier is concerned.
        """
        rows = await self._db.fetch_all(
            f"SELECT {_COLUMNS} FROM calls c WHERE c.campaign_id=? "
            f"AND c.state NOT IN {_TERMINAL_VALUES} AND c.agent_id IS NOT NULL "
            "AND EXISTS (SELECT 1 FROM agents a WHERE a.id=c.agent_id "
            "            AND a.state='OFFLINE')",
            (campaign_id,),
        )
        return [_row_to_call(row) for row in rows]

    async def find_orphaned_answered(self, ttl_seconds: float) -> list[Call]:
        cutoff = self._clock.now() - ttl_seconds
        rows = await self._db.fetch_all(
            f"SELECT {_COLUMNS} FROM calls WHERE state=? AND agent_id IS NULL "
            "AND updated_at < ?",
            (CallState.ANSWERED.value, cutoff),
        )
        return [_row_to_call(r) for r in rows]

    async def get_active_for_borrower(self, borrower_id: str) -> Call | None:
        row = await self._db.fetch_one(
            f"SELECT {_COLUMNS} FROM calls WHERE borrower_id=? "
            f"AND state NOT IN {_TERMINAL_VALUES} LIMIT 1",
            (borrower_id,),
        )
        return _row_to_call(row) if row else None

    async def active_call_for_agent(self, agent_id: str) -> Call | None:
        row = await self._db.fetch_one(
            f"SELECT {_COLUMNS} FROM calls WHERE agent_id=? "
            f"AND state NOT IN {_TERMINAL_VALUES} LIMIT 1",
            (agent_id,),
        )
        return _row_to_call(row) if row else None

    async def durations(self, campaign_id: str) -> list[float]:
        rows = await self._db.fetch_all(
            "SELECT connected_at, ended_at FROM calls WHERE campaign_id=? "
            "AND connected_at IS NOT NULL AND ended_at IS NOT NULL",
            (campaign_id,),
        )
        return [r["ended_at"] - r["connected_at"] for r in rows]


def _record(
    conn: sqlite3.Connection, event: ProviderEvent, outcome: EventOutcome, now: float
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO processed_events "
        "(event_id, call_id, event_type, sequence, provider, received_at, outcome) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            event.event_id,
            event.call_id,
            event.type.value,
            event.sequence,
            event.provider,
            now,
            outcome.value,
        ),
    )
