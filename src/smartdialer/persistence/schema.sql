-- SmartDialer prototype schema.
-- SQLite is the single source of truth for all reservation and lifecycle state.
-- Anything held in memory (metrics, estimator) is derived and disposable.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS campaigns (
    id                   TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    max_concurrent_calls INTEGER NOT NULL DEFAULT 1000,
    active               INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS agents (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    campaign_id      TEXT NOT NULL,
    state            TEXT NOT NULL,
    reservation_id   TEXT,
    current_call_id  TEXT,
    reserved_at      REAL,
    state_changed_at REAL NOT NULL DEFAULT 0,
    wrap_up_until    REAL
);
CREATE INDEX IF NOT EXISTS idx_agents_pick ON agents(campaign_id, state, state_changed_at);
CREATE INDEX IF NOT EXISTS idx_agents_lease ON agents(state, reserved_at);

CREATE TABLE IF NOT EXISTS borrowers (
    id             TEXT PRIMARY KEY,
    campaign_id    TEXT NOT NULL,
    phone          TEXT NOT NULL,
    state          TEXT NOT NULL,
    priority       INTEGER NOT NULL DEFAULT 0,
    attempts       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 3,
    not_before     REAL NOT NULL DEFAULT 0,
    reservation_id TEXT,
    reserved_at    REAL,
    last_outcome   TEXT,
    created_at     REAL NOT NULL DEFAULT 0
);
-- Selection policy index: eligible borrowers ordered by priority then age.
CREATE INDEX IF NOT EXISTS idx_borrowers_pick
    ON borrowers(campaign_id, state, not_before, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_borrowers_lease ON borrowers(state, reserved_at);

CREATE TABLE IF NOT EXISTS calls (
    id               TEXT PRIMARY KEY,
    campaign_id      TEXT NOT NULL,
    borrower_id      TEXT NOT NULL,
    agent_id         TEXT,
    agent_bound      INTEGER NOT NULL DEFAULT 0,
    mode             TEXT NOT NULL,
    state            TEXT NOT NULL,
    provider         TEXT,
    provider_call_id TEXT,
    reservation_id   TEXT,
    last_sequence    INTEGER NOT NULL DEFAULT -1,
    attempts         INTEGER NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL DEFAULT 0,
    updated_at       REAL NOT NULL DEFAULT 0,
    initiated_at     REAL,
    ringing_at       REAL,
    answered_at      REAL,
    connected_at     REAL,
    ended_at         REAL,
    abandoned        INTEGER NOT NULL DEFAULT 0,
    failure_reason   TEXT
);
CREATE INDEX IF NOT EXISTS idx_calls_state ON calls(campaign_id, state);
CREATE INDEX IF NOT EXISTS idx_calls_lease ON calls(state, updated_at);
-- Invariant #2 is also enforced by the database, not only by application code:
-- a borrower can have at most one non-terminal call at a time.
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_call_per_borrower
    ON calls(borrower_id)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
-- Invariant #1 corollary: an agent can be bound to at most one live call.
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_call_per_agent
    ON calls(agent_id)
    WHERE agent_id IS NOT NULL
      AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');

-- Idempotency ledger. The PRIMARY KEY is the idempotency key: an event that is
-- already here has already had all of its side effects.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id    TEXT PRIMARY KEY,
    call_id     TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    sequence    INTEGER NOT NULL,
    provider    TEXT NOT NULL,
    received_at REAL NOT NULL,
    outcome     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_call ON processed_events(call_id, sequence);

-- Append-only decision log: every pacing/safety decision, for explainability.
CREATE TABLE IF NOT EXISTS decision_log (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    at        REAL NOT NULL,
    kind      TEXT NOT NULL,
    detail    TEXT NOT NULL
);
