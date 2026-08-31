# Architecture

## The flow

```
Campaign
   |
   v
Snapshot ......... one immutable read of the world, shared by the next two
   |
   v
Pacing Engine .... "I would like 34 calls"        (probabilistic, untrusted)
   |
   v
=========== SAFETY CONTROLLER =========== NON-BYPASSABLE BOUNDARY ===========
   |              "you may have 10"                (deterministic, trusted)
   v
Call Allocator ... the only component that talks to a carrier
   |
   v
Telecom Provider
   |
   v  events
Call Event Processor --> agents, borrowers, calls, estimator
```

The pacing engine produces a *request*. It carries no authority. The controller
issues a `SafetyDecision`, and the allocator accepts nothing else. There is no
branch anywhere in which the pacing engine's number reaches a telephone. See
`safety-model.md` for the three mechanisms that enforce this and the tests that
assert them.

## Components

| module | responsibility |
|---|---|
| `dialer/orchestrator.py` | the control loop: refresh health, snapshot, enforce the continuous invariant, dial |
| `dialer/snapshot.py` | one consistent read that pacing and safety both use |
| `pacing/estimator.py` | answer-rate estimate: Beta-smoothed history blended with a sliding window |
| `pacing/engine.py` | how many calls to ask for. Pure: no DB, no provider, no allocator |
| `safety/controller.py` | how many calls are permitted. Recomputes everything itself |
| `allocation/allocator.py` | places calls, bridges answered ones, cancels excess |
| `events/processor.py` | applies carrier events and their side effects |
| `reliability/` | circuit breaker, classified retries, provider routing, reconciliation |
| `repositories/` | all SQL. Compare-and-swap reservations |
| `persistence/` | SQLite with WAL, `BEGIN IMMEDIATE`, thread-local connections |

## Progressive vs predictive

**Progressive** reserves an agent *before* the call exists. A call cannot be
created without first winning an agent, so agent-bound calls in flight never
exceed agents reserved for dialling, and nobody can answer into an empty seat.

**Predictive** places calls with no agent held. An agent is grabbed at the
instant the borrower answers. This is where the utilisation gain comes from and
where every abandonment risk in the system lives.

`ANSWERED` means the borrower picked up but is not yet bridged. `CONNECTED`
means bridged to an agent. Abandonment is a call that reached ANSWERED and found
no agent within `abandon_grace_seconds`.

## Concurrency

Everything durable is in SQLite, which is the single source of truth; there is
no cache to fall out of sync with it. Concurrency is real rather than simulated:
thread-local connections to one WAL file, `BEGIN IMMEDIATE` for writes, a
10-second busy timeout, and every call dispatched through `asyncio.to_thread`.

Reservation is a conditional UPDATE:

```sql
UPDATE agents SET state='RESERVED', ...
 WHERE id = ? AND state = 'AVAILABLE'
```

Exactly one of N concurrent callers sees `rowcount == 1`. The losers are told
which state they lost to, which is what makes a production log line useful. The
same technique claims borrowers.

Two invariants are enforced by the database rather than by application code
being careful, because application code can be bypassed and a partial unique
index cannot:

```sql
uq_active_call_per_borrower   -- one live call per borrower
uq_active_call_per_agent      -- one live call per agent
```

## Event handling

Layered acceptance policy, in order: duplicate by `event_id` against a ledger;
stale by the provider's per-call sequence number; terminal-state protection;
no-op if the target equals the current state; then the legal-transition graph.

Sequence numbers rather than "is this state later?" because call states are not
totally ordered — COMPLETED and FAILED are siblings, so the question has no
answer.

The state change and the ledger write share one transaction, which guarantees
**at most once** side effects. Cross-aggregate follow-ups — releasing an agent,
settling a borrower — happen immediately afterwards outside that transaction,
are individually idempotent, and are repaired by the reconciler. The ledger
guarantees at-most-once; the reconciler guarantees eventually.

Accepted events run as tracked, shielded tasks so that cancelling the carrier's
delivery task cannot leave a committed state change with unfinished side
effects. Cost measured in `results.md`.

## Recovery

The reconciler does not blindly reset stale state to AVAILABLE. For each stale
lease it asks the provider:

- **live** → renew the lease and leave it alone;
- **terminal** → finish locally and release the agent;
- **unreachable** → retry on the next sweep;
- **no `provider_call_id`** → the call never left the process, so fail it and
  return the borrower *without* burning a contact attempt.

Additional sweeps cover agents left busy against a dead call, answered calls
nobody bridged, stale borrower reservations, and live calls whose agent has gone
offline.

## Time

Every component takes time from an injected `Clock`. `ScaledClock(scale=0.01)`
makes one simulated second cost 10 real milliseconds, so a 90-second call
finishes in 0.9s while all configuration stays written in readable seconds.
`ManualClock` drives tests deterministically. Note the caveat in `results.md`:
`ScaledClock` reads wall-clock time, so seeds do not pin interleaving and
simulation runs are not bit-reproducible.

## Scaling

Measured limits are in `results.md`. The first thing to break is borrower
selection under contention, not the write lock in general.

- **100 agents** — comfortable everywhere.
- **1,000** — borrower selection already 3.4× slower; everything else flat.
- **10,000** — borrower selection at 74/s with lock refusals. Single-process
  SQLite is past its limit for the borrower queue specifically.
- **100,000** — out of scope for this design. Needs a real queue with
  partitioned claiming, the campaign sharded across dialer processes, and the
  Safety Controller made per-shard with a global concurrency ceiling above it.

The Safety Controller itself is not a bottleneck at any of these scales: it is
pure arithmetic at ~28k decisions/s and holds flat across the sweep.
