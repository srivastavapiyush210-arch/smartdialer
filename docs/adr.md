# Decisions

## SQLite as the single source of truth
Real concurrency (WAL, `BEGIN IMMEDIATE`, thread-local connections, work
dispatched off the event loop) rather than one guarded connection pretending to
be a database. Alternative considered: an in-memory store with a lock. Rejected
because it would make the crash-recovery story fiction — there would be nothing
durable to recover *from* — and crash recovery is a graded requirement. Cost:
one writer at a time, which the load test quantifies.

## Compare-and-swap reservations, not `SELECT ... FOR UPDATE`
SQLite has no row locks. A conditional UPDATE guarded on the expected state is
the portable equivalent and needs no lock manager. It also degrades honestly:
the loser gets a reason string, not an exception.

## Invariants in the schema, not only in code
Partial unique indexes for "one live call per borrower/agent". Application code
can be bypassed by a future change; an index cannot. The allocator catches the
resulting `IntegrityError` and returns both resources.

## Sequence numbers for event ordering
Call states are not totally ordered, so "did this state come later?" is not a
well-formed question. The provider's per-call sequence is.

## One small transaction plus a repair loop
Rather than one transaction spanning agents, borrowers and calls. The ledger
gives at-most-once; the reconciler gives eventually. This is the standard trade
and it keeps write transactions short, which matters given a single writer.

## Identity guards on agent transitions
`expected_call_id` / `expected_reservation_id` alongside the expected state.
Added after a real defect: state alone is not identity, and an agent released
and re-reserved for different work could be hijacked by a slow continuation
from an earlier call.

## Bounded concurrent dial waves
Carrier setup is I/O bound. Dialling serially starved the control loop badly
enough to make predictive look worse than progressive. Waves of
`max_parallel_dials`, with revalidation between waves rather than before every
call.

## Shielded event handling
An accepted event always completes its side effects. Measured cost in
`results.md`; the reasoning for keeping it is there too.

## An interpretable estimator, not a model
Beta-smoothed historical rate blended with a sliding window, with conservatism
running *upward* because a high answer rate is the dangerous side. No ML model:
nothing here needs one, and "the answer rate over the last 25 calls" is a
quantity an operations manager can check by hand. A trained model would also
have to be defended in an interview without the training data to hand.

## Windowed answer-rate bound in the controller
Cumulative counters cannot see a regime change. This was not a theoretical
concern — it produced 23.75% abandonment. The controller takes the higher of
the cumulative and recent upper bounds.

## No FastAPI
There is no consumer for an HTTP API in this assignment. CLI demos are more
reproducible and every endpoint would be untested surface area. The modular
monolith already has the seams an API would sit on.

## No Kafka, Redis, Kubernetes or microservices
The requirement is a working prototype that can be reasoned about in an
interview. Each of those would add operational surface without changing a
single graded behaviour.
