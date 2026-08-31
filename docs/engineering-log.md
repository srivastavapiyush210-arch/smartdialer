# Engineering log

The defects below were found by running the system, not by reading it. Each is
recorded with what it looked like, why it happened and what was done, because
the reasoning is more useful than the patch.

## 1. Carrier events beating our own bookkeeping

Seven `invalid_transition_events` in the first progressive smoke test. The
mock's RINGING event could be delivered before `mark_initiated` had committed,
so the call was still RESERVED and RESERVED → RINGING was illegal.

This is not an artefact of the mock: a real webhook can absolutely arrive
before the HTTP response to the request that caused it. Two changes: RINGING,
ANSWERED and COMPLETED became legal from RESERVED, and `mark_initiated` was
split so that provider identifiers are written unconditionally as *facts* while
the state transition stays conditional. Invalid transitions went to zero.

## 2. Estimator confidence pinned near zero

47 of 77 ticks were falling back to progressive because confidence sat around
0.22. An EMA over Bernoulli outcomes is extremely noisy, and volatility was
being computed as a raw normalised difference.

Replaced with a sliding window of 25 outcomes and volatility as a z-score of
divergence between the recent window and the long-run rate. Random noise now
gives z ≈ 1 and barely moves confidence; a genuine 70% → 10% regime change
drives confidence to zero within one window. A window was chosen over an EMA
because "the answer rate over the last 25 calls" is a quantity an operations
manager can check by hand.

## 3. Predictive losing to progressive — a control-loop bug, not a model flaw

The first honest comparison had predictive *behind* progressive. The cause was
not the pacing model: the allocator dialled strictly one call at a time and,
with per-call revalidation, rebuilt a three-query snapshot before every single
dial. In a 600-second run the dialer managed 46 ticks instead of ~600. Agents
sat idle waiting for a decision nobody was taking. Progressive did less work
per tick so it suffered less, which made an implementation defect look like a
strategy difference.

Fixed by dialling in bounded concurrent waves and revalidating *between* waves
rather than before every call, skipping the first wave entirely because the
orchestrator built that snapshot microseconds earlier. Concurrency is safe here
precisely because reservation is a compare-and-swap.

## 4. The predictive bridge race — three distinct defects

Found by an invariant assertion, not by a scenario failing: an agent in
CONNECTED with no live call.

**4a. Finalising against a stale row.** `_finalise` read `agent_id` from the
copy of the call captured inside the event transaction. If the bridge wrote
`agent_id` after that read, the finaliser saw NULL and released nobody.
Reproduced deterministically by wrapping `mark_connected` to deliver the
COMPLETED event inside the two-write window.

**4b. State guards are not identity guards.** After 4a the suite passed, but a
12-seed stress failed 2 of 12. An agent could be released, re-reserved and
dialled for a *different* call, at which point a slow continuation from the old
call found exactly the state it expected and hijacked the agent onto a call
that had already ended. Fixed with `expected_call_id` and
`expected_reservation_id` guards pinning each transition to the work it was
issued for. The first attempt at this fix was itself wrong — it released an
agent unconditionally, which would have freed one another dial had legitimately
claimed.

**4c. Cancellation between commit and side effect.** Still failing about 1 run
in 14, with `ended_at` *after* the run ended. Not a race at all: `stop()`
cancels the carrier's delivery task, and cancelling it between the committed
state change and the follow-up work strands the agent. A partial shield over
the side effects was not enough, because the commit happens in a worker thread
and cancellation can land between the commit and the task creation. The whole
`handle` unit is now a tracked, shielded task.

Lesson worth keeping: one green run proved nothing here. Repeated stress plus
targeted deterministic tests found two defects that a passing suite had hidden.

## 5. Fault injection that avoided the interesting case

`force_offline(only_available=False)` was quietly excluding CONNECTED agents,
so "40 agents disappear" only ever took idle ones and the
agent-vanishes-mid-conversation path had never executed. With CONNECTED
included, seven live calls were left held by OFFLINE agents — borrowers
connected to nobody, with no provider event coming because the call is fine as
far as the carrier is concerned.

Fixed with a reconciler sweep that ends such calls and hangs up the carrier
leg. A connected call reaped this way is counted as **abandoned**, not
completed; recording it otherwise would flatter the exact number that exists to
catch this.

The deeper problem was the invariant, not the code. `assert_invariants` checked
that every CONNECTED agent had a live call but never the converse, which is why
94 tests passed over a broken path. Both directions are now asserted.

## 6. The abandonment budget was never a bound

Covered in full in `docs/safety-model.md`. Summary: `max_abandon_rate` was a
cumulative, lagging, binary throttle presented as a budget. Under an
answer-rate spike, BALANCED abandoned 23.75% against a supposed 3% limit.
Mitigations reduced that to 6.1%; it is still over budget under compound
attacks, and the mode is now labelled experimental rather than tuned until a
seed looked good.

The same investigation showed STRICT is not zero-abandonment either once agents
can vanish mid-flight. The claim in the code was corrected rather than the
measurement being explained away.

## 7. The dial path was not cancellation-safe

The same defect class as 4c, in the other half of the system. A dial is
reserve-then-release: win an agent, look for a borrower, give the agent back if
there is none. Cancelling the orchestrator between those steps left the agent
RESERVED until its lease expired — up to `agent_reservation_ttl_seconds` of a
real agent sitting idle. It surfaced as a one-in-three flake in the
zero-borrower adversarial test.

Fixed with the same mechanism used for events: dial waves run as tracked,
shielded tasks and `stop()` drains the allocator. Agents are back to AVAILABLE
by the time `stop()` returns, with no reconciliation needed. Two direct
regression tests were added rather than relying on the flaky test that found
it.

Worth noting that finding the event-path bug did not prompt me to check the
dial path. It took an unrelated adversarial test failing intermittently. A
defect class is worth grepping for once it is understood.

## 8. Frozen call from a bogus sequence number

A carrier sending `sequence = 2^62` makes every later event stale, so the call
cannot leave RINGING. The agent was already covered — the lease sweep includes
DIALING — but the borrower reservation leaked permanently, because the borrower
sweep skips borrowers whose call is still live.

Decision: this did **not** need a new sweep. `find_stuck_in_setup` already
keys on `updated_at`, and a frozen call has a stale `updated_at` by definition;
it simply was not looking at RINGING. Adding that one state closes the leak,
and `_recover_stale_calls` already asks the provider before ending anything, so
a call that is genuinely still ringing is left alone. A bounded correction to
an existing mechanism rather than a new one.

## 9. Ambiguous initiate timeouts — found, analysed, not fixed

`CallRequest` carries an `idempotency_key`. Nothing reads it. The mock ignores
it and mints a fresh `provider_call_id` on every call, so it is decoration that
reads like a guarantee — worse than no field at all, because a reviewer skims
it and stops asking.

The failure sequence, reproduced by forcing `initiate_call` to create a call
and *then* raise `ProviderTimeoutError`:

```
initiate_call invocations: 2
physical calls at carrier : 2
local call rows           : 1
duplicates: 0   stale: 3
```

`ProviderTimeoutError` subclasses `TransientProviderError`, so `classify()`
returns TRANSIENT and the retry budget applies in full. There is a
`FailureClass.UNKNOWN` branch whose comment says "we do not know what happened,
so we are stingier" — the concept was there, but a timeout, the canonical
ambiguous failure, was not routed into it.

What the database does and does not protect: the partial unique indexes
guarantee one live *local* row per borrower and per agent. They say nothing
about the carrier, because the retry loop lives inside `ResilientProvider`,
below the allocator, so only one row is ever written. `mark_initiated` then
overwrites `provider_call_id` with the second handle and the first call becomes
unreachable — not cancellable, not `get_status`-able, invisible to the
reconciler.

The sharp end: both carrier calls emit events under the same logical `call_id`
with independent sequence counters. The sequence guard absorbs the collision
without corrupting state (`stale: 3`), but it does so by discarding real events
from a real live call. `duplicates: 0` confirms the ledger is no defence —
`event_id` is a fresh UUID per emission. In production the orphan rings a
borrower, they answer, the ANSWERED event is rejected as stale, and nobody is
bridged. That is an abandoned call **no counter in this system records**, which
matters because measured abandonment is the central claim of
`docs/safety-model.md`.

### Why it is not fixed

Not a scope excuse; the analysis changed the answer twice.

First, no-retry alone does not close it. It prevents *us* creating a second
call, but the first one is already live and still abandons the borrower. "No
duplicate dial" and "no orphan" are different problems.

Second, and decisively: **the mock cannot produce the ambiguous case at all.**
`initiate_call` raises before it creates the `_CallSim`, so a timeout never
leaves an orphan anywhere in the test suite or the simulations. Every candidate
fix — reclassifying to UNKNOWN, borrower cooldown, counting the orphan via the
existing `late_answer_merged` branch — would be code no test exercises. Making
it real means adding an ambiguous-accept mode to `ProviderProfile`, which
changes provider B and demo 4 and therefore requires re-measuring the
comparison table, the attack matrix and the load figures.

Third, teaching only the mock to honour the key would be actively harmful here.
The abandonment numbers come from simulation against that mock; a deduplicating
mock never generates the orphan and would systematically understate real-world
abandonment. That is the same error as `force_offline` skipping CONNECTED
agents — fault injection gentler than reality, letting a passing suite hide a
broken path. A mock that is more correct than the world is a confidence
generator, not a test.

Carrier support cannot be assumed either. Twilio publishes idempotency-key
handling for Monitor Alarms and for Conversations Orchestrator configuration
operations, but not for call origination; the IETF draft is explicit that a
client cannot assume a server honours the header without prior knowledge.

### What a real adapter would need

At-most-once setup (`max_attempts = 1` for ambiguous failures) plus a borrower
cooldown longer than max call duration, so the deferred retry cannot recreate
the duplicate; carrier-side idempotency or cancel-by-client-reference to
eliminate the orphan; and counting `late_answer_merged` on a terminal call that
never connected, so the residual risk is visible rather than invisible. The
first and third are cheap. The second is a carrier capability, not a code
change.
