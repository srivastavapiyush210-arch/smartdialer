# Measured results

Every number here was produced by a command in this repository. Nothing is
estimated or carried over from a previous run.

## Predictive vs progressive

`scripts/run_simulation.py --scenario <name> --compare`. 50 agents. Mean agent
utilisation (connected agent-seconds / online agent-seconds); 3 seeds for the
low-answer-rate case, 2 for the others.

| answer rate | PROGRESSIVE | PREDICTIVE/STRICT | PREDICTIVE/BALANCED |
|---|---|---|---|
| 20%, 120s talk | 0.603 | 0.578 | **0.718** |
| 50%, 90s talk | 0.697 | 0.682 | **0.744** |
| 70%, 180s talk | 0.828 | 0.824 | 0.827 |

Zero abandoned calls in all runs at this stage. These runs do not include the
mid-flight agent-loss attacks; see `safety-model.md`, and note that the
abandonment figures there were re-measured after agent-offline reaping was
added, which raised them across every mode.

The shape is the result, not the individual figures. Predictive dialling
recovers wasted ring time, so its benefit is inversely proportional to the
answer rate: large at 20%, modest at 50%, essentially nothing at 70% where
agents are close to saturated anyway. STRICT sits marginally *below*
progressive by construction — with `p = 1.0` the cap collapses to "unbound in
flight ≤ available agents", which can only match agent-bound dialling, minus a
little coordination cost.

**Measurement caveat.** Simulations run the real system on a compressed clock,
so per-tick compute consumes simulated time and penalises whichever mode does
more work per tick — predictive. A single pair of runs put the apparent
difference anywhere between −11% and +19% before this was understood; the
figures above alternate configurations and take means across seeds. Utilisation
here is "productive" utilisation, which deliberately excludes RESERVED and
DIALING: an agent listening to a ring tone is occupied but not productive, and
counting that as busy would flatter progressive.

## Load test

`scripts/run_load_test.py --scale N`, 32 concurrent in-flight operations
(matching the allocator's bounded waves).

| operation | 100 agents | 1,000 | 10,000 |
|---|---|---|---|
| agent reservation (CAS) | 9,344/s | 8,023/s | 9,393/s |
| **borrower selection** | 2,542/s | 742/s | **74/s** |
| call creation | 5,743/s | 8,245/s | 8,091/s |
| agent state transition | 9,051/s | 10,760/s | 10,830/s |
| pacing + safety decision | 29,885/s | 27,382/s | 27,744/s |

**The bottleneck is borrower selection**, which collapses 34× between 100 and
10,000 agents: p50 latency 5.6 ms → 376 ms, with 18 operations refused outright
with `database is locked`.

It is not SQLite's write lock in general — call creation and transitions at the
same concurrency held at 8–10k/s. It is specific to `reserve_next`: every
worker runs the same `ORDER BY priority DESC, not_before, created_at, id
LIMIT 1` scan against a 40,000-row table inside `BEGIN IMMEDIATE`, so they all
contend on the identical head-of-queue rows and serialise behind one another.
Classic queue hot-spotting, and the total ordering chosen for reproducibility is
exactly what makes every worker want the same row.

The remedy is partitioned claiming, or randomised selection among the top N
eligible borrowers — trading strict ordering for reduced contention. Not
implemented here.

Two caveats on these numbers. The first 10,000-agent attempt crashed with lock
exhaustion because the harness fired all 10,000 operations concurrently, which
is not how the dialer behaves; it was bounded to 32 to match the allocator and
now counts lock refusals rather than dying. And the flat ~28k/s for
pacing + safety is unremarkable: pure arithmetic on a synthetic snapshot, no
I/O.

## Cost of shielded event handling

`scripts/run_load_test.py --shield-comparison`. Five alternating repetitions,
medians.

| metric | shield off | shield on | delta |
|---|---|---|---|
| throughput | 1184/s | 1101/s | −7.0% |
| drain time | ~0s | ~0s | 0% |
| peak outstanding tasks | 85 | 84 | −1.2% |
| memory growth | 351.8 KB | 359.2 KB | +2.1% |
| peak traced memory | 633.4 KB | 743.5 KB | +17.4% |
| event latency p99 | 53.98 ms | 56.01 ms | — |

Run-to-run spread with the shield **off** alone was 1143–1205/s, so the
throughput cost is best read as "somewhere between 0 and 7%", not as a precise
7%. A first pass measuring one run of each gave −1.2%, +15.3%, +12.5% and +2.3%
on successive attempts — that was noise, and reporting any one of those figures
would have been dishonest.

The real cost is one task allocation per event, a couple of microseconds
against a per-event budget of ~850 µs, since every event is a SQLite
transaction dispatched to a thread. The +17% peak memory is a percentage of a
very small base — about 110 KB of task objects — and does not grow with
campaign duration.

**Judgement.** Kept. Not kept merely because it fixes the race, and not removed
for performance: at any scale this prototype can reach, the event rate outgrows
a single SQLite writer long before task overhead registers. If event processing
ever moved to a store with sub-100 µs writes, this measurement would need
redoing, because the constant would stop being negligible.

## Tests

139 passing.

| suite | what it covers |
|---|---|
| `tests/unit` | state machines, safety arithmetic, the three non-bypassability mechanisms |
| `tests/concurrency` | reservation races, event idempotency and ordering, the bridge race, cancellation safety |
| `tests/integration` | end-to-end campaigns, provider failure and recovery, crash recovery, agent loss |
| `tests/simulation` | behaviour across answer-rate regimes and injected faults |
| `tests/adversarial` | attacks on the arithmetic, the event ledger, resource accounting and the safety story |
