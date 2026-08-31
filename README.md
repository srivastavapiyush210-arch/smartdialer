# SmartDialer

A working progressive + predictive dialer for collections campaigns, built
around one rule: **prediction may be probabilistic, execution must be
deterministic.**

The predictive pacing engine cannot place a call. It produces a request; the
Safety Controller decides; the allocator executes only what the controller
authorised. That separation is enforced by three independent mechanisms and
asserted mechanically in CI, not described in a comment.

## Run it

```bash
git clone <repo> && cd smartdialer
pip install -r requirements.txt      # pytest + pytest-asyncio, nothing else

python3 -m pytest tests -q           # 139 tests, ~80s
python3 scripts/run_demo.py --all    # 10 demonstrations
python3 scripts/run_simulation.py --scenario B-medium-answer-rate --compare
python3 scripts/run_load_test.py --scale 1000
```

Python 3.11+. No services, no containers, no cloud. Storage is a SQLite file.

## What is guaranteed, and what is not

Worst-case abandonment, four adversarial attacks × four seeds:

| attack | PROGRESSIVE | PREDICTIVE/STRICT | PREDICTIVE/BALANCED |
|---|---|---|---|
| answer rate spike 5% → 95% | 0% | 0% | 7.7% |
| mass agent drop mid-flight | 46.7% | 43.9% | 42.9% |
| both at once | 22.0% | 6.3% | 15.6% |

The middle row is almost entirely agents vanishing mid-conversation, and it is
the same in every mode: forty-five of sixty agents disappearing kills those
calls however they were dialled. The rows that distinguish the modes are the
first and third.

**No predictive mode guarantees zero abandonment under arbitrary mid-flight
agent loss.** What the Safety Controller guarantees is about the *dialling
decision*: committed unbound calls never exceed available agents at the moment
of authorisation. Converting that into zero abandoned calls additionally
requires the agent pool not to shrink before those calls are answered.

STRICT is the default. BALANCED is experimental and carries no
abandonment-rate guarantee — `max_abandon_rate` is a reactive throttle, not a
bound. The reasoning, and the measurement that disproved an earlier and more
confident claim in this file, is in
[`docs/safety-model.md`](docs/safety-model.md).

## How it dials, and what that buys

Progressive dialling reserves an agent before the call exists, so nobody can
answer into an empty seat. Predictive dialling places calls with no agent held
and grabs one at the instant the borrower answers — that is where the
utilisation gain comes from, and where every abandonment risk lives.

| answer rate | progressive | predictive/STRICT | predictive/BALANCED |
|---|---|---|---|
| 20% | 0.603 | 0.578 | **0.718** |
| 50% | 0.697 | 0.682 | **0.744** |
| 70% | 0.828 | 0.824 | 0.827 |

Agent utilisation, means across seeds, with a stable agent pool. Predictive
recovers wasted ring time, so
its benefit is inversely proportional to the answer rate — large at 20%,
essentially nothing at 70%. STRICT sits marginally *below* progressive by
construction, which is a property of the model rather than a disappointment.
Full numbers and caveats: [`docs/results.md`](docs/results.md).

## Layout

```
src/smartdialer/
  dialer/         control loop and snapshot
  pacing/         estimator and pacing engine (no DB, no provider, no allocator)
  safety/         the non-bypassable boundary
  allocation/     the only component that talks to a carrier
  events/         carrier event application and side effects
  reliability/    circuit breaker, retries, routing, reconciliation
  repositories/   all SQL; compare-and-swap reservations
  persistence/    SQLite, WAL, BEGIN IMMEDIATE
  simulation/     scenarios and the simulation runner
tests/            unit, concurrency, integration, simulation, adversarial
scripts/          run_demo.py, run_simulation.py, run_load_test.py
docs/
```

## Docs

- [`docs/safety-model.md`](docs/safety-model.md) — what is guaranteed, what is
  not, and the measurements
- [`docs/architecture.md`](docs/architecture.md) — components, concurrency,
  event handling, recovery, scaling
- [`docs/results.md`](docs/results.md) — every measured number and how to
  reproduce it
- [`docs/adr.md`](docs/adr.md) — decisions and what was rejected
- [`docs/engineering-log.md`](docs/engineering-log.md) — six defects found by
  running the system, and two known gaps left open

## Known limits

- **Borrower selection collapses 34×** between 100 and 10,000 agents — queue
  hot-spotting, remedy named in `docs/results.md`, not implemented.
- **Agents vanishing mid-conversation drop those calls** in every mode. No
  dialling policy prevents it.
- **BALANCED exceeds its own configured budget** under compound attack.
- **Simulations are not bit-reproducible**: `ScaledClock` reads wall-clock
  time, so seeds do not pin interleaving. Medians across repeated runs are used
  instead of single-run figures.
- **Ambiguous initiate timeouts can create two physical carrier calls.**
  `idempotency_key` is carried but honoured by no adapter. Traced in full in
  `docs/engineering-log.md`, including why fixing it in the mock would have
  corrupted the abandonment measurements above.
