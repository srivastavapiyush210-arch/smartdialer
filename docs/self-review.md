# Self-review

Nothing is claimed here that is not in the repository. Where a requirement is
only partly met, it says so.

| requirement | done | where | how demonstrated |
|---|---|---|---|
| Progressive dialling | yes | `allocation/allocator.py::run_progressive` | demo 1; `test_progressive_campaign_connects_calls_and_abandons_none` |
| Predictive pacing | yes | `pacing/engine.py`, `pacing/estimator.py` | demo 2; `tests/simulation` |
| Safety Controller, non-bypassable | yes | `safety/controller.py` | demo 10; `tests/unit/test_safety_boundary.py` (3 mechanisms) |
| Prediction cannot place a call | yes | AST scan over `pacing/` | `test_pacing_package_cannot_reach_a_provider_or_allocator` |
| Campaign → pacing → safety → allocator → provider | yes | `dialer/orchestrator.py::tick` | `docs/architecture.md` |
| Agent state machine | yes | `state/agent_fsm.py` | `tests/unit/test_state_machines.py` |
| Call state machine | yes | `state/call_fsm.py` | same |
| Atomic agent allocation | yes | conditional UPDATE in `repositories/agents.py` | demo 7; `test_fifty_workers_racing_for_ten_agents_reserve_exactly_ten` |
| Atomic borrower allocation | yes | `repositories/borrowers.py` | `test_concurrent_workers_never_hand_out_the_same_borrower` |
| Idempotent event handling | yes | ledger in the event transaction | demo 5; `test_duplicate_event_has_no_duplicate_side_effect` |
| Out-of-order events | yes | per-call sequence numbers | demo 6; `test_out_of_order_events_do_not_regress_the_call` |
| Provider failure handling | yes | `reliability/` | demo 4; `test_provider_outage_stops_or_reduces_new_dialing` |
| Circuit breaker + retries | yes | `reliability/circuit_breaker.py`, `retry.py` | demo 4 |
| Crash recovery | yes | `reliability/recovery.py` | demo 8; `test_worker_crash_leaves_no_agent_stranded` |
| Agents disappearing | yes | continuous invariant + reaping sweep | demo 9; `test_calls_are_reaped_when_their_agent_goes_offline` |
| Simulation across answer rates | yes | `simulation/scenarios.py` A–H | `scripts/run_simulation.py --compare`; `docs/results.md` |
| Load test 100/1k/10k | yes | `scripts/run_load_test.py` | `docs/results.md` |
| Bottleneck identified | yes | borrower queue hot-spotting | measured 34× degradation, remedy named, **not implemented** |
| 10 demos | yes | `scripts/run_demo.py` | `--all` |
| Documentation | yes | `docs/` | this table |

## Honest gaps

- **Zero abandonment is not guaranteed by any predictive mode** under
  mid-flight agent loss. Measured, documented, and the earlier stronger claim
  was retracted rather than defended.
- **BALANCED exceeds its own configured budget** under compound attack (31%).
  Mitigated from 23.75% to 6.1% on the single-attack case; labelled
  experimental rather than tuned until a seed looked acceptable.
- **Agent loss mid-conversation drops calls in every mode**, at roughly the
  same rate (~43-47%). No dialling policy prevents it; the numbers are reported
  rather than hidden, and the earlier 0% for progressive was a counting leak.
- **Borrower-queue contention not fixed.** Measured and explained; the fix is a
  design change rather than a correction.
- **Ambiguous initiate timeouts are unhandled.** A carrier that accepts a call
  the client then times out on can be dialled twice, and the orphan can abandon
  a borrower without incrementing any counter. Analysed rather than fixed; the
  reasoning, including why mock-side idempotency was rejected, is in the
  engineering log.
- **Simulations are not bit-reproducible** — `ScaledClock` reads wall-clock
  time, so seeds do not pin interleaving. Repeated runs and medians are used
  instead of single-run figures.
- **Utilisation numbers carry a measurement caveat**: per-tick compute consumes
  simulated time and penalises whichever mode does more work per tick.

## What I would do next, in order

1. Partitioned borrower claiming to remove the queue hot spot — the one
   measured bottleneck still open.
2. Reduce BALANCED's exposure on compound attacks, or withdraw the mode.
3. Split the drop metric into predictive abandonment and agent-loss drops.
   They are both compliance events and both must be counted, but they have
   different owners and different fixes.
