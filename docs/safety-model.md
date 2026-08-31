# Safety model

The design principle is that prediction may be probabilistic but execution must
be deterministic. This document says precisely what that buys and — more
importantly — what it does not.

## What the Safety Controller guarantees

**The guarantee is about the dialling decision, not about the eventual
outcome.**

At the moment it authorises a batch, the controller guarantees:

```
committed unbound calls  <=  max_unbound_in_flight(snapshot)
```

where `committed = unbound calls in flight + calls answered and awaiting an
agent`. In STRICT mode `max_unbound_in_flight` is exactly `available_agents`, so
even if every ringing phone were answered in the same instant there would be an
agent for each one.

That property is arithmetic, and it is tested by sweeping the whole small-input
space rather than by example (`test_strict_never_approves_more_than_available_agents`).

**It does not follow that no call is abandoned.** Between the decision and the
borrower answering, the world can change. Agents log out, laptops close, VPNs
drop. A call that has already reached ANSWERED cannot be cancelled — hanging up
on someone who has just said hello is the outcome the whole system exists to
avoid — so if the agent pool shrinks underneath it, that call is abandoned.

An earlier version of this document, and of the docstrings in `config.py`, said
abandonment was "impossible by arithmetic rather than by luck". That was wrong,
and the measurements below are what disproved it.

## Measured abandonment under attack

Four attacks, four seeds each, worst case by rate. "Dropped" counts every
borrower who was talking to nobody: classic predictive abandonment (answered,
no agent free) *and* calls reaped because their agent went offline mid-
conversation. Both are compliance events; the system counts them under one
metric deliberately, because recording the second as a clean completion would
flatter the exact number that exists to catch it.

| attack | PROGRESSIVE | PREDICTIVE/STRICT | PREDICTIVE/BALANCED |
|---|---|---|---|
| short campaign, few answers | 0% | 0% | 0% |
| answer rate spike 5% → 95% | 0% | 0% | 7.7% |
| mass agent drop mid-flight | 46.7% | 43.9% | 42.9% |
| spike and drop together | 22.0% | 6.3% | 15.6% |

Read the third row carefully, because it is the most misleading number in this
repository if taken at face value. In the mass-agent-drop attack **essentially
100% of the drops are agents vanishing mid-conversation**, and the rate is
about the same in all three modes. Forty-five of sixty agents disappearing
kills those conversations whichever way the calls were dialled. That is an
agent-infrastructure failure, not a dialling-strategy failure, and no pacing
policy can prevent it.

The rows that actually distinguish the modes are the second and fourth, where
the drops are predictive abandonment with no agent-offline component:
progressive and STRICT abandon nobody on a pure answer-rate spike; BALANCED
abandons 7.7%.

An earlier version of this table showed 0% for progressive on the agent-drop
rows. That was not because progressive handled it — it was because those
dropped calls were being silently leaked instead of counted, which is defect 5
in the engineering log. Fixing the leak made the numbers worse and correct.

## The three policies

**PROGRESSIVE** — the agent is reserved before the call exists, so an answered
call always has an agent waiting and no call is abandoned for want of one. It
is the floor the system falls back to. It is *not* immune to agents vanishing
mid-conversation: on that attack it fares no better than the predictive modes,
because the failure is upstream of dialling entirely.

**PREDICTIVE / STRICT** — the production default (`SafetyConfig().mode is
SafetyMode.STRICT`, asserted by a test). Zero abandonment with a stable agent
pool; small and bounded when the pool shrinks. Utilisation is roughly equal to
progressive, because a cap of "unbound in flight ≤ available agents" can only
match agent-bound dialling. Its value is that it uses the same machinery as
BALANCED and can be relaxed without changing any execution code.

**PREDICTIVE / BALANCED** — experimental. Deliberately over-dials. Carries no
abandonment-rate guarantee of any kind. Opt in knowingly.

## What `max_abandon_rate` actually is

A reactive throttle, not a bound. It is worth being blunt because the name
invites the wrong reading.

The 3% default was chosen by me as a nod to the FCC/Ofcom convention. The
assignment does not specify a figure. Originally it compared the *cumulative*
abandonment rate against the threshold and returned a binary stop/continue.
That has three defects:

1. it is measured after the fact and can only stop the *next* call;
2. a cumulative rate is self-healing in the worst way — abandon nineteen calls
   early and a long campaign dilutes them below the threshold while nothing has
   actually improved;
3. `min_answers_for_abandon_rate = 20` left the first twenty answers entirely
   ungoverned.

Under an answer-rate spike this produced **23.75%** abandonment against a
supposed 3% budget (measured before agent-offline reaping existed, so that
figure is pure predictive abandonment). The root cause was upstream: the controller's own
independent answer-rate estimate was cumulative, so a campaign that had dialled
for an hour at 5% kept reporting roughly 5% long after the true rate jumped to
95%, authorised an enormous overshoot, and abandoned the difference.

Three changes followed:

- the controller computes its bound over a rolling 200-dial window *and*
  cumulatively, taking the **higher** of the two (high answer rates are the
  dangerous side, so every error is forced upward);
- the abandon check is windowed, with an absolute count trip
  (`max_abandoned_in_window`) so a short campaign cannot hide behind "too few
  answers to judge";
- `max_overshoot_calls` caps how far BALANCED may exceed capacity in calls
  rather than sigmas, bounding how wrong a single tick can be when the
  stationarity assumption behind the statistics breaks.

Spike abandonment fell from 23.75% to 6.1%. Still over budget. BALANCED cannot
bound abandonment under a simultaneous regime change and mass agent loss, and
the honest response was to label it experimental rather than to keep tuning
until one particular seed looked acceptable.

## The capacity formula

Both modes are the same formula with a different assumed answer rate. Find the
largest `U` such that:

```
U*p + z*sqrt(U*p*(1-p))  <=  capacity
```

"Even at the upper end of the plausible range, the number of borrowers who pick
up does not exceed the agents available to take them." Substituting
`x = sqrt(U)` makes it a quadratic with a closed-form root, so it is pure
arithmetic — deterministic, unit-testable and checkable on paper.

- **STRICT**: `p = 1.0`, which collapses it to `U <= capacity`, with capacity
  being agents available right now.
- **BALANCED**: `p` from the controller's own upper-confidence estimate,
  `z = overshoot_sigmas`, capacity including a discounted credit for agents
  about to finish wrapping up.

The pooling effect falls out of the maths rather than being configured: safe
overshoot grows with `sqrt(capacity)`, so a 5-agent pool may overshoot by 7%
while a 500-agent pool may nearly double. That is the real reason predictive
dialling pays off at scale, and it is asserted in
`test_overshoot_grows_with_pool_size`.

## Non-bypassability

Three independent mechanisms, each with its own test. Defeating one leaves the
other two standing.

1. **Unforgeable authorisation.** `SafetyDecision.__post_init__` requires a
   module-private token held only by `safety.controller`. Constructing one
   anywhere else raises `PermissionError`. The allocator re-checks the token
   before performing any I/O.
2. **No code path from prediction to a telephone.** The `pacing` package
   imports no provider, no allocator and no I/O library. An AST scan asserts
   it, and a second scan asserts that only the allocator, the provider
   abstraction, recovery and wiring import a provider at all.
3. **Independent recomputation.** The controller never reuses the pacing
   engine's arithmetic. Handed a request claiming a 1% answer rate — the number
   that would justify a hundred calls per agent — its approval is unchanged
   (`test_controller_ignores_the_pacing_engines_answer_rate`).

## The continuous invariant

The capacity check runs every tick, not only when dialling. When agents
disappear, calls that were safe when placed are re-evaluated and the excess is
cancelled **while still ringing**. Answered calls are never cancelled. This is
what limits the damage in the agent-drop attacks above; it cannot eliminate it,
because a call that has already been answered has nowhere to go.
