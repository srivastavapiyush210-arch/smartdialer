"""In-process metrics.

Deliberately small: counters, gauges and a bounded sample list that can answer
percentile questions. No Prometheus, no Grafana -- there is exactly one process
in this prototype, so a shared dictionary behind a lock is the correct amount of
machinery. The scaling section of the architecture document explains what
replaces it when there is more than one process.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Iterable


# How many recent dial attempts the Safety Controller reasons over. Large
# enough that the rate is not noise, small enough to turn over within a couple
# of minutes of real dialling.
RECENT_WINDOW = 200


@dataclass
class Histogram:
    """Reservoir of observations, enough for p50/p95/p99 on a prototype."""

    name: str
    values: list[float] = field(default_factory=list)
    max_samples: int = 20_000

    def observe(self, value: float) -> None:
        if len(self.values) < self.max_samples:
            self.values.append(value)

    @property
    def count(self) -> int:
        return len(self.values)

    def percentile(self, q: float) -> float:
        if not self.values:
            return 0.0
        ordered = sorted(self.values)
        idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
        return ordered[idx]

    def summary(self) -> dict[str, float]:
        if not self.values:
            return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0,
                    "max": 0.0}
        return {
            "count": len(self.values),
            "mean": sum(self.values) / len(self.values),
            "p50": self.percentile(0.50),
            "p95": self.percentile(0.95),
            "p99": self.percentile(0.99),
            "max": max(self.values),
        }


class MetricsCollector:
    """Thread-safe because SQLite work runs on a thread pool."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, Histogram] = {}
        self._events: list[tuple[float, str, str]] = []
        self._recent: deque[tuple[int, int]] = deque(maxlen=RECENT_WINDOW)

    def incr(self, name: str, value: float = 1.0) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0.0) + value

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            hist = self._histograms.get(name)
            if hist is None:
                hist = self._histograms[name] = Histogram(name)
            hist.observe(value)

    def note(self, at: float, kind: str, detail: str) -> None:
        """Timeline entry: used by demos to show *when* the system reacted."""
        with self._lock:
            self._events.append((at, kind, detail))

    def counter(self, name: str) -> float:
        with self._lock:
            return self._counters.get(name, 0.0)

    def get_gauge(self, name: str) -> float:
        with self._lock:
            return self._gauges.get(name, 0.0)

    def histogram(self, name: str) -> Histogram:
        with self._lock:
            return self._histograms.get(name, Histogram(name))

    def timeline(self) -> list[tuple[float, str, str]]:
        with self._lock:
            return list(self._events)

    def record_dial_outcome(self, *, answered: bool, abandoned: bool = False) -> None:
        """Append one completed dial attempt to the rolling window.

        Cumulative counters cannot see a regime change: a campaign that dialled
        200,000 numbers at a 5% answer rate keeps reporting roughly 5% for a
        very long time after the true rate jumps to 95%. The Safety Controller
        plans against the *recent* window for exactly that reason.
        """
        with self._lock:
            self._recent.append((1 if answered else 0, 1 if abandoned else 0))

    def recent_outcomes(self) -> tuple[int, int, int]:
        """(dials, answers, abandoned) over the rolling window."""
        with self._lock:
            dials = len(self._recent)
            answers = sum(a for a, _ in self._recent)
            abandoned = sum(b for _, b in self._recent)
        return dials, answers, abandoned

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "counters": dict(sorted(self._counters.items())),
                "gauges": dict(sorted(self._gauges.items())),
                "histograms": {
                    k: v.summary() for k, v in sorted(self._histograms.items())
                },
            }

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
            self._events.clear()
            self._recent.clear()


# Canonical metric names, so typos do not silently create new series.
class M:
    CALLS_INITIATED = "calls_initiated"
    CALLS_ANSWERED = "calls_answered"
    CALLS_CONNECTED = "calls_connected"
    CALLS_COMPLETED = "calls_completed"
    CALLS_FAILED = "calls_failed"
    CALLS_CANCELLED = "calls_cancelled"
    CALLS_ABANDONED = "calls_abandoned_unsafe"
    CALLS_RESCUED_BY_GRACE = "calls_rescued_by_grace"
    SETUP_FAILURES = "call_setup_failures"

    PROVIDER_FAILURES = "provider_failures"
    PROVIDER_TIMEOUTS = "provider_timeouts"
    PROVIDER_RETRIES = "provider_retries"
    PROVIDER_PERMANENT = "provider_permanent_failures"
    CIRCUIT_OPENED = "circuit_breaker_opened"
    CIRCUIT_CLOSED = "circuit_breaker_closed"
    CIRCUIT_REJECTED = "circuit_breaker_rejections"
    PROVIDER_FAILOVER = "provider_failover"

    EVENTS_RECEIVED = "provider_events_received"
    EVENTS_APPLIED = "provider_events_applied"
    DUPLICATE_EVENTS = "duplicate_events"
    OUT_OF_ORDER_EVENTS = "out_of_order_events"
    TERMINAL_PROTECTED = "terminal_state_protected"
    INVALID_TRANSITIONS = "invalid_transition_events"
    NOOP_EVENTS = "noop_events"
    LATE_ANSWER_MERGED = "late_answer_facts_merged"

    SAFETY_APPROVALS = "safety_approvals"
    SAFETY_REDUCTIONS = "safety_reductions"
    SAFETY_REJECTIONS = "safety_rejections"
    SAFETY_FALLBACKS = "progressive_fallbacks"
    SAFETY_CANCELLATIONS = "safety_cancelled_ringing_calls"
    ALLOCATION_STOPPED_EARLY = "allocation_batches_stopped_early"
    STRANDED_AGENTS_RECOVERED = "stranded_agents_recovered"
    CALLS_REAPED_OFFLINE_AGENT = "calls_reaped_offline_agent"
    PACING_REQUESTS = "pacing_requests"
    PACING_REQUESTED_CALLS = "pacing_requested_calls_total"
    SAFETY_APPROVED_CALLS = "safety_approved_calls_total"

    AGENT_RESERVE_SUCCESS = "agent_reservations_succeeded"
    AGENT_RESERVE_CONTENTION = "agent_reservation_contention"
    BORROWER_RESERVE_CONTENTION = "borrower_reservation_contention"
    STALE_AGENTS_RECOVERED = "stale_agent_reservations_recovered"
    STALE_BORROWERS_RECOVERED = "stale_borrower_reservations_recovered"
    STALE_CALLS_RECOVERED = "stale_calls_recovered"
    RECOVERY_RUNS = "recovery_runs"

    G_AVAILABLE_AGENTS = "available_agents"
    G_RESERVED_AGENTS = "reserved_agents"
    G_CONNECTED_AGENTS = "connected_agents"
    G_WRAP_UP_AGENTS = "wrap_up_agents"
    G_OFFLINE_AGENTS = "offline_agents"
    G_RINGING_CALLS = "ringing_calls"
    G_UNBOUND_IN_FLIGHT = "unbound_calls_in_flight"
    G_ESTIMATED_ANSWER_RATE = "estimated_answer_rate"
    G_ESTIMATOR_CONFIDENCE = "estimator_confidence"
    G_PROVIDER_HEALTH = "provider_health"

    H_SETUP_LATENCY = "call_setup_latency_seconds"
    H_CALL_DURATION = "call_duration_seconds"
    H_RESERVE_LATENCY = "agent_reserve_latency_ms"
    H_EVENT_LATENCY = "event_processing_latency_ms"


def sum_counters(collector: MetricsCollector, names: Iterable[str]) -> float:
    return sum(collector.counter(n) for n in names)
