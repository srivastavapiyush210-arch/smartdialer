"""Retry policy.

Two rules that matter more than the code:

1. **Classify before retrying.** A timeout may succeed next time; an invalid
   phone number never will. Retrying a permanent failure wastes an agent's
   time and burns the borrower's attempt budget for nothing.
2. **Bound everything.** Fixed maximum attempts, exponential backoff, and
   jitter so that N workers whose calls failed together do not all come back
   at the same instant.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from ..config import ReliabilityConfig
from ..models.domain import CircuitOpenError, ProviderError
from ..models.enums import FailureClass


def classify(error: BaseException) -> FailureClass:
    if isinstance(error, ProviderError):
        return error.failure_class
    if isinstance(error, TimeoutError):
        return FailureClass.TRANSIENT
    return FailureClass.UNKNOWN


@dataclass(frozen=True)
class RetryDecision:
    should_retry: bool
    delay_seconds: float
    reason: str


class RetryPolicy:
    def __init__(self, config: ReliabilityConfig, *, seed: int = 7) -> None:
        self._config = config
        self._rng = random.Random(seed)

    def max_attempts(self, failure_class: FailureClass) -> int:
        if failure_class is FailureClass.PERMANENT:
            return 1
        if failure_class is FailureClass.UNKNOWN:
            # We do not know what happened, so we are stingier than for a
            # confirmed transient error.
            return min(2, self._config.max_retries + 1)
        return self._config.max_retries + 1

    def decide(self, attempt: int, error: BaseException) -> RetryDecision:
        """``attempt`` is 1-based: the number of tries already made."""
        failure_class = classify(error)
        if failure_class is FailureClass.PERMANENT:
            return RetryDecision(False, 0.0, "permanent_failure")
        if isinstance(error, CircuitOpenError):
            # The breaker already decided the provider is unwell. Retrying now
            # would defeat the purpose of the breaker.
            return RetryDecision(False, 0.0, "circuit_open")
        if attempt >= self.max_attempts(failure_class):
            return RetryDecision(False, 0.0, "retry_budget_exhausted")
        return RetryDecision(True, self.backoff(attempt), f"retry_{failure_class.value}")

    def backoff(self, attempt: int) -> float:
        base = self._config.base_backoff_seconds * (2 ** (attempt - 1))
        base = min(base, self._config.max_backoff_seconds)
        jitter = base * self._config.backoff_jitter
        return max(0.0, base + self._rng.uniform(-jitter, jitter))
