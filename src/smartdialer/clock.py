"""Injectable clock.

Every component takes its time from a ``Clock`` instead of calling
``time.time()`` directly. That buys two things:

* simulations run 100x faster than wall-clock while all configuration
  (talk time, TTLs, backoff) stays written in ordinary, readable seconds;
* tests can drive time deterministically instead of sleeping.

The unit used everywhere in this codebase is the *simulated second*.
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol


class Clock(Protocol):
    """Source of time for the whole system."""

    def now(self) -> float:
        """Current time in simulated seconds since an arbitrary origin."""

    async def sleep(self, seconds: float) -> None:
        """Sleep for ``seconds`` simulated seconds."""


class RealClock:
    """Wall-clock time. Used by the production-ish entry points."""

    def now(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))


class ScaledClock:
    """Time-compressed clock.

    ``scale=0.01`` means one simulated second costs 10 real milliseconds, so a
    90-second phone call finishes in 0.9s of real time. ``now()`` still returns
    simulated seconds, so metrics and TTLs read naturally.
    """

    def __init__(self, scale: float = 0.01, origin: float = 0.0) -> None:
        if scale <= 0:
            raise ValueError("scale must be > 0")
        self._scale = scale
        self._origin = origin
        self._t0 = time.monotonic()

    @property
    def scale(self) -> float:
        return self._scale

    def now(self) -> float:
        return self._origin + (time.monotonic() - self._t0) / self._scale

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds) * self._scale)


class ManualClock:
    """Fully deterministic clock for unit tests. Time only moves when told."""

    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def now(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds

    async def sleep(self, seconds: float) -> None:
        self._t += max(0.0, seconds)
        await asyncio.sleep(0)
