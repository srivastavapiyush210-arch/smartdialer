"""Telecom provider interface.

The dialer depends on this Protocol and never on a concrete provider. Adding a
real Plivo/Twilio adapter later means implementing four methods; nothing in the
dialer, pacing engine or Safety Controller changes.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Protocol, runtime_checkable

from ..models.domain import CallHandle, CallRequest, ProviderEvent, ProviderHealth
from ..models.enums import CallState

EventSink = Callable[[ProviderEvent], Awaitable[None]]


@runtime_checkable
class TelecomProvider(Protocol):
    name: str

    async def initiate_call(self, request: CallRequest) -> CallHandle:
        """Ask the carrier to place a call.

        Raises ``TransientProviderError`` (retryable) or
        ``PermanentProviderError`` (never retry).
        """

    async def cancel_call(self, provider_call_id: str) -> bool:
        """Best-effort cancel of a call that has not been answered yet."""

    async def get_status(self, provider_call_id: str) -> CallState | None:
        """Authoritative state pull, used by the reconciler after a crash."""

    async def health_check(self) -> ProviderHealth:
        """Cheap health signal consumed by pacing and the Safety Controller."""
