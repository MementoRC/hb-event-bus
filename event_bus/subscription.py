"""Subscription handle returned by EventBus.subscribe()."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

Handler = Callable[[Any], None | Awaitable[None]]
SyncHandler = Callable[[Any], None]


class Subscription:
    """Opaque handle returned by EventBus.subscribe. Equality is identity.

    Holds a reference to the event type, handler, and owning bus.
    Call cancel() to unsubscribe; the operation is idempotent.
    """

    __slots__ = ("_active", "bus", "event_type", "handler")

    def __init__(self, *, event_type: str, handler: Handler, bus: Any) -> None:
        self.event_type: str = event_type
        self.handler: Handler = handler
        self.bus: Any = bus
        self._active: bool = True

    def cancel(self) -> None:
        """Unsubscribe from the owning bus. Idempotent — safe to call multiple times."""
        if self._active:
            self._active = False
            # bus.unsubscribe(self) is wired in Task A6 when EventBus exists.
            if hasattr(self.bus, "unsubscribe"):
                self.bus.unsubscribe(self)

    @property
    def active(self) -> bool:
        """True until cancel() has been called."""
        return self._active

    def __eq__(self, other: object) -> bool:
        return self is other

    def __hash__(self) -> int:
        return id(self)
