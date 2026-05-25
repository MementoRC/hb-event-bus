"""EventBus — string-keyed publish/subscribe with sync and async dispatch."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from event_bus.subscription import Subscription

if TYPE_CHECKING:
    from collections.abc import Callable


class EventBus:
    """Central event bus: subscribe handlers to string event types and publish payloads.

    Storage: ``defaultdict[str, list[Subscription]]`` — insertion-order preserved,
    strong references held until ``unsubscribe`` is called.

    Thread-safety: NOT thread-safe; callers must synchronise external access if
    the bus is shared across threads.
    """

    __slots__ = ("_subscriptions", "name")

    def __init__(self, *, name: str = "default") -> None:
        self.name: str = name
        self._subscriptions: dict[str, list[Subscription]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Public subscription API
    # ------------------------------------------------------------------

    def subscribe(self, event_type: str, handler: Callable[[Any], Any]) -> Subscription:
        """Register *handler* for *event_type* and return an opaque :class:`Subscription`.

        Raises:
            TypeError: if *handler* is not callable.
        """
        if not callable(handler):
            raise TypeError(f"handler must be callable, got {type(handler).__name__!r}")
        sub = Subscription(event_type=event_type, handler=handler, bus=self)
        self._subscriptions[event_type].append(sub)
        return sub

    def unsubscribe(self, subscription: Subscription) -> None:
        """Remove *subscription* from the bus. Idempotent — safe to call multiple times.

        Also marks the subscription inactive (``subscription.active`` becomes ``False``).
        """
        subs = self._subscriptions.get(subscription.event_type)
        if subs is not None:
            try:
                subs.remove(subscription)
            except ValueError:
                pass  # already removed — idempotent no-op
        # Always mark inactive so repeated calls are harmless.
        subscription._active = False

    def get_subscribers(self, event_type: str) -> tuple[Subscription, ...]:
        """Return a snapshot tuple of active subscriptions for *event_type*."""
        return tuple(self._subscriptions.get(event_type, ()))
