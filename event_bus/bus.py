"""EventBus — string-keyed publish/subscribe with sync and async dispatch."""

from __future__ import annotations

import inspect
import logging
from collections import defaultdict
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from event_bus._internal import invoke_sync_handlers, schedule_async_handler
from event_bus.subscription import Subscription

if TYPE_CHECKING:
    from collections.abc import Callable

    from event_bus.subscription import SyncHandler

logger = logging.getLogger("event_bus")


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
            with suppress(ValueError):
                subs.remove(subscription)
        # Always mark inactive so repeated calls are harmless.
        subscription._active = False

    def get_subscribers(self, event_type: str) -> tuple[Subscription, ...]:
        """Return a snapshot tuple of active subscriptions for *event_type*."""
        return tuple(self._subscriptions.get(event_type, ()))

    # ------------------------------------------------------------------
    # Public dispatch API
    # ------------------------------------------------------------------

    def publish(self, event_type: str, payload: Any = None) -> None:
        """Synchronous dispatch: invoke sync handlers inline; schedule async handlers.

        Args:
            event_type: The string key identifying the event channel.
            payload:    Arbitrary value forwarded to each handler.

        Behaviour:
        - Takes a snapshot of current subscriptions before iterating, so handlers
          that subscribe/unsubscribe during dispatch do not affect the current call.
        - Sync handlers are invoked in registration order; exceptions are logged
          and isolated — subsequent handlers still run.
        - Async handlers are scheduled on the running event loop via
          :func:`asyncio.ensure_future`; if there is no running loop a WARNING is
          emitted (once per bus instance) and the handler is silently skipped.
        - An empty *event_type* logs a DEBUG message and dispatches to no handlers
          (empty string keys are never subscribed to in normal usage).
        """
        if not event_type:
            logger.debug("publish called with empty event_type")
        subs = list(self._subscriptions.get(event_type, []))  # snapshot for re-entrancy safety
        sync_handlers: list[SyncHandler] = []
        for sub in subs:
            if inspect.iscoroutinefunction(sub.handler):
                schedule_async_handler(event_type, payload, sub.handler, bus=self)
            else:
                sync_handlers.append(sub.handler)  # type: ignore[arg-type]
        invoke_sync_handlers(event_type, payload, sync_handlers, bus=self)

    async def apublish(self, event_type: str, payload: Any = None) -> None:
        """Async dispatch: await async handlers sequentially; invoke sync handlers inline.

        Args:
            event_type: The string key identifying the event channel.
            payload:    Arbitrary value forwarded to each handler.

        Behaviour:
        - Takes a snapshot of current subscriptions before iterating, so handlers
          that subscribe/unsubscribe during dispatch do not affect the current call.
        - Handlers are invoked in registration order regardless of sync/async kind.
        - Async handlers are awaited sequentially (not concurrently via gather),
          preserving ordering guarantees.
        - Sync handlers are invoked inline at their position in registration order.
        - Exceptions from any handler are logged and isolated — subsequent handlers
          still run.
        - An empty *event_type* logs a DEBUG message and dispatches to no handlers.
        """
        if not event_type:
            logger.debug("apublish called with empty event_type")
        subs = list(self._subscriptions.get(event_type, []))  # snapshot for re-entrancy safety
        for sub in subs:
            try:
                if inspect.iscoroutinefunction(sub.handler):
                    await sub.handler(payload)
                else:
                    sub.handler(payload)
            except Exception:  # noqa: BLE001
                logger.exception("handler failed on event_type=%r", event_type)
