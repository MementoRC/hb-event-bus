"""Event logging utility for testing and debugging.

Captures events dispatched through an :class:`EventBus` into a bounded deque
and supports awaiting specific events by name with a timeout.

Port of ``hummingbot/core/event/event_logger.pyx`` (Cython) — adapted to the
string-keyed pub/sub API introduced in Phase 1.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from event_bus.bus import EventBus
    from event_bus.subscription import Subscription

logger = logging.getLogger("event_bus")


@dataclass(frozen=True, slots=True)
class LoggedEvent:
    """Snapshot of an event captured by :class:`EventLogger`.

    Attributes:
        name: Event name the bus dispatched.
        payload: The payload object passed to ``publish``/``apublish``.
        timestamp: ``time.monotonic()`` reading at capture time.
    """

    name: str
    payload: Any
    timestamp: float


class EventLogger:
    """Captures bus events for inspection in tests.

    Subscribe an instance to one or more event names via :meth:`attach`; each
    dispatched event is appended to an internal bounded deque. Tests can poll
    :attr:`events` or block on :meth:`wait_for` to await a specific event.

    Example:
        >>> bus = EventBus()
        >>> log = EventLogger(maxlen=10)
        >>> log.attach(bus, "order_created", "order_filled")
        >>> bus.publish("order_created", {"id": 1})
        >>> assert log.events[-1].name == "order_created"

    Args:
        maxlen: Maximum number of events to retain. ``None`` for unbounded
            (use with care in long-running tests). Defaults to 50, matching
            the historical Cython EventLogger's generic deque cap.
    """

    __slots__ = ("_events", "_subscriptions", "_waiters")

    def __init__(self, *, maxlen: int | None = 50) -> None:
        self._events: deque[LoggedEvent] = deque(maxlen=maxlen)
        self._subscriptions: list[Subscription] = []
        # event_name -> list of (asyncio.Event, payload_holder)
        self._waiters: dict[str, list[tuple[asyncio.Event, list[Any]]]] = {}

    def attach(self, bus: EventBus, *event_names: str) -> None:
        """Subscribe to one or more event names on ``bus``.

        Each subscription is tracked internally so :meth:`detach` can release
        them all at once.

        Args:
            bus: The event bus to subscribe to.
            *event_names: One or more event names. At least one is required.

        Raises:
            ValueError: If ``event_names`` is empty.
        """
        if not event_names:
            raise ValueError("attach() requires at least one event name")
        for name in event_names:
            sub = bus.subscribe(name, self._make_handler(name))
            self._subscriptions.append(sub)

    def detach(self) -> None:
        """Cancel all active subscriptions and clear the subscription list.

        Idempotent: safe to call multiple times. Does not clear captured
        events; use :meth:`clear` for that.
        """
        for sub in self._subscriptions:
            sub.cancel()
        self._subscriptions.clear()

    @property
    def events(self) -> list[LoggedEvent]:
        """Return a snapshot list of captured events in arrival order."""
        return list(self._events)

    def clear(self) -> None:
        """Discard all captured events. Leaves subscriptions intact."""
        self._events.clear()

    async def wait_for(self, event_name: str, *, timeout: float = 180.0) -> Any:
        """Block until an event with ``event_name`` arrives; return its payload.

        Each call registers a fresh waiter so concurrent waits for the same
        name are isolated — the next event dispatch wakes all of them and
        each receives the payload independently.

        Args:
            event_name: Event name to wait on. Must be one previously
                :meth:`attach`-ed (else this will time out).
            timeout: Maximum seconds to wait. Defaults to 180.0, matching
                the historical Cython EventLogger default.

        Returns:
            The payload of the first matching event after the wait began.

        Raises:
            TimeoutError: If no matching event arrives within ``timeout``.
        """
        notifier = asyncio.Event()
        payload_holder: list[Any] = []
        self._waiters.setdefault(event_name, []).append((notifier, payload_holder))
        try:
            async with asyncio.timeout(timeout):
                await notifier.wait()
        finally:
            waiters = self._waiters.get(event_name)
            if waiters is not None:
                with suppress(ValueError):
                    waiters.remove((notifier, payload_holder))
                if not waiters:
                    self._waiters.pop(event_name, None)
        return payload_holder[0] if payload_holder else None

    def _make_handler(self, name: str) -> Callable[[Any], None]:
        def handler(payload: Any) -> None:
            self._events.append(LoggedEvent(name=name, payload=payload, timestamp=time.monotonic()))
            waiters = self._waiters.get(name)
            if not waiters:
                return
            for notifier, holder in list(waiters):
                holder.append(payload)
                notifier.set()

        return handler
