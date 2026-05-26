"""event_bus._internal — module-private dispatch helpers.

These functions are the low-level machinery used by EventBus to invoke
registered handlers.  Nothing in this module is part of the public API.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from event_bus.bus import EventBus

logger = logging.getLogger("event_bus")

# Track which bus_ids have already emitted the "no running loop" warning so
# that repeated calls only produce one log entry (rate-limited per bus).
_warned_buses: set[int] = set()


def _is_async_handler(handler: Any) -> bool:
    """True if handler should dispatch via the async (await) path.

    Returns True when:
    - handler is a coroutine function (``async def f(...)``), OR
    - handler is a class instance whose ``__call__`` is a coroutine function.

    The two branches do not overlap in practice: a plain coroutine function
    returns True at the first check; a class instance returns False there
    and falls through to probe ``__call__``.
    """
    if inspect.iscoroutinefunction(handler):
        return True
    call = getattr(handler, "__call__", None)  # noqa: B004
    if call is None:
        return False
    return inspect.iscoroutinefunction(call)


def _is_event_listener(obj: Any) -> bool:
    """Lazy isinstance check for EventListener; avoids circular import.

    listener.py and _internal.py are both imported by bus.py; importing
    EventListener at module scope here would create a cycle. The local
    import is cached by Python's module system after the first call.
    """
    from event_bus.listener import EventListener

    return isinstance(obj, EventListener)


def invoke_sync_handlers(
    event_type: str,
    payload: Any,
    handlers: Iterable[Callable[[Any], None]],
    *,
    bus: EventBus,
) -> None:
    """Invoke each sync handler in iteration order with EventListener context injection.

    For EventListener instances, ``current_event_type`` and ``current_event_bus``
    are set immediately before the call. Exceptions are caught, logged, and
    isolated — subsequent handlers still run. Coroutines accidentally returned
    from a sync dispatch path are closed and a WARNING is emitted.
    """
    for handler in handlers:
        if _is_event_listener(handler):
            handler.current_event_type = event_type  # type: ignore[attr-defined]
            handler.current_event_bus = bus  # type: ignore[attr-defined]
        try:
            result = handler(payload)
            if inspect.iscoroutine(result):
                result.close()
                logger.warning(
                    "Sync dispatch received a coroutine from %r on topic=%r; "
                    "use apublish() or expose async __call__ via a real method.",
                    handler,
                    event_type,
                )
        except Exception:  # noqa: BLE001
            logger.exception("handler failed on event_type=%r", event_type)


def schedule_async_handler(
    event_type: str,
    payload: Any,
    handler: Callable[[Any], Awaitable[None]],
    *,
    bus_id: int = 0,
) -> None:
    """Schedule *handler* on the running event loop via :func:`asyncio.ensure_future`.

    If there is no running loop the call is a no-op; a single WARNING is
    emitted per *bus_id* (rate-limited to avoid log flooding).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        if bus_id not in _warned_buses:
            _warned_buses.add(bus_id)
            logger.warning(
                "no running event loop; async handler for event_type=%r skipped",
                event_type,
            )
        return
    asyncio.ensure_future(_run_async_handler(event_type, payload, handler), loop=loop)


async def _run_async_handler(
    event_type: str,
    payload: Any,
    handler: Callable[[Any], Awaitable[None]],
) -> None:
    """Await *handler* and isolate any exception it raises."""
    try:
        await handler(payload)
    except Exception:
        logger.exception("async handler failed on event_type=%r", event_type)
