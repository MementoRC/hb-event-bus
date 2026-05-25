"""event_bus._internal — module-private dispatch helpers.

These functions are the low-level machinery used by EventBus to invoke
registered handlers.  Nothing in this module is part of the public API.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

logger = logging.getLogger("event_bus")

# Track which bus_ids have already emitted the "no running loop" warning so
# that repeated calls only produce one log entry (rate-limited per bus).
_warned_buses: set[int] = set()


def invoke_sync_handlers(
    event_type: str,
    payload: Any,
    handlers: Iterable[Callable[[Any], None]],
) -> None:
    """Invoke each sync handler in iteration order.

    Exceptions are caught, logged with the offending event_type, and
    isolated — subsequent handlers still run.
    """
    for handler in handlers:
        try:
            handler(payload)
        except Exception:
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
