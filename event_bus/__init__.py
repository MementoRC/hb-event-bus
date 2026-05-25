"""Canonical event bus for the hummingbot ecosystem."""

from collections.abc import Awaitable, Callable
from typing import Any

from event_bus.__about__ import __version__
from event_bus.bus import EventBus
from event_bus.event_logger import EventLogger, LoggedEvent
from event_bus.subscription import Handler, Subscription

EventPayload = Any
SyncHandler = Callable[[EventPayload], None]
AsyncHandler = Callable[[EventPayload], Awaitable[None]]

__all__ = [
    "AsyncHandler",
    "EventBus",
    "EventLogger",
    "Handler",
    "LoggedEvent",
    "Subscription",
    "SyncHandler",
    "__version__",
]
