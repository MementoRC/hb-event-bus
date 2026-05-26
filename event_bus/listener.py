"""Callable-object handlers with dispatch context (EventListener and forwarders)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from event_bus.bus import EventBus


class EventListener:
    """Base class for callable-object handlers that need dispatch context.

    Instances expose ``current_event_type`` and ``current_event_bus`` attributes
    that ``EventBus.publish`` / ``apublish`` populates before invoking ``__call__``.

    Subclass and override ``__call__``. Reading the context attributes outside
    ``__call__`` is undefined — they reflect the most recent dispatch and may be
    ``""`` / ``None`` before any dispatch occurs. Do not share a single instance
    across multiple buses or topics if you depend on the attrs being stable
    across awaits.
    """

    __slots__ = ("current_event_type", "current_event_bus")

    def __init__(self) -> None:
        self.current_event_type: str = ""
        self.current_event_bus: EventBus | None = None

    def __call__(self, payload: Any) -> None:
        raise NotImplementedError
