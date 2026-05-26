"""Tests for event_bus.listener."""

from __future__ import annotations

from typing import Any

import pytest

from event_bus.listener import EventListener


class TestEventListenerBase:
    def test_init_defaults(self) -> None:
        listener = EventListener()
        assert listener.current_event_type == ""
        assert listener.current_event_bus is None

    def test_call_raises_notimplemented(self) -> None:
        listener = EventListener()
        with pytest.raises(NotImplementedError):
            listener("any payload")

    def test_slots_prevent_arbitrary_attrs(self) -> None:
        listener = EventListener()
        with pytest.raises(AttributeError):
            listener.unexpected_attr = 42  # type: ignore[attr-defined]

    def test_subclass_can_override_call(self) -> None:
        captured: list[Any] = []

        class Capturing(EventListener):
            def __call__(self, payload: Any) -> None:
                captured.append(payload)

        c = Capturing()
        c("hello")
        assert captured == ["hello"]
