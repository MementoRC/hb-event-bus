"""Tests for event_bus.listener."""

from __future__ import annotations

from typing import Any

import pytest

from event_bus.bus import EventBus
from event_bus.listener import EventForwarder, EventListener


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


class TestEventForwarder:
    def test_calls_to_function_with_payload(self) -> None:
        captured: list[Any] = []
        fwd = EventForwarder(captured.append)
        fwd("payload")
        assert captured == ["payload"]

    def test_rejects_non_callable(self) -> None:
        with pytest.raises(TypeError, match="must be callable"):
            EventForwarder(None)  # type: ignore[arg-type]

    def test_inherits_eventlistener(self) -> None:
        fwd = EventForwarder(lambda _: None)
        assert isinstance(fwd, EventListener)

    def test_via_eventbus_publish_passes_payload(self) -> None:
        bus = EventBus(name="test")
        captured: list[Any] = []
        fwd = EventForwarder(captured.append)
        bus.subscribe("topic.x", fwd)
        bus.publish("topic.x", {"v": 1})
        assert captured == [{"v": 1}]

    def test_isolated_on_exception(self) -> None:
        bus = EventBus(name="test")
        captured: list[Any] = []

        def boom(_: Any) -> None:
            raise RuntimeError("boom")

        bus.subscribe("t", EventForwarder(boom))
        bus.subscribe("t", EventForwarder(captured.append))
        bus.publish("t", "payload")
        # Second handler still runs despite first raising
        assert captured == ["payload"]
