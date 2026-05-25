"""Tests for EventBus.publish (sync dispatch) — plan task A7."""

from __future__ import annotations

import logging

import pytest

from event_bus.bus import EventBus


def test_publish_invokes_subscribers_in_registration_order() -> None:
    calls: list[tuple[str, object]] = []
    bus = EventBus()
    bus.subscribe("foo", lambda p: calls.append(("a", p)))
    bus.subscribe("foo", lambda p: calls.append(("b", p)))
    bus.subscribe("foo", lambda p: calls.append(("c", p)))
    bus.publish("foo", "payload")
    assert calls == [("a", "payload"), ("b", "payload"), ("c", "payload")]


def test_publish_unknown_event_no_op() -> None:
    bus = EventBus()
    bus.publish("nothing-listening", "payload")  # must not raise


def test_publish_exception_isolation(caplog: pytest.LogCaptureFixture) -> None:
    bus = EventBus()
    bus.subscribe("foo", lambda p: (_ for _ in ()).throw(RuntimeError("boom")))
    later_called: list[object] = []
    bus.subscribe("foo", lambda p: later_called.append(p))
    with caplog.at_level(logging.ERROR):
        bus.publish("foo", "x")
    assert later_called == ["x"]
    assert "boom" in caplog.text


def test_publish_with_async_handler_no_loop_warns(caplog: pytest.LogCaptureFixture) -> None:
    async def h(p: object) -> None:
        pass

    bus = EventBus()
    bus.subscribe("foo", h)
    with caplog.at_level(logging.WARNING):
        bus.publish("foo", "x")
    assert "no running event loop" in caplog.text


def test_publish_with_empty_event_type_is_debug_logged(caplog: pytest.LogCaptureFixture) -> None:
    bus = EventBus()
    with caplog.at_level(logging.DEBUG):
        bus.publish("", "x")
    # debug log emitted; no exception
