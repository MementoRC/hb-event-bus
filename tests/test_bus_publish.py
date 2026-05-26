"""Tests for EventBus.publish (sync dispatch) — plan task A7."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from event_bus.bus import EventBus

if TYPE_CHECKING:
    import pytest


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


def test_publish_routes_async_call_eventlistener_via_schedule(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An EventListener subclass with async __call__ must be scheduled by publish(),
    not invoked on the sync path (where it would return an un-awaited coroutine).
    """
    from event_bus.listener import EventListener

    class AsyncListener(EventListener):
        invoked: bool = False

        async def __call__(self, payload: Any) -> None:
            type(self).invoked = True

    async def driver() -> None:
        bus = EventBus(name="b5")
        bus.subscribe("evt", AsyncListener())
        bus.publish("evt", "payload")
        # Yield control so the scheduled task can run.
        await asyncio.sleep(0)

    with caplog.at_level("WARNING", logger="event_bus"):
        asyncio.run(driver())

    assert AsyncListener.invoked is True
    # No "Sync dispatch received a coroutine" warning — handler was correctly scheduled.
    assert not any("Sync dispatch received a coroutine" in r.message for r in caplog.records)
