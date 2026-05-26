"""Tests for event_bus._internal dispatch helpers (plan task A5)."""

import asyncio
import logging
from typing import Any

import pytest

from event_bus._internal import (
    _is_async_handler,
    _is_event_listener,
    invoke_sync_handlers,
    schedule_async_handler,
)
from event_bus.bus import EventBus


def test_invoke_sync_calls_all_handlers_in_order() -> None:
    calls: list[tuple[str, object]] = []
    handlers = [
        lambda p: calls.append(("a", p)),
        lambda p: calls.append(("b", p)),
        lambda p: calls.append(("c", p)),
    ]
    invoke_sync_handlers("evt", "payload", handlers, bus=EventBus(name="test"))
    assert calls == [("a", "payload"), ("b", "payload"), ("c", "payload")]


def test_invoke_sync_isolates_exceptions(caplog: pytest.LogCaptureFixture) -> None:
    def bad(_: object) -> None:
        raise RuntimeError("boom")

    def good(p: object) -> None:
        good.called = p  # type: ignore[attr-defined]

    good.called = None  # type: ignore[attr-defined]
    with caplog.at_level(logging.ERROR):
        invoke_sync_handlers("evt", "payload", [bad, good], bus=EventBus(name="test"))
    assert good.called == "payload"  # type: ignore[attr-defined]
    assert "boom" in caplog.text
    assert "evt" in caplog.text


def test_schedule_async_handler_no_running_loop_warns_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def h(_: object) -> None:
        pass

    with caplog.at_level(logging.WARNING):
        schedule_async_handler("evt", "payload", h, bus=EventBus(name="test"))
        schedule_async_handler("evt", "payload", h, bus=EventBus(name="test"))  # second: suppressed
    warnings = [r for r in caplog.records if "no running event loop" in r.message]
    assert len(warnings) == 1  # rate-limited


class TestIsAsyncHandler:
    def test_plain_function_returns_false(self) -> None:
        def f(p: Any) -> None: ...

        assert _is_async_handler(f) is False

    def test_plain_coroutine_function_returns_true(self) -> None:
        async def af(p: Any) -> None: ...

        assert _is_async_handler(af) is True

    def test_class_instance_with_async_call_returns_true(self) -> None:
        class AsyncCallable:
            async def __call__(self, p: Any) -> None: ...

        assert _is_async_handler(AsyncCallable()) is True

    def test_class_instance_with_sync_call_returns_false(self) -> None:
        class SyncCallable:
            def __call__(self, p: Any) -> None: ...

        assert _is_async_handler(SyncCallable()) is False


class TestIsEventListener:
    def test_returns_true_for_eventlistener_instance(self) -> None:
        from event_bus.listener import EventListener

        class L(EventListener):
            def __call__(self, p: Any) -> None: ...

        assert _is_event_listener(L()) is True

    def test_returns_false_for_plain_function(self) -> None:
        assert _is_event_listener(lambda p: None) is False

    def test_returns_false_for_arbitrary_object(self) -> None:
        assert _is_event_listener(object()) is False


class TestInvokeSyncHandlersInjection:
    def test_injects_event_listener_attrs_before_call(self) -> None:
        from event_bus._internal import invoke_sync_handlers
        from event_bus.bus import EventBus
        from event_bus.listener import EventListener

        captured: dict[str, Any] = {}

        class Spy(EventListener):
            def __call__(self, payload: Any) -> None:
                captured["topic"] = self.current_event_type
                captured["bus_name"] = self.current_event_bus.name  # type: ignore[union-attr]
                captured["payload"] = payload

        bus = EventBus(name="b1")
        invoke_sync_handlers("evt.x", "data", [Spy()], bus=bus)
        assert captured == {"topic": "evt.x", "bus_name": "b1", "payload": "data"}

    def test_warns_and_closes_coroutine_returned_from_sync_path(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from event_bus._internal import invoke_sync_handlers
        from event_bus.bus import EventBus

        async def returns_coro(_: Any) -> None: ...

        bus = EventBus(name="b1")
        with caplog.at_level(logging.WARNING, logger="event_bus"):
            invoke_sync_handlers("evt", "payload", [returns_coro], bus=bus)
        assert any("Sync dispatch received a coroutine" in rec.message for rec in caplog.records)


class TestScheduleAsyncHandlerInjection:
    @pytest.mark.asyncio
    async def test_injects_attrs_at_await_time_not_schedule_time(self) -> None:
        from event_bus._internal import schedule_async_handler
        from event_bus.bus import EventBus
        from event_bus.listener import EventListener

        captured: dict[str, Any] = {}
        ready = asyncio.Event()

        class AsyncSpy(EventListener):
            async def __call__(self, payload: Any) -> None:
                captured["topic"] = self.current_event_type
                captured["bus_name"] = self.current_event_bus.name  # type: ignore[union-attr]
                ready.set()

        bus = EventBus(name="async-b1")
        spy = AsyncSpy()
        schedule_async_handler("evt.async", "payload", spy, bus=bus)
        await asyncio.wait_for(ready.wait(), timeout=1.0)
        assert captured == {"topic": "evt.async", "bus_name": "async-b1"}
