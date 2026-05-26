"""Tests for event_bus._internal dispatch helpers (plan task A5)."""

import logging
from typing import Any

import pytest

from event_bus._internal import (
    _is_async_handler,
    _is_event_listener,
    invoke_sync_handlers,
    schedule_async_handler,
)


def test_invoke_sync_calls_all_handlers_in_order() -> None:
    calls: list[tuple[str, object]] = []
    handlers = [
        lambda p: calls.append(("a", p)),
        lambda p: calls.append(("b", p)),
        lambda p: calls.append(("c", p)),
    ]
    invoke_sync_handlers("evt", "payload", handlers)
    assert calls == [("a", "payload"), ("b", "payload"), ("c", "payload")]


def test_invoke_sync_isolates_exceptions(caplog: pytest.LogCaptureFixture) -> None:
    def bad(_: object) -> None:
        raise RuntimeError("boom")

    def good(p: object) -> None:
        good.called = p  # type: ignore[attr-defined]

    good.called = None  # type: ignore[attr-defined]
    with caplog.at_level(logging.ERROR):
        invoke_sync_handlers("evt", "payload", [bad, good])
    assert good.called == "payload"  # type: ignore[attr-defined]
    assert "boom" in caplog.text
    assert "evt" in caplog.text


def test_schedule_async_handler_no_running_loop_warns_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def h(_: object) -> None:
        pass

    with caplog.at_level(logging.WARNING):
        schedule_async_handler("evt", "payload", h)
        schedule_async_handler("evt", "payload", h)  # second call should be suppressed
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
