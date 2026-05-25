"""Tests for event_bus._internal dispatch helpers (plan task A5)."""

import logging

import pytest

from event_bus._internal import invoke_sync_handlers, schedule_async_handler


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
