"""Tests for EventBus.apublish (async sequential dispatch) — plan task A8."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from event_bus.bus import EventBus


async def test_apublish_invokes_sync_handlers_inline() -> None:
    calls: list[tuple[str, object]] = []
    bus = EventBus()
    bus.subscribe("foo", lambda p: calls.append(("sync", p)))
    await bus.apublish("foo", "x")
    assert calls == [("sync", "x")]


async def test_apublish_awaits_async_handlers_sequentially() -> None:
    calls: list[tuple[str, object]] = []

    async def slow_a(p: object) -> None:
        await asyncio.sleep(0.01)
        calls.append(("a", p))

    async def slow_b(p: object) -> None:
        await asyncio.sleep(0.01)
        calls.append(("b", p))

    bus = EventBus()
    bus.subscribe("foo", slow_a)
    bus.subscribe("foo", slow_b)
    await bus.apublish("foo", "x")
    assert calls == [("a", "x"), ("b", "x")]


async def test_apublish_mixed_sync_async_in_registration_order() -> None:
    calls: list[tuple[str, object]] = []

    async def a(p: object) -> None:
        calls.append(("a", p))

    bus = EventBus()
    bus.subscribe("foo", a)
    bus.subscribe("foo", lambda p: calls.append(("b", p)))
    bus.subscribe("foo", a)
    await bus.apublish("foo", "x")
    assert calls == [("a", "x"), ("b", "x"), ("a", "x")]


async def test_apublish_exception_isolation(caplog: pytest.LogCaptureFixture) -> None:
    async def bad(p: object) -> None:
        raise RuntimeError("boom")

    called: list[object] = []

    async def good(p: object) -> None:
        called.append(p)

    bus = EventBus()
    bus.subscribe("foo", bad)
    bus.subscribe("foo", good)
    with caplog.at_level(logging.ERROR):
        await bus.apublish("foo", "x")
    assert called == ["x"]
    assert "boom" in caplog.text


async def test_apublish_unknown_event_no_op() -> None:
    bus = EventBus()
    await bus.apublish("nothing-listening", "payload")  # must not raise


async def test_apublish_empty_event_type_is_debug_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    bus = EventBus()
    with caplog.at_level(logging.DEBUG):
        await bus.apublish("", "x")
    assert "apublish" in caplog.text


async def test_apublish_sync_exception_isolation(caplog: pytest.LogCaptureFixture) -> None:
    """Sync handler exceptions are also isolated — subsequent handlers still run."""
    later_called: list[object] = []
    bus = EventBus()
    bus.subscribe("foo", lambda p: (_ for _ in ()).throw(RuntimeError("sync-boom")))
    bus.subscribe("foo", lambda p: later_called.append(p))
    with caplog.at_level(logging.ERROR):
        await bus.apublish("foo", "x")
    assert later_called == ["x"]
    assert "sync-boom" in caplog.text


@pytest.mark.asyncio
async def test_apublish_injects_event_info_before_each_await() -> None:
    from event_bus.bus import EventBus
    from event_bus.listener import EventListener

    seen: list[tuple[str, str, Any]] = []

    class AsyncSpy(EventListener):
        async def __call__(self, payload: Any) -> None:
            seen.append((self.current_event_type, self.current_event_bus.name, payload))  # type: ignore[union-attr]

    bus = EventBus(name="integration-async")
    bus.subscribe("t.A", AsyncSpy())
    bus.subscribe("t.B", AsyncSpy())
    await bus.apublish("t.A", "alpha")
    await bus.apublish("t.B", "beta")
    assert seen == [("t.A", "integration-async", "alpha"), ("t.B", "integration-async", "beta")]
