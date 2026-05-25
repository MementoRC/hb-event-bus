"""Tests for :mod:`event_bus.event_logger`."""

from __future__ import annotations

import asyncio
import dataclasses

import pytest

from event_bus import EventBus, EventLogger, LoggedEvent


class TestEventLoggerBasics:
    def test_attach_requires_event_name(self) -> None:
        bus = EventBus()
        log = EventLogger()
        with pytest.raises(ValueError, match="at least one event name"):
            log.attach(bus)

    def test_captures_sync_published_event(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "tick")
        bus.publish("tick", {"v": 1})
        assert len(log.events) == 1
        assert log.events[0].name == "tick"
        assert log.events[0].payload == {"v": 1}
        assert log.events[0].timestamp > 0

    def test_captures_multiple_event_names(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "a", "b")
        bus.publish("a", 1)
        bus.publish("b", 2)
        bus.publish("a", 3)
        names = [e.name for e in log.events]
        assert names == ["a", "b", "a"]

    def test_clear_empties_event_log(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "x")
        bus.publish("x", 1)
        bus.publish("x", 2)
        log.clear()
        assert log.events == []

    def test_clear_preserves_subscriptions(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "x")
        bus.publish("x", 1)
        log.clear()
        bus.publish("x", 2)
        assert len(log.events) == 1
        assert log.events[0].payload == 2

    def test_maxlen_bounds_event_log(self) -> None:
        bus = EventBus()
        log = EventLogger(maxlen=3)
        log.attach(bus, "x")
        for i in range(5):
            bus.publish("x", i)
        assert [e.payload for e in log.events] == [2, 3, 4]

    def test_unbounded_maxlen(self) -> None:
        bus = EventBus()
        log = EventLogger(maxlen=None)
        log.attach(bus, "x")
        for i in range(100):
            bus.publish("x", i)
        assert len(log.events) == 100

    def test_detach_cancels_subscriptions(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "x")
        bus.publish("x", 1)
        log.detach()
        bus.publish("x", 2)
        assert len(log.events) == 1

    def test_detach_is_idempotent(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "x")
        log.detach()
        log.detach()  # must not raise


class TestEventLoggerWaitFor:
    async def test_wait_for_returns_payload(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "ready")

        async def fire() -> None:
            await asyncio.sleep(0.01)
            bus.publish("ready", "go")

        asyncio.create_task(fire())
        payload = await log.wait_for("ready", timeout=1.0)
        assert payload == "go"

    async def test_wait_for_times_out(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "never")
        with pytest.raises(TimeoutError):
            await log.wait_for("never", timeout=0.05)

    async def test_concurrent_waiters_both_resolve(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "ev")

        async def fire() -> None:
            await asyncio.sleep(0.01)
            bus.publish("ev", 42)

        asyncio.create_task(fire())
        results = await asyncio.gather(
            log.wait_for("ev", timeout=1.0),
            log.wait_for("ev", timeout=1.0),
        )
        assert results == [42, 42]

    async def test_wait_for_after_event_does_not_resolve(self) -> None:
        # wait_for only sees events that arrive AFTER the wait began
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "past")
        bus.publish("past", "already")
        with pytest.raises(TimeoutError):
            await log.wait_for("past", timeout=0.05)

    async def test_waiter_cleanup_on_timeout(self) -> None:
        bus = EventBus()
        log = EventLogger()
        log.attach(bus, "x")
        with pytest.raises(TimeoutError):
            await log.wait_for("x", timeout=0.05)
        # internal _waiters dict should be drained
        assert "x" not in log._waiters


class TestLoggedEventDataclass:
    def test_logged_event_is_frozen(self) -> None:
        ev = LoggedEvent(name="a", payload=1, timestamp=0.0)
        with pytest.raises(dataclasses.FrozenInstanceError):
            ev.name = "b"  # type: ignore[misc]

    def test_logged_event_uses_slots(self) -> None:
        # __slots__ presence on the class is the contract; we don't probe setattr
        # because frozen=True + slots=True on Python 3.12 raises TypeError from
        # FrozenInstanceError's super() chain, not AttributeError.
        assert hasattr(LoggedEvent, "__slots__")
        assert set(LoggedEvent.__slots__) == {"name", "payload", "timestamp"}
        # __dict__ should not exist on instances when __slots__ is defined
        ev = LoggedEvent(name="a", payload=1, timestamp=0.0)
        assert not hasattr(ev, "__dict__")
