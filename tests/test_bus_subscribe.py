"""Tests for EventBus.subscribe and EventBus.unsubscribe — plan task A6."""

from __future__ import annotations

import pytest

from event_bus.bus import EventBus
from event_bus.subscription import Subscription


def test_subscribe_returns_subscription_handle() -> None:
    bus = EventBus(name="test")
    sub = bus.subscribe("foo", lambda p: None)
    assert isinstance(sub, Subscription)
    assert sub.event_type == "foo"
    assert sub.bus is bus
    assert sub.active is True


def test_subscribe_distinct_handles_for_duplicate_subscriptions() -> None:
    bus = EventBus()
    handler = lambda p: None  # noqa: E731
    s1 = bus.subscribe("foo", handler)
    s2 = bus.subscribe("foo", handler)
    assert s1 is not s2


def test_unsubscribe_by_handle() -> None:
    bus = EventBus()
    sub = bus.subscribe("foo", lambda p: None)
    bus.unsubscribe(sub)
    assert sub.active is False
    assert bus.get_subscribers("foo") == ()


def test_unsubscribe_idempotent() -> None:
    bus = EventBus()
    sub = bus.subscribe("foo", lambda p: None)
    bus.unsubscribe(sub)
    bus.unsubscribe(sub)  # no exception


def test_subscribe_non_callable_raises_typeerror() -> None:
    bus = EventBus()
    with pytest.raises(TypeError):
        bus.subscribe("foo", "not-a-callable")  # type: ignore[arg-type]


def test_get_subscribers_snapshot() -> None:
    bus = EventBus()
    s1 = bus.subscribe("foo", lambda p: None)
    s2 = bus.subscribe("foo", lambda p: None)
    snapshot = bus.get_subscribers("foo")
    assert isinstance(snapshot, tuple)
    assert set(snapshot) == {s1, s2}
