"""Tests for the Subscription handle (plan task A4)."""

from event_bus.subscription import Subscription


def _noop(payload: object) -> None:
    pass


def test_subscription_has_event_type_and_handler() -> None:
    sub = Subscription(event_type="foo", handler=_noop, bus=object())
    assert sub.event_type == "foo"
    assert sub.handler is _noop
    assert sub.active is True


def test_subscription_cancel_makes_inactive() -> None:
    sub = Subscription(event_type="foo", handler=_noop, bus=object())
    sub.cancel()
    assert sub.active is False


def test_subscription_cancel_idempotent() -> None:
    sub = Subscription(event_type="foo", handler=_noop, bus=object())
    sub.cancel()
    sub.cancel()  # no exception
    assert sub.active is False


def test_subscription_equality_is_identity() -> None:
    bus = object()
    s1 = Subscription(event_type="foo", handler=_noop, bus=bus)
    s2 = Subscription(event_type="foo", handler=_noop, bus=bus)
    assert s1 != s2  # distinct handles
    assert s1 == s1
    assert hash(s1) != hash(s2)
