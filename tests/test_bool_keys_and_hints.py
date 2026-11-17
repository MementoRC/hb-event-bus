"""Tests: bool event keys are rejected; EventKey annotations resolve at runtime."""

from __future__ import annotations

import typing

import pytest

from event_bus import EventBus, EventKey, EventLogger, LoggedEvent, Subscription


def _noop(_payload: object) -> None:
    return None


def test_subscribe_rejects_bool() -> None:
    with pytest.raises(TypeError, match="not bool"):
        EventBus().subscribe(True, _noop)  # type: ignore[arg-type]


def test_publish_rejects_bool() -> None:
    with pytest.raises(TypeError, match="not bool"):
        EventBus().publish(False)  # type: ignore[arg-type]


async def test_apublish_rejects_bool() -> None:
    with pytest.raises(TypeError, match="not bool"):
        await EventBus().apublish(True)  # type: ignore[arg-type]


def test_get_subscribers_rejects_bool() -> None:
    with pytest.raises(TypeError, match="not bool"):
        EventBus().get_subscribers(True)  # type: ignore[arg-type]


async def test_wait_for_rejects_bool() -> None:
    with pytest.raises(TypeError, match="not bool"):
        await EventLogger().wait_for(True, timeout=0.01)  # type: ignore[arg-type]


def test_attach_rejects_bool() -> None:
    with pytest.raises(TypeError, match="not bool"):
        EventLogger().attach(EventBus(), True)  # type: ignore[arg-type]


def test_bool_publish_does_not_reach_int_subscriber() -> None:
    bus = EventBus()
    received: list[object] = []
    bus.subscribe(1, received.append)
    with pytest.raises(TypeError):
        bus.publish(True, "x")  # type: ignore[arg-type]
    assert received == []
    bus.publish(1, "y")
    assert received == ["y"]


@pytest.mark.parametrize(
    "target",
    [EventBus.publish, EventBus.subscribe, Subscription.__init__, LoggedEvent],
    ids=["publish", "subscribe", "Subscription.__init__", "LoggedEvent"],
)
def test_event_key_hint_resolves(target: object) -> None:
    hints = typing.get_type_hints(target)
    key = "name" if target is LoggedEvent else "event_type"
    assert hints[key] is EventKey
