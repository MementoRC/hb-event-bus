"""Contract rows relied on by hummingbot's ``EventBusPort``.

These tests mirror the rows that hummingbot's ``EventBusPort`` / event contract
suite (``test/hummingbot/contracts/test_event_port.py``) depends on. The port
uses only ``EventBus(name=...)``, ``subscribe``, ``unsubscribe`` and ``publish``.

Coordinate with the hummingbot orchestrator before changing any of these
behaviours.
"""

from __future__ import annotations

import gc
import weakref
from typing import TYPE_CHECKING, Any

from event_bus.bus import EventBus

if TYPE_CHECKING:
    from event_bus.subscription import Subscription


class _Handler:
    """Weakref-able callable handler."""

    def __call__(self, payload: Any) -> None:
        return None


class _Payload:
    """Weakref-able payload."""


def test_publish_delivers_synchronously() -> None:
    bus = EventBus(name="test")
    seen: list[object] = []
    bus.subscribe("1", seen.append)
    bus.publish("1", "x")
    assert seen == ["x"]


def test_topics_are_isolated() -> None:
    bus = EventBus(name="test")
    seen_1: list[object] = []
    seen_2: list[object] = []
    bus.subscribe("1", seen_1.append)
    bus.subscribe("2", seen_2.append)
    bus.publish("1", "a")
    bus.publish("2", "b")
    assert seen_1 == ["a"]
    assert seen_2 == ["b"]


def test_handler_added_during_dispatch_runs_only_on_next_publish() -> None:
    bus = EventBus(name="test")
    late_calls: list[object] = []

    def late(p: object) -> None:
        late_calls.append(p)

    def adder(_: object) -> None:
        bus.subscribe("1", late)

    bus.subscribe("1", adder)
    bus.publish("1", "first")
    assert late_calls == []
    bus.publish("1", "second")
    assert late_calls == ["second"]


def test_handler_unsubscribing_itself_runs_once() -> None:
    bus = EventBus(name="test")
    calls: list[object] = []
    handles: list[Subscription] = []

    def once(p: object) -> None:
        calls.append(p)
        bus.unsubscribe(handles[0])

    handles.append(bus.subscribe("1", once))
    bus.publish("1", "first")
    bus.publish("1", "second")
    assert calls == ["first"]


def test_publish_uses_snapshot_taken_at_publish_start() -> None:
    # Pinned deliberately: matches compiled PubSub, which copies its listener
    # set before dispatch, so a handler removed mid-dispatch still runs for the
    # in-flight publish.
    bus = EventBus(name="test")
    b_calls: list[object] = []
    handles: dict[str, Subscription] = {}

    def a(_: object) -> None:
        bus.unsubscribe(handles["b"])

    def b(p: object) -> None:
        b_calls.append(p)

    bus.subscribe("1", a)
    handles["b"] = bus.subscribe("1", b)
    bus.publish("1", "first")
    assert b_calls == ["first"]
    bus.publish("1", "second")
    assert b_calls == ["first"]


def test_unsubscribe_releases_handler() -> None:
    bus = EventBus(name="test")
    handler = _Handler()
    ref = weakref.ref(handler)
    handle = bus.subscribe("1", handler)
    bus.unsubscribe(handle)
    del handler, handle
    gc.collect()
    assert ref() is None


def test_publish_does_not_retain_payload() -> None:
    bus = EventBus(name="test")
    bus.subscribe("1", lambda _: None)
    payload = _Payload()
    ref = weakref.ref(payload)
    bus.publish("1", payload)
    del payload
    gc.collect()
    assert ref() is None


def test_constructor_accepts_name_keyword() -> None:
    assert EventBus(name="hummingbot").name == "hummingbot"
