# EventListener / EventForwarder / SourceInfoEventForwarder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three callable-object handler classes (`EventListener`, `EventForwarder`, `SourceInfoEventForwarder`) to `hb-event-bus`, with bus-level dispatch context injection, so hb-* sub-packages can adopt a shared listener/forwarder pattern in greenfield code.

**Architecture:** New module `event_bus/listener.py` holds the three classes. `event_bus/_internal.py` gains two helpers (`_is_async_handler` for callable-class async detection; `_is_event_listener` lazy-import probe to avoid circular dependency) and modifies its three dispatch functions (`invoke_sync_handlers`, `schedule_async_handler`, `_run_async_handler`) to accept a `bus` keyword and inject EventListener context attrs immediately before each handler call. `event_bus/bus.py`'s `publish()` and `apublish()` loops are replaced to pass `bus=self` through. Public API gains three additive exports.

**Tech Stack:** Python 3.12+, pixi-managed env, pytest + pytest-asyncio, ruff (lint+format), mypy. No new runtime dependencies.

**Reference spec:** `docs/superpowers/specs/2026-05-26-event-listener-forwarder-design.md`

**Branch:** `feat/event-listener-forwarder` (off `development`; already created)

**Issue:** [hb-event-bus#1](https://github.com/MementoRC/hb-event-bus/issues/1)

---

## PR Strategy

Four atomic PRs matching the Phase A pattern. Each PR is self-contained, passes the full test suite, and is mergeable without depending on later PRs.

| PR  | Branch                                       | Scope                                                    | Test delta |
|-----|----------------------------------------------|----------------------------------------------------------|------------|
| B1  | `feat/b1-listener-module`                    | `event_bus/listener.py` + unit tests for 3 classes       | +15        |
| B2  | `feat/b2-dispatch-wiring`                    | `_internal.py` helpers + sig changes + `bus.py` loops    | +12        |
| B3  | `feat/b3-listener-public-exports`            | `__init__.py` exports + smoke test                       | +1         |
| B4  | `docs/b4-listener-readme`                    | README listener/forwarder section                        | 0          |

Each PR branches off the previous one's merge into `development`. After each merge, the next branch is rebased onto fresh `development`.

---

## PR B1 — listener.py + unit tests

> All B1.x tasks (B1.1 through B1.3) commit on the existing `feat/event-listener-forwarder` branch (created off `development` during brainstorming). Task B1.4 renames/branches that work into `feat/b1-listener-module` for the PR. Subsequent PRs (B2, B3, B4) each start from a fresh branch off `development` after the previous PR merges.

### Task B1.1: Create `event_bus/listener.py` skeleton with EventListener base

**Files:**
- Create: `event_bus/listener.py`
- Create: `tests/test_listener.py`

- [ ] **Step 1: Write failing tests for EventListener base**

Create `tests/test_listener.py`:

```python
"""Tests for event_bus.listener."""

from __future__ import annotations

from typing import Any

import pytest

from event_bus.listener import EventListener


class TestEventListenerBase:
    def test_init_defaults(self) -> None:
        listener = EventListener()
        assert listener.current_event_type == ""
        assert listener.current_event_bus is None

    def test_call_raises_notimplemented(self) -> None:
        listener = EventListener()
        with pytest.raises(NotImplementedError):
            listener("any payload")

    def test_slots_prevent_arbitrary_attrs(self) -> None:
        listener = EventListener()
        with pytest.raises(AttributeError):
            listener.unexpected_attr = 42  # type: ignore[attr-defined]

    def test_subclass_can_override_call(self) -> None:
        captured: list[Any] = []

        class Capturing(EventListener):
            def __call__(self, payload: Any) -> None:
                captured.append(payload)

        c = Capturing()
        c("hello")
        assert captured == ["hello"]
```

- [ ] **Step 2: Run tests to verify they fail (module not found)**

Run: `pixi run pytest tests/test_listener.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'event_bus.listener'`

- [ ] **Step 3: Create `event_bus/listener.py` with EventListener base**

```python
"""Callable-object handlers with dispatch context (EventListener and forwarders)."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from event_bus.bus import EventBus


class EventListener:
    """Base class for callable-object handlers that need dispatch context.

    Instances expose ``current_event_type`` and ``current_event_bus`` attributes
    that ``EventBus.publish`` / ``apublish`` populates before invoking ``__call__``.

    Subclass and override ``__call__``. Reading the context attributes outside
    ``__call__`` is undefined — they reflect the most recent dispatch and may be
    ``""`` / ``None`` before any dispatch occurs. Do not share a single instance
    across multiple buses or topics if you depend on the attrs being stable
    across awaits.
    """

    __slots__ = ("current_event_type", "current_event_bus")

    def __init__(self) -> None:
        self.current_event_type: str = ""
        self.current_event_bus: EventBus | None = None

    def __call__(self, payload: Any) -> None:
        raise NotImplementedError
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_listener.py::TestEventListenerBase -v`
Expected: 4 passed

- [ ] **Step 5: Run lint + format**

Run: `pixi run lint` then `pixi run format --check`
Expected: 0 errors, 0 violations

- [ ] **Step 6: Commit**

```
git add event_bus/listener.py tests/test_listener.py
git commit -m "feat(listener): add EventListener base class

Pure Python base for callable-object handlers needing dispatch context.
Exposes current_event_type / current_event_bus attrs (default '' / None)
populated by EventBus dispatch in a later PR.

Refs: hb-event-bus#1"
```

---

### Task B1.2: Add EventForwarder to listener.py

**Files:**
- Modify: `event_bus/listener.py`
- Modify: `tests/test_listener.py`

- [ ] **Step 1: Write failing tests for EventForwarder**

Append to `tests/test_listener.py`:

```python
from event_bus.bus import EventBus
from event_bus.listener import EventForwarder


class TestEventForwarder:
    def test_calls_to_function_with_payload(self) -> None:
        captured: list[Any] = []
        fwd = EventForwarder(captured.append)
        fwd("payload")
        assert captured == ["payload"]

    def test_rejects_non_callable(self) -> None:
        with pytest.raises(TypeError, match="must be callable"):
            EventForwarder(None)  # type: ignore[arg-type]

    def test_inherits_eventlistener(self) -> None:
        fwd = EventForwarder(lambda _: None)
        assert isinstance(fwd, EventListener)

    def test_via_eventbus_publish_passes_payload(self) -> None:
        bus = EventBus(name="test")
        captured: list[Any] = []
        fwd = EventForwarder(captured.append)
        bus.subscribe("topic.x", fwd)
        bus.publish("topic.x", {"v": 1})
        assert captured == [{"v": 1}]

    def test_isolated_on_exception(self) -> None:
        bus = EventBus(name="test")
        captured: list[Any] = []

        def boom(_: Any) -> None:
            raise RuntimeError("boom")

        bus.subscribe("t", EventForwarder(boom))
        bus.subscribe("t", EventForwarder(captured.append))
        bus.publish("t", "payload")
        # Second handler still runs despite first raising
        assert captured == ["payload"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run pytest tests/test_listener.py::TestEventForwarder -v`
Expected: FAIL with `ImportError: cannot import name 'EventForwarder'`

- [ ] **Step 3: Add EventForwarder to listener.py**

Append to `event_bus/listener.py`:

```python


class EventForwarder(EventListener):
    """Adapter that forwards the payload to a plain callable."""

    __slots__ = ("_to_function",)

    def __init__(self, to_function: Callable[[Any], None]) -> None:
        if not callable(to_function):
            raise TypeError(f"to_function must be callable, got {type(to_function).__name__!r}")
        super().__init__()
        self._to_function = to_function

    def __call__(self, payload: Any) -> None:
        self._to_function(payload)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_listener.py -v`
Expected: 9 passed (4 base + 5 forwarder)

- [ ] **Step 5: Run lint + format**

Run: `pixi run lint && pixi run format --check`
Expected: clean

- [ ] **Step 6: Commit**

```
git add event_bus/listener.py tests/test_listener.py
git commit -m "feat(listener): add EventForwarder adapter

Wraps Callable[[Any], None] as an EventListener subclass.
Constructor raises TypeError on non-callable input.

Refs: hb-event-bus#1"
```

---

### Task B1.3: Add SourceInfoEventForwarder to listener.py

**Files:**
- Modify: `event_bus/listener.py`
- Modify: `tests/test_listener.py`

- [ ] **Step 1: Write failing tests for SourceInfoEventForwarder**

Append to `tests/test_listener.py`:

```python
from event_bus.listener import SourceInfoEventForwarder


class TestSourceInfoEventForwarder:
    def test_receives_topic_bus_payload_when_attrs_set_manually(self) -> None:
        # Unit-level: simulate dispatch by setting attrs by hand,
        # then call. Integration through bus.publish is covered in PR B2.
        captured: list[tuple[str, Any, Any]] = []

        def receiver(topic: str, bus: Any, payload: Any) -> None:
            captured.append((topic, bus, payload))

        fwd = SourceInfoEventForwarder(receiver)
        fwd.current_event_type = "order.filled"
        fwd.current_event_bus = "sentinel-bus"  # type: ignore[assignment]
        fwd({"order_id": 1})
        assert captured == [("order.filled", "sentinel-bus", {"order_id": 1})]

    def test_rejects_non_callable(self) -> None:
        with pytest.raises(TypeError, match="must be callable"):
            SourceInfoEventForwarder(42)  # type: ignore[arg-type]

    def test_inherits_eventlistener(self) -> None:
        fwd = SourceInfoEventForwarder(lambda t, b, p: None)
        assert isinstance(fwd, EventListener)

    def test_attrs_default_to_empty_and_none(self) -> None:
        fwd = SourceInfoEventForwarder(lambda t, b, p: None)
        assert fwd.current_event_type == ""
        assert fwd.current_event_bus is None

    def test_payload_threaded_through_call(self) -> None:
        captured: list[Any] = []
        fwd = SourceInfoEventForwarder(lambda t, b, p: captured.append(p))
        fwd.current_event_type = "x"
        fwd("data")
        assert captured == ["data"]

    def test_topic_passed_correctly(self) -> None:
        topics: list[str] = []
        fwd = SourceInfoEventForwarder(lambda t, b, p: topics.append(t))
        fwd.current_event_type = "topic.A"
        fwd(None)
        fwd.current_event_type = "topic.B"
        fwd(None)
        assert topics == ["topic.A", "topic.B"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run pytest tests/test_listener.py::TestSourceInfoEventForwarder -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add SourceInfoEventForwarder to listener.py**

Append to `event_bus/listener.py`:

```python


class SourceInfoEventForwarder(EventListener):
    """Adapter that forwards (topic, bus, payload) to a 3-arg callable."""

    __slots__ = ("_to_function",)

    def __init__(self, to_function: Callable[[str, "EventBus | None", Any], None]) -> None:
        if not callable(to_function):
            raise TypeError(f"to_function must be callable, got {type(to_function).__name__!r}")
        super().__init__()
        self._to_function = to_function

    def __call__(self, payload: Any) -> None:
        self._to_function(self.current_event_type, self.current_event_bus, payload)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_listener.py -v`
Expected: 15 passed (4 + 5 + 6)

- [ ] **Step 5: Full test suite must still pass**

Run: `pixi run test`
Expected: 44 existing + 15 new = 59 passed

- [ ] **Step 6: Run lint + format + typecheck**

Run: `pixi run lint && pixi run format --check && pixi run typecheck`
Expected: clean

- [ ] **Step 7: Commit**

```
git add event_bus/listener.py tests/test_listener.py
git commit -m "feat(listener): add SourceInfoEventForwarder adapter

Wraps Callable[[str, EventBus | None, Any], None] as an EventListener
subclass. Reads current_event_type and current_event_bus attrs (populated
by bus dispatch in PR B2) and threads them into the forwarded call.

Closes part of: hb-event-bus#1

Refs: hb-event-bus#1"
```

---

### Task B1.4: Push branch and open PR B1

- [ ] **Step 1: Verify branch state**

Run: `git log --oneline development..HEAD`
Expected: 3 commits (B1.1, B1.2, B1.3) ahead of `development`

- [ ] **Step 2: Create branch and push**

```
git checkout -b feat/b1-listener-module
git push -u origin feat/b1-listener-module
```

- [ ] **Step 3: Open PR via gh**

```
gh pr create --base development --title "feat(listener): EventListener + EventForwarder + SourceInfoEventForwarder" --body "$(cat <<'EOF'
## Summary

Adds three callable-object handler classes to `hb-event-bus`:
- `EventListener` — base class exposing `current_event_type` / `current_event_bus` attrs
- `EventForwarder(fn)` — wraps `Callable[[Any], None]`
- `SourceInfoEventForwarder(fn)` — wraps `Callable[[str, EventBus | None, Any], None]`

Module is fully unit-tested (15 new tests). Bus-level dispatch context injection lands in PR B2; public API exports land in PR B3.

## Spec

See `docs/superpowers/specs/2026-05-26-event-listener-forwarder-design.md`.

## Test plan

- [x] `pixi run test` — 59 passed (44 existing + 15 new)
- [x] `pixi run lint` — clean
- [x] `pixi run format --check` — clean
- [x] `pixi run typecheck` — clean

Refs: hb-event-bus#1
EOF
)"
```

- [ ] **Step 4: Verify CI passes**

Monitor: `gh pr checks <pr-number>` until all checks green.

---

## PR B2 — dispatch wiring (helpers + signature changes + bus loops)

### Task B2.1: Add `_is_async_handler` helper to `_internal.py`

**Files:**
- Modify: `event_bus/_internal.py`
- Modify: `tests/test_internal.py`

- [ ] **Step 1: Write failing tests for `_is_async_handler`**

Append to `tests/test_internal.py`:

```python
from event_bus._internal import _is_async_handler


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run pytest tests/test_internal.py::TestIsAsyncHandler -v`
Expected: FAIL with `ImportError: cannot import name '_is_async_handler'`

- [ ] **Step 3: Add `import inspect` and `_is_async_handler` to `_internal.py`**

Modify `event_bus/_internal.py`:

- Top imports: ensure `import inspect` exists alongside `import asyncio`, `import logging`
- Below the existing `_warned_buses` line, add:

```python


def _is_async_handler(handler: Any) -> bool:
    """True if handler should dispatch via the async (await) path.

    Returns True when:
    - handler is a coroutine function (``async def f(...)``), OR
    - handler is a class instance whose ``__call__`` is a coroutine function.

    The two branches do not overlap in practice: a plain coroutine function
    returns True at the first check; a class instance returns False there
    and falls through to probe ``__call__``.
    """
    if inspect.iscoroutinefunction(handler):
        return True
    call = getattr(handler, "__call__", None)
    if call is None:
        return False
    return inspect.iscoroutinefunction(call)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_internal.py::TestIsAsyncHandler -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```
git add event_bus/_internal.py tests/test_internal.py
git commit -m "feat(internal): add _is_async_handler for callable-class detection

inspect.iscoroutinefunction(instance) returns False even when __call__ is
async def. The helper probes both the handler directly and its __call__
attribute, enabling dispatch routing for async EventListener subclasses.

Refs: hb-event-bus#1"
```

---

### Task B2.2: Add `_is_event_listener` lazy-import helper

**Files:**
- Modify: `event_bus/_internal.py`
- Modify: `tests/test_internal.py`

- [ ] **Step 1: Write failing test for `_is_event_listener`**

Append to `tests/test_internal.py`:

```python
from event_bus._internal import _is_event_listener
from event_bus.listener import EventListener


class TestIsEventListener:
    def test_returns_true_for_eventlistener_instance(self) -> None:
        class L(EventListener):
            def __call__(self, p: Any) -> None: ...
        assert _is_event_listener(L()) is True

    def test_returns_false_for_plain_function(self) -> None:
        assert _is_event_listener(lambda p: None) is False

    def test_returns_false_for_arbitrary_object(self) -> None:
        assert _is_event_listener(object()) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run pytest tests/test_internal.py::TestIsEventListener -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add `_is_event_listener` to `_internal.py`**

Append below `_is_async_handler` in `event_bus/_internal.py`:

```python


def _is_event_listener(obj: Any) -> bool:
    """Lazy isinstance check for EventListener; avoids circular import.

    listener.py and _internal.py are both imported by bus.py; importing
    EventListener at module scope here would create a cycle. The local
    import is cached by Python's module system after the first call.
    """
    from event_bus.listener import EventListener
    return isinstance(obj, EventListener)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_internal.py::TestIsEventListener -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```
git add event_bus/_internal.py tests/test_internal.py
git commit -m "feat(internal): add _is_event_listener lazy-import helper

Avoids circular import between _internal.py and listener.py when bus.py
imports both. Local import is cached after first call.

Refs: hb-event-bus#1"
```

---

### Task B2.3: Modify `invoke_sync_handlers` — bus kwarg + EventListener injection + warn-and-close

**Files:**
- Modify: `event_bus/_internal.py`
- Modify: `tests/test_internal.py`

- [ ] **Step 1: Write failing tests for the new behavior**

Append to `tests/test_internal.py`:

```python
import logging
from typing import Any


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
        # Calling returns_coro(payload) returns a coroutine — simulate the case
        # where an async function reached the sync path without _is_async_handler
        # filtering it out.

        bus = EventBus(name="b1")
        with caplog.at_level(logging.WARNING, logger="event_bus"):
            invoke_sync_handlers("evt", "payload", [returns_coro], bus=bus)
        assert any("Sync dispatch received a coroutine" in rec.message for rec in caplog.records)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run pytest tests/test_internal.py::TestInvokeSyncHandlersInjection -v`
Expected: FAIL — `invoke_sync_handlers` does not accept `bus` kwarg yet

- [ ] **Step 3: Modify `invoke_sync_handlers` in `_internal.py`**

Add to TYPE_CHECKING block: `from event_bus.bus import EventBus`

Replace the existing `invoke_sync_handlers` function (entire definition) with:

```python
def invoke_sync_handlers(
    event_type: str,
    payload: Any,
    handlers: Iterable[Callable[[Any], None]],
    *,
    bus: EventBus,
) -> None:
    """Invoke each sync handler in iteration order with EventListener context injection.

    For EventListener instances, ``current_event_type`` and ``current_event_bus``
    are set immediately before the call. Exceptions are caught, logged, and
    isolated — subsequent handlers still run. Coroutines accidentally returned
    from a sync dispatch path are closed and a WARNING is emitted.
    """
    for handler in handlers:
        if _is_event_listener(handler):
            handler.current_event_type = event_type  # type: ignore[union-attr]
            handler.current_event_bus = bus  # type: ignore[union-attr]
        try:
            result = handler(payload)
            if inspect.iscoroutine(result):
                result.close()
                logger.warning(
                    "Sync dispatch received a coroutine from %r on topic=%r; "
                    "use apublish() or expose async __call__ via a real method.",
                    handler, event_type,
                )
        except Exception:  # noqa: BLE001
            logger.exception("handler failed on event_type=%r", event_type)
```

- [ ] **Step 3b: Update existing `invoke_sync_handlers` call sites in `tests/test_internal.py`**

The existing test file (pre-this-PR) has direct calls to `invoke_sync_handlers(event_type, payload, handlers)` without the new `bus` kwarg. Find them with:

```
grep -n "invoke_sync_handlers(" tests/test_internal.py
```

For each match in the existing test functions (NOT in the new tests added in Step 1), append `, bus=EventBus(name="test")` as a keyword argument. Add `from event_bus.bus import EventBus` at the top of `tests/test_internal.py` if not already imported.

- [ ] **Step 4: bus.py call site MUST be updated in this same task to keep tests green**

In `event_bus/bus.py` `publish()`, find the call:
```python
invoke_sync_handlers(event_type, payload, sync_handlers)
```
Replace with:
```python
invoke_sync_handlers(event_type, payload, sync_handlers, bus=self)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pixi run pytest tests/test_internal.py::TestInvokeSyncHandlersInjection -v`
Expected: 2 passed.

Also: `pixi run test` — full suite must still pass.

- [ ] **Step 6: Commit**

```
git add event_bus/_internal.py event_bus/bus.py tests/test_internal.py
git commit -m "feat(internal): invoke_sync_handlers takes bus kwarg + injects EventListener attrs

Signature gains keyword-only bus: EventBus. EventListener instances have
current_event_type / current_event_bus set immediately before each call,
preserving the 'attrs valid inside __call__' invariant. Coroutines
accidentally returned from the sync path are closed and a WARNING is
logged instead of being silently dropped.

bus.py publish() updated to pass bus=self.

Refs: hb-event-bus#1"
```

---

### Task B2.4: Modify `schedule_async_handler` + `_run_async_handler` — bus kwarg + injection at await time

**Files:**
- Modify: `event_bus/_internal.py`
- Modify: `tests/test_internal.py`

- [ ] **Step 1: Write failing test for async-injection timing**

Append to `tests/test_internal.py`:

```python
import asyncio


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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_internal.py::TestScheduleAsyncHandlerInjection -v`
Expected: FAIL — `schedule_async_handler` does not accept `bus` kwarg yet

- [ ] **Step 3: Modify `schedule_async_handler` and `_run_async_handler`**

Replace the existing `schedule_async_handler` function entirely with:

```python
def schedule_async_handler(
    event_type: str,
    payload: Any,
    handler: Callable[[Any], Awaitable[None]],
    *,
    bus: EventBus,
) -> None:
    """Schedule *handler* on the running event loop via :func:`asyncio.ensure_future`.

    If there is no running loop the call is a no-op; a single WARNING is
    emitted per bus (rate-limited via id(bus) to avoid log flooding).

    The scheduled coroutine (``_run_async_handler``) sets EventListener
    context attrs at await time (not at schedule time) so attrs are correct
    even if multiple publish() calls schedule the same listener concurrently.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        bus_id = id(bus)
        if bus_id not in _warned_buses:
            _warned_buses.add(bus_id)
            logger.warning(
                "no running event loop; async handler for event_type=%r skipped",
                event_type,
            )
        return
    asyncio.ensure_future(_run_async_handler(event_type, payload, handler, bus), loop=loop)
```

Replace the existing `_run_async_handler` function entirely with:

```python
async def _run_async_handler(
    event_type: str,
    payload: Any,
    handler: Callable[[Any], Awaitable[None]],
    bus: EventBus,
) -> None:
    """Inject EventListener context attrs, await handler, isolate any exception."""
    if _is_event_listener(handler):
        handler.current_event_type = event_type  # type: ignore[union-attr]
        handler.current_event_bus = bus  # type: ignore[union-attr]
    try:
        await handler(payload)
    except Exception:  # noqa: BLE001
        logger.exception("async handler failed on event_type=%r", event_type)
```

- [ ] **Step 3b: Update existing `schedule_async_handler` call sites in `tests/test_internal.py`**

The existing test file has direct calls to `schedule_async_handler(event_type, payload, handler)` (or with `bus_id=...`) without the new `bus` kwarg. Find them with:

```
grep -n "schedule_async_handler(" tests/test_internal.py
```

For each match in EXISTING test functions (NOT the new test added in Step 1), append `, bus=EventBus(name="test")` as a keyword argument (replacing the old `bus_id=...` kwarg if present). The existing `test_schedule_async_handler_no_running_loop_warns_once` test has two such call sites. Ensure `from event_bus.bus import EventBus` is imported at the top of the test file.

- [ ] **Step 4: Update bus.py `publish()` call site to pass `bus=self`**

In `event_bus/bus.py` `publish()`, find:
```python
schedule_async_handler(event_type, payload, sub.handler, bus_id=id(self))
```
Replace with:
```python
schedule_async_handler(event_type, payload, sub.handler, bus=self)
```

(B2.5 will introduce a `handler = sub.handler` local variable when the for-loop is rewritten wholesale. For now keep `sub.handler` inline to minimize the B2.4 diff.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `pixi run pytest tests/test_internal.py::TestScheduleAsyncHandlerInjection -v`
Expected: 1 passed

Also: `pixi run test`
Expected: full suite green.

- [ ] **Step 6: Commit**

```
git add event_bus/_internal.py event_bus/bus.py tests/test_internal.py
git commit -m "feat(internal): schedule_async_handler injects EventListener attrs at await time

Replaces bus_id: int kwarg with bus: EventBus. Rate-limited 'no running
loop' warning is preserved (keyed by id(bus)). _run_async_handler sets
EventListener context attrs immediately before the await, not at schedule
time — closes a race when multiple publish() calls schedule the same
listener concurrently.

bus.py publish() updated to pass bus=self.

Refs: hb-event-bus#1"
```

---

### Task B2.5: Replace bus.py `publish()` for-loop to use `_is_async_handler`

**Files:**
- Modify: `event_bus/bus.py`
- Modify: `tests/test_bus_publish.py`

The existing publish() for-loop classifies handlers via inline `inspect.iscoroutinefunction(sub.handler)`. This returns False for class instances with async `__call__`, causing such handlers to be routed to the sync path (where they'd return an un-awaited coroutine). B2.5 fixes this by replacing the classifier with `_is_async_handler` and adopting a `handler = sub.handler` local for readability.

- [ ] **Step 1: Write failing test — async-call EventListener via publish() must be scheduled, not sync-called**

Append to `tests/test_bus_publish.py`:

```python
import asyncio


def test_publish_routes_async_call_eventlistener_via_schedule(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An EventListener subclass with async __call__ must be scheduled by publish(),
    not invoked on the sync path (where it would return an un-awaited coroutine).
    """
    from event_bus.bus import EventBus
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_bus_publish.py::test_publish_routes_async_call_eventlistener_via_schedule -v`
Expected: FAIL — the existing `inspect.iscoroutinefunction(sub.handler)` returns False for the AsyncListener instance; the handler is routed to sync, returns a coroutine, and `invoke_sync_handlers` warn-and-closes it (so `AsyncListener.invoked` stays False).

- [ ] **Step 3: Replace publish() for-loop and update imports in bus.py**

Top of `event_bus/bus.py`, modify imports:

Remove (if no other use remains):
```python
import inspect
```

Add (or extend the existing `from event_bus._internal import ...` line):
```python
from event_bus._internal import _is_async_handler, invoke_sync_handlers, schedule_async_handler
```

(`invoke_sync_handlers` and `schedule_async_handler` are already imported — confirm and add `_is_async_handler` to the same import. Do NOT add `EventListener` here; B2.6 needs it for apublish() and will add it then.)

In `publish()`, replace the entire for-loop section (from `for sub in subs:` through `invoke_sync_handlers(...)`) with:

```python
        for sub in subs:
            handler = sub.handler
            if _is_async_handler(handler):
                schedule_async_handler(event_type, payload, handler, bus=self)
            else:
                sync_handlers.append(handler)  # type: ignore[arg-type]
        invoke_sync_handlers(event_type, payload, sync_handlers, bus=self)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_bus_publish.py -v`
Expected: all existing tests + the new async-routing test all pass.

- [ ] **Step 5: Commit**

```
git add event_bus/bus.py tests/test_bus_publish.py
git commit -m "feat(bus): publish() uses _is_async_handler for callable-class routing

Replaces the inline inspect.iscoroutinefunction(sub.handler) classifier
with _is_async_handler(handler), which correctly routes EventListener
subclasses whose __call__ is async def. Inline 'inspect' import removed
from bus.py as it is no longer used directly.

Refs: hb-event-bus#1"
```

---

### Task B2.6: Replace bus.py `apublish()` for-loop with EventListener injection

**Files:**
- Modify: `event_bus/bus.py`
- Modify: `tests/test_bus_apublish.py`

- [ ] **Step 1: Write integration test**

Append to `tests/test_bus_apublish.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_bus_apublish.py::test_apublish_injects_event_info_before_each_await -v`
Expected: FAIL — apublish() does not yet do EventListener injection nor use `_is_async_handler`.

- [ ] **Step 3: Add EventListener import + replace apublish() for-loop in bus.py**

Top of `event_bus/bus.py`, add:
```python
from event_bus.listener import EventListener
```

In `event_bus/bus.py` `apublish()`, replace the entire for-loop (from `for sub in subs:` through the end of the loop body) with:

```python
        for sub in subs:
            handler = sub.handler
            if isinstance(handler, EventListener):
                handler.current_event_type = event_type
                handler.current_event_bus = self
            try:
                if _is_async_handler(handler):
                    await handler(payload)
                else:
                    handler(payload)
            except Exception:  # noqa: BLE001
                logger.exception("handler failed on event_type=%r", event_type)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_bus_apublish.py -v`
Expected: all existing + new test pass.

Also: `pixi run test`
Expected: full suite green. PR B2 adds 12 new tests across test_internal.py (4 + 3 + 2 + 1 = 10), test_bus_publish.py (+1), and test_bus_apublish.py (+1). Total after B2: 59 + 12 = 71 expected.

- [ ] **Step 5: Run full quality gates**

Run: `pixi run lint && pixi run format --check && pixi run typecheck`
Expected: clean

- [ ] **Step 6: Commit**

```
git add event_bus/bus.py tests/test_bus_apublish.py
git commit -m "feat(bus): apublish() injects EventListener attrs + uses _is_async_handler

apublish() now (1) routes via _is_async_handler so callable-class async
listeners are awaited correctly, and (2) sets EventListener
current_event_type / current_event_bus immediately before each handler
invocation. Same dispatch-context contract as publish().

Refs: hb-event-bus#1"
```

---

### Task B2.7: Push branch and open PR B2

- [ ] **Step 1: Create branch and push**

```
git checkout -b feat/b2-dispatch-wiring
git push -u origin feat/b2-dispatch-wiring
```

- [ ] **Step 2: Open PR**

```
gh pr create --base development --title "feat: dispatch wiring — EventListener context injection + async-callable routing" --body "$(cat <<'EOF'
## Summary

Wires EventListener context injection into `EventBus.publish` / `apublish`.

- `_internal.py`: two new helpers (`_is_async_handler`, `_is_event_listener`). Three function signature changes (`invoke_sync_handlers`, `schedule_async_handler`, `_run_async_handler`) now take a `bus: EventBus` keyword arg.
- Injection happens at the per-call site (not the outer loop) so async-scheduled handlers see fresh context at await time, not schedule time.
- Coroutines accidentally returned from a sync dispatch path are closed and a WARNING is logged.
- `bus.py` `publish()` / `apublish()` for-loops replaced wholesale.
- Rate-limited 'no running loop' warning preserved, now keyed by `id(bus)`.

Depends on: PR B1 (listener module).

## Spec

See `docs/superpowers/specs/2026-05-26-event-listener-forwarder-design.md`.

## Test plan

- [x] `pixi run test` — full suite green (71 tests: 44 baseline + 15 from B1 + 12 from B2)
- [x] `pixi run lint` — clean
- [x] `pixi run format --check` — clean
- [x] `pixi run typecheck` — clean

Refs: hb-event-bus#1
EOF
)"
```

- [ ] **Step 3: Wait for CI green, then merge**

---

## PR B3 — public exports

### Task B3.1: Update `__init__.py` and add smoke test

**Files:**
- Modify: `event_bus/__init__.py`
- Modify: `tests/test_imports.py`

- [ ] **Step 1: Write failing smoke test**

Append to `tests/test_imports.py`:

```python
def test_listener_classes_exported_via_public_api() -> None:
    import event_bus
    assert hasattr(event_bus, "EventListener")
    assert hasattr(event_bus, "EventForwarder")
    assert hasattr(event_bus, "SourceInfoEventForwarder")
    assert "EventListener" in event_bus.__all__
    assert "EventForwarder" in event_bus.__all__
    assert "SourceInfoEventForwarder" in event_bus.__all__
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_imports.py::test_listener_classes_exported_via_public_api -v`
Expected: FAIL — symbols not in `event_bus.__all__`

- [ ] **Step 3: Update `event_bus/__init__.py`**

Add import:
```python
from event_bus.listener import EventForwarder, EventListener, SourceInfoEventForwarder
```

Extend `__all__` (keep alphabetical):
```python
__all__ = [
    "AsyncHandler",
    "EventBus",
    "EventForwarder",
    "EventListener",
    "EventLogger",
    "Handler",
    "LoggedEvent",
    "SourceInfoEventForwarder",
    "Subscription",
    "SyncHandler",
    "__version__",
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_imports.py -v`
Expected: all pass.

Also: `pixi run test`
Expected: full suite green.

- [ ] **Step 5: Commit**

```
git add event_bus/__init__.py tests/test_imports.py
git commit -m "feat(event-bus): export EventListener/EventForwarder/SourceInfoEventForwarder

Phase A's frozen public API gains three additive exports. No existing
signatures change.

Refs: hb-event-bus#1"
```

- [ ] **Step 6: Push and open PR**

```
git checkout -b feat/b3-listener-public-exports
git push -u origin feat/b3-listener-public-exports
gh pr create --base development --title "feat(event-bus): export EventListener + forwarders via public API" --body "$(cat <<'EOF'
## Summary

Additive exports: EventListener, EventForwarder, SourceInfoEventForwarder.

Depends on: PR B1, PR B2.

## Test plan

- [x] `pixi run test` — green
- [x] Smoke test verifies all three classes accessible via `event_bus.X`

Refs: hb-event-bus#1
EOF
)"
```

---

## PR B4 — README

### Task B4.1: Add listener/forwarder section to README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a new section to README.md**

After the existing "## API" section (or wherever class references live), insert the following markdown content (note: the outer fence below is a quoting fence for this plan only; in the README itself, use the inner content directly without any wrapping fence):

````
## Callable-Object Handlers: EventListener & Forwarders

For consumers that need topic/bus context inside their handler (vs. just receiving the payload), hb-event-bus ships three classes:

```python
from event_bus import EventBus, EventForwarder, EventListener, SourceInfoEventForwarder

bus = EventBus(name="trading")

# Plain forwarder — wraps a one-arg callable, gets payload only.
def on_fill(payload):
    print("filled:", payload)

bus.subscribe("order.filled", EventForwarder(on_fill))

# Source-info forwarder — wraps a three-arg callable, receives
# (topic, bus, payload).
def on_fill_with_context(topic: str, source_bus: EventBus, payload):
    print(f"[{source_bus.name}] {topic}: {payload}")

bus.subscribe("order.filled", SourceInfoEventForwarder(on_fill_with_context))

# Subclassing EventListener directly — for state-bearing handlers.
class OrderTracker(EventListener):
    def __init__(self):
        super().__init__()
        self.fills = []
    def __call__(self, payload):
        self.fills.append((self.current_event_type, payload))

bus.subscribe("order.filled", OrderTracker())
```

The bus sets `current_event_type` and `current_event_bus` on each EventListener instance immediately before invoking it. Inside `__call__`, the attrs are valid; reading them outside `__call__` is undefined.

EventListener subscriptions are held by **strong reference** (consistent with the rest of hb-event-bus); call `subscription.cancel()` to release.
````

(Continues with the rest of the README content — see the original code blocks below for the bus/forwarder/tracker example. Use single triple-backticks `python` for the example code block in the actual README; the four-backtick fence here is only to avoid nesting issues in the plan document itself.)
````

The body content of the README section (to paste between the `## Callable-Object Handlers` heading and the `## Status` update) is shown in the next code block. Triple-backtick `python` fences inside the README are unchanged; only this plan document uses four-backtick quoting fences to avoid renderer confusion.

Update the "## Status" section to note the addition:

```markdown
## Status

**Phase A Complete** — Public API (`EventBus`, `Subscription`, `Handler`, `AsyncHandler`, `EventListener`, `EventForwarder`, `SourceInfoEventForwarder`) is stable and exported via `event_bus/__init__.py`. Feature set and design are frozen for Phase B (Hummingbot integration).
```

- [ ] **Step 2: Run full test suite as smoke**

Run: `pixi run test`
Expected: green.

- [ ] **Step 3: Commit**

```
git add README.md
git commit -m "docs: README section for EventListener and forwarders

Documents the three callable-object handler classes added in PRs B1-B3:
- EventForwarder for single-arg callables
- SourceInfoEventForwarder for (topic, bus, payload) callables
- EventListener as a state-bearing handler base class

Updates Status section to list the additive public API.

Closes: hb-event-bus#1 (for this repo's Phase A scope)

Refs: hb-event-bus#1"
```

- [ ] **Step 4: Push and open PR**

```
git checkout -b docs/b4-listener-readme
git push -u origin docs/b4-listener-readme
gh pr create --base development --title "docs: README section for EventListener + forwarders" --body "$(cat <<'EOF'
## Summary

Documents the EventListener / EventForwarder / SourceInfoEventForwarder pattern with usage examples. Updates the Status section.

Depends on: PR B3 (public exports).

Closes: hb-event-bus#1 (for this repo's Phase A scope — Phase B/C still live in consumer repos).

Refs: hb-event-bus#1
EOF
)"
```

---

## Definition of Done

- [ ] PR B1 merged: 15 new tests, `listener.py` exists with three classes
- [ ] PR B2 merged: 6 new tests, dispatch helpers + bus loops wired to inject EventListener context
- [ ] PR B3 merged: public API exports three new classes
- [ ] PR B4 merged: README documents the additions
- [ ] Issue hb-event-bus#1 updated: Phase A complete with listener/forwarder additions; Phase B (hummingbot integration) and Phase C (sub-package harmonization) remain open in consumer repos

## Risk Register

| Risk | Mitigation |
|------|------------|
| `inspect.iscoroutinefunction` on bound method semantics across Python 3.12 patch versions | Tested directly in B2.1; pin behavior via unit tests |
| Circular import bug when `_internal.py` imports `EventListener` eagerly | Lazy local import in `_is_event_listener`; tested in B2.2 |
| `_warned_buses` set grows unboundedly across long-running processes | Pre-existing behavior; not in scope. Filed as future cleanup if needed. |
| Same EventListener instance subscribed to multiple topics → attrs race | Documented as sharp edge in docstring; not blocked |
| External consumers depending on `bus_id: int` parameter to `schedule_async_handler` | `schedule_async_handler` is `_internal.py`-private (underscore module); no breaking-change concern |
