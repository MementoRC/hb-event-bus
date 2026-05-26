# Design: EventListener / EventForwarder / SourceInfoEventForwarder

**Status**: Approved (brainstorming complete)
**Date**: 2026-05-26
**Issue**: [hb-event-bus#1](https://github.com/MementoRC/hb-event-bus/issues/1) — Extraction plan: PubSub + event dispatch machinery as standalone sub-package
**Scope**: Phase A additive — extends the Phase A public API without breaking it

---

## 1. Goal

Add three callable-object handler classes to `hb-event-bus`, ported from `hummingbot/core/event/event_listener.pyx` and `hummingbot/core/event/event_forwarder.py`, adapted to the string-keyed `EventBus` design:

- `EventListener` — base class for callable handlers that need dispatch context
- `EventForwarder(to_function)` — wraps `Callable[[Any], None]`
- `SourceInfoEventForwarder(to_function)` — wraps `Callable[[str, EventBus, Any], None]`, receives the topic + bus reference from each dispatch

**Use case**: Greenfield for new hb-* sub-packages adopting hb-event-bus. Not a Cython drop-in compat layer; no `int` event tags, no `PubSub` legacy.

**Non-goals**:
- No weak-ref behaviour (strong refs throughout, consistent with Phase A)
- No `int`-keyed dispatch
- No retention of the Cython `c_set_event_info` mechanism — replaced by Python attribute assignment in `EventBus.publish` / `apublish`

---

## 2. Architecture

**New module**: `event_bus/listener.py` — holds all three classes.

**Touched modules**:
- `event_bus/bus.py` — `publish()` and `apublish()` gain inline `isinstance(handler, EventListener)` injection before each handler invocation. No new public methods, no signature changes.
- `event_bus/__init__.py` — re-export the three new classes; extend `__all__`. Additive to the Phase A frozen API.
- `event_bus/_internal.py` — new helper `_is_async_handler(handler)` detects async-method `__call__` on class instances; used by both publish paths.

**Why a separate module** (not inlined into `bus.py`): listener semantics are independent of dispatch; mirrors hummingbot's file split; keeps `bus.py` under 200 lines.

**Why one module for all three classes** (not split into `listener.py` + `forwarder.py`): EventForwarder and SourceInfoEventForwarder are 8-line subclasses of EventListener and only make sense in its context. One cohesive concept = one file.

---

## 3. Components

### `event_bus/listener.py`

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

### `event_bus/bus.py` — modifications

Add import (top of file):

```python
from event_bus.listener import EventListener
from event_bus._internal import _is_async_handler  # replaces inline iscoroutinefunction
```

`publish()` snapshot loop:

```python
for sub in subs:
    handler = sub.handler
    if isinstance(handler, EventListener):
        handler.current_event_type = event_type
        handler.current_event_bus = self
    if _is_async_handler(handler):
        schedule_async_handler(event_type, payload, handler, bus_id=id(self))
    else:
        sync_handlers.append(handler)
invoke_sync_handlers(event_type, payload, sync_handlers)
```

`apublish()` loop:

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

### `event_bus/_internal.py` — new helper

```python
def _is_async_handler(handler: Any) -> bool:
    """True if handler is a coroutine function OR a class instance with async __call__."""
    if inspect.iscoroutinefunction(handler):
        return True
    call = getattr(handler, "__call__", None)
    return call is not None and inspect.iscoroutinefunction(call)
```

Also extend `invoke_sync_handlers` to warn-and-close any coroutine returned from a sync dispatch path:

```python
result = handler(payload)
if inspect.iscoroutine(result):
    result.close()
    logger.warning(
        "Sync dispatch received a coroutine from %r on topic=%r; "
        "use apublish() or expose async __call__ via a real method.",
        handler, event_type,
    )
```

### `event_bus/__init__.py` — additions

```python
from event_bus.listener import EventForwarder, EventListener, SourceInfoEventForwarder

__all__ = [
    "AsyncHandler", "EventBus", "EventForwarder", "EventListener",
    "EventLogger", "Handler", "LoggedEvent", "SourceInfoEventForwarder",
    "Subscription", "SyncHandler", "__version__",
]
```

---

## 4. Data Flow

### Sync dispatch (`bus.publish("order.filled", payload)`)

1. `publish` snapshots subscriptions for the topic.
2. For each subscription:
   - If handler is an `EventListener` instance: set `handler.current_event_type = topic`, `handler.current_event_bus = self`.
   - Classify via `_is_async_handler`: async → schedule; sync → append.
3. `invoke_sync_handlers` iterates sync list, calling each. Exceptions are logged + isolated.
4. After dispatch returns, attrs on the listener instance remain set to the last-dispatch values.

**Invariant**: between the attr-write and the `handler(payload)` call there is no `await` and no other Python code that could overwrite the attrs. Sync listeners always see consistent context inside their `__call__`.

### Async dispatch (`await bus.apublish(...)`)

Same injection happens before each `await handler(payload)` / `handler(payload)`. Inside an async listener's `__call__`, the attrs are valid **up to the first `await`** — after that, another publish on the same listener could overwrite them. Documented in the EventListener docstring.

### Coroutine-method listener routing

`inspect.iscoroutinefunction(listener_instance)` returns `False` even when `__call__` is `async def`. The new `_is_async_handler` helper probes `__call__` directly, so:

```python
class Async(EventListener):
    async def __call__(self, payload): ...
```

is correctly detected as async, scheduled on the running loop by `publish`, and awaited inline by `apublish`.

### Subscription lifecycle

Identical to existing handlers. `bus.subscribe(topic, forwarder)` returns a `Subscription`; `sub.cancel()` removes it. EventListener instances are held by strong reference; no special-case lifetime management.

---

## 5. Error Handling

### Constructor

- `EventListener()` — no args, no validation.
- `EventForwarder(to_function)` / `SourceInfoEventForwarder(to_function)` — `TypeError` if `not callable(to_function)`. Cheap guard against `None`-by-mistake at construction.
- No signature introspection on `to_function`; wrong arity is caught naturally at first dispatch.

### Dispatch

- `EventListener.__call__` raising `NotImplementedError` (the base class default) is treated like any other handler exception — logged via existing per-handler isolation in `invoke_sync_handlers` and `apublish`. No special path.
- User errors inside `_to_function` propagate the same way: logged, isolated, dispatch continues. Matches Phase A's documented contract.

### Coroutine-on-sync-path

If `_is_async_handler` returns `False` for an actually-async handler (e.g., a wrapper hiding `__call__` behind `__getattr__`), `invoke_sync_handlers` would silently drop the coroutine. The added `inspect.iscoroutine(result)` check closes the coroutine and emits a `WARNING` log — loud failure instead of silent.

### Reading context attrs outside dispatch

- Before any dispatch: `current_event_type == ""`, `current_event_bus is None`.
- After dispatch: attrs hold the most recent values; reading them is **undefined behavior** documented in the docstring.
- No exception raised. By design — defensive guards would hide bugs.

### Sharp edges (documented, not blocked)

- Sharing one `EventListener` instance across topics or buses → attrs reflect last dispatch.
- Publishing from inside a listener's `__call__` → inner dispatch may overwrite outer attrs.

### Explicit non-defenses

- No arity check on `to_function`.
- No try/except around `isinstance`.
- No re-entrancy guard on attrs.

---

## 6. Testing

### New file: `tests/test_listener.py`

**EventListener (4 tests)**
- `test_eventlistener_init_defaults` — attrs default to `""` and `None`
- `test_eventlistener_call_raises_notimplemented`
- `test_eventlistener_slots_prevent_arbitrary_attrs`
- `test_eventlistener_subclass_can_override_call`

**EventForwarder (5 tests)**
- `test_forwarder_calls_to_function_with_payload`
- `test_forwarder_rejects_non_callable`
- `test_forwarder_inherits_eventlistener`
- `test_forwarder_via_eventbus_publish` — integration with bus
- `test_forwarder_isolated_on_exception`

**SourceInfoEventForwarder (6 tests)**
- `test_source_forwarder_receives_topic_bus_payload`
- `test_source_forwarder_rejects_non_callable`
- `test_source_forwarder_attrs_set_before_call`
- `test_source_forwarder_attrs_unset_outside_dispatch`
- `test_source_forwarder_via_apublish` — async path
- `test_source_forwarder_two_topics_attrs_reflect_last_dispatch` — documents sharp edge

### Additions to existing files

**`tests/test_bus_publish.py` (+2)**
- `test_publish_injects_event_info_before_call`
- `test_publish_warns_on_coroutine_from_sync_path`

**`tests/test_bus_apublish.py` (+1)**
- `test_apublish_injects_event_info_before_each_await`

**`tests/test_internal.py` (+4)**
- `test_is_async_handler_plain_function_false`
- `test_is_async_handler_plain_coroutine_function_true`
- `test_is_async_handler_class_instance_with_async_call_true`
- `test_is_async_handler_class_instance_with_sync_call_false`

### Out of scope

- No type-arity tests on `to_function`.
- No weak-ref behavior tests (strong refs by design).
- No multi-bus stress tests.
- No cross-thread tests.

### Totals

22 new tests. Repo: 44 → ~66.

---

## 7. Public API Impact

Phase A's frozen public API gains three additive exports. No existing class signatures change.

| Symbol | Before | After |
|--------|--------|-------|
| `EventListener` | — | exported |
| `EventForwarder` | — | exported |
| `SourceInfoEventForwarder` | — | exported |
| `EventBus.publish` / `apublish` | unchanged signatures | inline isinstance injection, same docstring contract |
| `Subscription` | unchanged | unchanged |

The README "Phase A Complete" wording will need a brief note that listener/forwarder support landed as an additive feature without changing the existing API contract.

---

## 8. Open Questions / Deferred

None. All design decisions resolved in brainstorming:
- ✅ Greenfield use case (no int-tag legacy)
- ✅ EventListener base with mutable attrs + isinstance injection
- ✅ Strong refs (Phase A consistent)
- ✅ Three classes in one module
- ✅ Coroutine-on-sync-path warns-and-closes
