# hb-event-bus

<!-- [![CI](https://github.com/MementoRC/hb-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/MementoRC/hb-event-bus/actions/workflows/ci.yml) -->
<!-- [![codecov](https://codecov.io/gh/MementoRC/hb-event-bus)](https://codecov.io/gh/MementoRC/hb-event-bus) -->
<!-- [![PyPI version](https://badge.fury.io/py/hb-event-bus.svg)](https://badge.fury.io/py/hb-event-bus) -->
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Overview

**hb-event-bus** is an asynchronous event bus for inter-component messaging in Hummingbot sub-packages. It enables decoupled, publish-subscribe communication with both synchronous and asynchronous subscribers, extracted from the original `pubsub.pyx` module in Hummingbot core.

---

## Installation

Install via pixi (recommended):

```bash
pixi add hb-event-bus
```

Or pip:

```bash
pip install hb-event-bus
```

## Quick Start

Subscribe to events and publish messages:

```python
from event_bus import EventBus, Handler, AsyncHandler

# Create an event bus instance
bus = EventBus()

# Define a sync handler
@Handler(bus)
def on_trade_executed(trade_id: str, price: float):
    print(f"Trade {trade_id} executed at {price}")

# Define an async handler
@AsyncHandler(bus)
async def on_trade_logged(trade_id: str, price: float):
    await log_to_database(trade_id, price)

# Publish an event (synchronously)
bus.publish("on_trade_executed", "TRD123", 42.50)

# Publish an event with async handlers (async context required)
await bus.apublish("on_trade_logged", "TRD124", 42.75)

# Unsubscribe when needed
subscription = bus.subscribe("on_trade_executed", on_trade_executed)
subscription.cancel()
```

---

## Public API

### EventBus

Main event bus class for managing subscriptions and publishing events.

**Methods:**

- `subscribe(topic: str, handler: Callable) -> Subscription` — Register a synchronous handler for a topic. Returns a subscription that can be cancelled.
- `publish(topic: str, *args, **kwargs) -> None` — Publish an event synchronously. All registered handlers are called in order.
- `apublish(topic: str, *args, **kwargs) -> Awaitable` — Publish an event with async-aware dispatch. Coroutines are wrapped; await this method in async context.

### Subscription

Returned by `subscribe()` to manage a handler registration.

**Methods:**

- `cancel() -> None` — Unsubscribe the associated handler from the topic.

### Handler & AsyncHandler

Decorators for registering handlers:

- `@Handler(bus)` — Registers a synchronous handler on the bus.
- `@AsyncHandler(bus)` — Registers an asynchronous handler (coroutine) on the bus.

---

## Strong References vs Weak References: Migration Note

The original `pubsub.pyx` used **weak references** to handlers to avoid circular reference leaks. This behavior has changed in hb-event-bus: **handlers are now held by strong references by default** via the `Subscription` object.

**For consumers migrating from pubsub.pyx:**

If you were relying on weak reference cleanup (i.e., handlers being garbage-collected automatically when the handler object is deleted), you must now explicitly call `subscription.cancel()` to unsubscribe:

```python
# OLD (pubsub.pyx weak-ref behavior):
bus.subscribe(topic, handler)  # Auto-cleaned on handler GC

# NEW (hb-event-bus strong-ref behavior):
subscription = bus.subscribe(topic, handler)
# ... later, when handler is no longer needed:
subscription.cancel()  # Must explicitly unsubscribe
```

This change eliminates subtle GC-dependent bugs and makes subscription lifetime explicit. See the extraction plan (GitHub issue #1) for architectural rationale.

---

## Async Dispatch Semantics

When publishing events with async handlers:

- **`publish()`** — Synchronous only. Async handlers are NOT awaited; their coroutines are returned as a side-effect but not run.
- **`apublish()`** — Returns an awaitable that collects and executes all async handlers. Use in async contexts only.

Example:

```python
@AsyncHandler(bus)
async def on_event(data: str):
    await asyncio.sleep(0.1)

# In async context:
await bus.apublish("on_event", "data")  # Awaits all async handlers

# In sync context:
bus.publish("on_event", "data")  # Does not await async handlers
```

For Phase C refactors in Hummingbot, ensure all async subscribers use `apublish()` to prevent race conditions.

---

## Development

Install and test:

```bash
pixi install
pixi run test
pixi run lint
pixi run format --check
pixi run check  # Full suite
```

---

## Status

**Phase A Complete** — Public API (`EventBus`, `Subscription`, `Handler`, `AsyncHandler`) is stable and exported via `event_bus/__init__.py`. Feature set and design are frozen for Phase B (Hummingbot integration).

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## Resources

- **Extraction Plan**: [GitHub Issue #1](https://github.com/MementoRC/hb-event-bus/issues/1) — Design rationale and Phase B/C integration roadmap.
- **Related**: Extracted from Hummingbot's `pubsub.pyx` module as part of sub-package modularization initiative.
