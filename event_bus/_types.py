"""Shared type aliases for event_bus."""

type EventKey = str | int
"""Event-type key: compared by equality/hash, so ``1`` and ``"1"`` are distinct keys."""
