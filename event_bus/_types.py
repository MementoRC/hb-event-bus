"""Shared type aliases for event_bus."""

type EventKey = str | int
"""Event-type key: compared by equality/hash, so ``1`` and ``"1"`` are distinct keys."""


def _reject_bool_key(event_type: object) -> None:
    """Raise ``TypeError`` for ``bool`` keys (``True == 1``/``False == 0`` alias int channels)."""
    if isinstance(event_type, bool):
        raise TypeError("event_type must be str or int, not bool")
