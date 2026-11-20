"""Shared type aliases for event_bus."""

type EventKey = str | int
"""Event-type key: ``str | int``, compared by equality/hash, so ``1`` and ``"1"`` are distinct keys.

``bool`` keys raise ``TypeError``. ``EventKey`` is a PEP 695 alias: ``isinstance(x, EventKey)``
raises ``TypeError``. Introspect with ``typing.get_args(EventKey.__value__)`` (``(str, int)``);
for runtime checks use ``isinstance(x, (str, int)) and not isinstance(x, bool)``.
"""


def _reject_bool_key(event_type: object) -> None:
    """Raise ``TypeError`` for ``bool`` keys (``True == 1``/``False == 0`` alias int channels)."""
    if isinstance(event_type, bool):
        raise TypeError("event_type must be str or int, not bool")
