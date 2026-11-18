"""typing.get_type_hints must succeed for every public export of event_bus."""

from __future__ import annotations

import inspect
import subprocess
import sys
import typing
from typing import Any

import pytest

import event_bus
from event_bus import EventKey


def _is_alias(obj: Any) -> bool:
    # Pure aliases (PEP 695 TypeAliasType, subscripted generics like Callable[[Any], None],
    # Any) and non-callables (e.g. __version__) carry no annotations of their own to resolve.
    return (
        isinstance(obj, typing.TypeAliasType)
        or typing.get_origin(obj) is not None
        or obj is Any
        or not callable(obj)
    )


def _targets(obj: Any) -> list[Any]:
    if not inspect.isclass(obj):
        return [obj]
    targets: list[Any] = [obj]
    for name, member in vars(obj).items():
        if name.startswith("_") and name != "__init__":
            continue
        if isinstance(member, (staticmethod, classmethod)):
            member = member.__func__
        elif isinstance(member, property):
            member = member.fget
        if inspect.isfunction(member):
            targets.append(member)
    return targets


_NAMES = sorted(event_bus.__all__)


@pytest.mark.parametrize("name", _NAMES)
def test_get_type_hints_succeeds_for_export(name: str) -> None:
    obj = getattr(event_bus, name)
    if _is_alias(obj):
        pytest.skip(f"{name} is an alias or non-callable; no annotations to resolve")
    for target in _targets(obj):
        typing.get_type_hints(target)


def test_event_key_args() -> None:
    assert typing.get_args(EventKey.__value__) == (str, int)


@pytest.mark.parametrize(
    "module",
    ["event_bus.listener", "event_bus.event_logger", "event_bus.bus", "event_bus"],
)
def test_fresh_interpreter_import(module: str) -> None:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
