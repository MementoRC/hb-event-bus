"""Smoke test: verify the package is importable and version is set."""

import re

import event_bus


def test_version_is_set() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", event_bus.__version__)
