"""Smoke test: verify the package is importable and version is set."""

import event_bus


def test_version_is_set() -> None:
    assert event_bus.__version__ == "0.0.1"
