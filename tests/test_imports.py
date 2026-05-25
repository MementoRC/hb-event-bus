"""Test public API exports."""


def test_public_exports():
    """Verify that EventBus, Subscription, and Handler types are accessible."""
    import event_bus

    assert hasattr(event_bus, "EventBus")
    assert hasattr(event_bus, "Subscription")
    assert hasattr(event_bus, "Handler")
    assert hasattr(event_bus, "SyncHandler")
    assert hasattr(event_bus, "AsyncHandler")
    assert hasattr(event_bus, "__version__")
    assert set(event_bus.__all__) >= {
        "AsyncHandler",
        "EventBus",
        "Handler",
        "Subscription",
        "SyncHandler",
        "__version__",
    }


def test_version_present():
    """Verify that __version__ is accessible."""
    from event_bus.__about__ import __version__

    assert __version__ == "0.1.0"
