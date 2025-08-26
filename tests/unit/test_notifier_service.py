from typing import Any


def test_get_or_create_notifier_singleton() -> None:
    from dg_sqlmesh.notifier_service import get_or_create_notifier

    n1 = get_or_create_notifier()
    n2 = get_or_create_notifier()
    assert n1 is n2


def test_register_notifier_in_context_idempotent() -> None:
    from dg_sqlmesh.notifier_service import (
        get_or_create_notifier,
        register_notifier_in_context,
    )

    class DummyContext:
        def __init__(self) -> None:
            self.notification_targets: list[Any] = []
            self._register_calls = 0

        def _register_notification_targets(self) -> None:  # type: ignore[no-untyped-def]
            self._register_calls += 1

    ctx = DummyContext()
    notifier = get_or_create_notifier()

    # First registration
    register_notifier_in_context(ctx)
    assert notifier in ctx.notification_targets
    first_calls = ctx._register_calls

    # Second registration should be idempotent (no duplicate)
    register_notifier_in_context(ctx)
    assert ctx.notification_targets.count(notifier) == 1
    # _register_notification_targets may be called again safely
    assert ctx._register_calls >= first_calls


def test_get_audit_failures_safe() -> None:
    from dg_sqlmesh.notifier_service import (
        get_or_create_notifier,
        get_audit_failures,
    )

    get_or_create_notifier()
    # Should return list even when empty
    failures = get_audit_failures()
    assert isinstance(failures, list)
