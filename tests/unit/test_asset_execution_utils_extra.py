from typing import Any

import pytest
from dagster import AssetKey


def test_get_check_severity_for_blocking() -> None:
    from dagster import AssetCheckSeverity
    from dg_sqlmesh.sqlmesh_asset_execution_utils import get_check_severity_for_blocking

    assert get_check_severity_for_blocking(True) == AssetCheckSeverity.ERROR
    assert get_check_severity_for_blocking(False) == AssetCheckSeverity.WARN


def test_create_materialize_result_downstream_block() -> None:
    from dg_sqlmesh.sqlmesh_asset_execution_utils import create_materialize_result
    from dg_sqlmesh.resource import UpstreamAuditFailureError

    class DummyLog:
        def info(self, *args: Any, **kwargs: Any) -> None: ...
        def debug(self, *args: Any, **kwargs: Any) -> None: ...
        def error(self, *args: Any, **kwargs: Any) -> None: ...

    class DummyContext:
        log = DummyLog()

    class Spec:
        def __init__(self, key: AssetKey) -> None:
            self.key = key

    context = DummyContext()
    asset_key = AssetKey(["db", "schema", "model"])
    current_asset_spec = Spec(asset_key)

    with pytest.raises(UpstreamAuditFailureError):
        create_materialize_result(
            context=context,
            current_model_name="schema.model",
            current_asset_spec=current_asset_spec,
            current_model_checks=[],
            model_was_skipped=False,
            model_has_audit_failures=False,
            non_blocking_audit_warnings=[],
            notifier_audit_failures=[],
            affected_downstream_asset_keys=[asset_key],
        )
