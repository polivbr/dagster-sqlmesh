from typing import Any

from dagster import AssetKey


def test_convert_notifier_failures_to_asset_check_results_basic() -> None:
    from dg_sqlmesh.sqlmesh_asset_check_utils import (
        convert_notifier_failures_to_asset_check_results,
    )

    class DummyTranslator:
        def get_asset_key(self, model: Any) -> AssetKey:
            return AssetKey(["db", "schema", getattr(model, "view_name", "model")])

    class DummyContext:
        def __init__(self) -> None:
            self._models = {"s.m": type("M", (), {"view_name": "m"})()}

        def get_model(self, name: str) -> Any:
            return self._models.get(name)

    ctx = DummyContext()
    translator = DummyTranslator()

    failures = [
        {
            "model": "s.m",
            "audit": "a1",
            "sql": "select 1",
            "blocking": True,
            "count": 2,
        },
        {
            "model": "s.m",
            "audit": "a2",
            "sql": "select 2",
            "blocking": False,
            "count": 0,
        },
    ]

    results = convert_notifier_failures_to_asset_check_results(
        context=ctx, translator=translator, failures=failures
    )
    assert len(results) == 2
    # Ensure severity is set based on blocking flag and metadata is present
    by_name = {r.check_name: r for r in results}
    assert by_name["a1"].metadata["audit_blocking"].value is True
    assert by_name["a2"].metadata["audit_blocking"].value is False
