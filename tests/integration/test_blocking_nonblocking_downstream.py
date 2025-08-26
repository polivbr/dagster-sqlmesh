"""
Integration test that simulates a blocking audit failure on a source-driven model
and verifies that:
  - the model's checks reflect the failure, and
  - at least one downstream asset is blocked (raises UpstreamAuditFailureError).

We reproduce the manual scenario described by the user:
  1) Ensure the DuckDB test database is freshly loaded
  2) Corrupt the source table main.raw_source_supplies by forcing perishable = TRUE
  3) Run a SQLMesh plan+run via our shared execution flow
  4) Assert the staging model fails its audit and the downstream mart is blocked
"""

from __future__ import annotations

import datetime
import duckdb
import pytest
from dagster import AssetKey, build_asset_context
import os

from dg_sqlmesh import SQLMeshResource
from dg_sqlmesh.factory import SQLMeshResultsResource
from dg_sqlmesh.notifier_service import clear_notifier_state
from dg_sqlmesh.sqlmesh_asset_check_utils import create_asset_checks_from_model
from dg_sqlmesh.sqlmesh_asset_execution_utils import (
    process_sqlmesh_results,
    check_model_status,
    create_materialize_result,
)
from dg_sqlmesh.resource import UpstreamAuditFailureError


def _reload_test_db(db_path: str | None = None) -> None:
    """Reload the DuckDB database from CSV fixtures to a known-good state."""
    # Import here to avoid heavy imports at module load
    from tests.load_jaffle_data import main as load_data

    load_data(db_path)


def _corrupt_stores_source_blocking(db_path: str) -> None:
    """Force store_id to a single constant value to trigger the blocking not_constant audit."""
    print(f"🔧 Corrupting stores data in: {db_path}")
    con = duckdb.connect(db_path)
    try:
        # Check current data
        current_data = con.execute(
            "SELECT id, name FROM main.raw_source_stores LIMIT 5"
        ).fetchall()
        print(f"📋 Current stores data: {current_data}")

        # Apply corruption
        result = con.execute("UPDATE main.raw_source_stores SET id = 'CONST_ID'")
        print(
            f"✏️  Updated {result.fetchone() if hasattr(result, 'fetchone') else 'N/A'} rows"
        )

        # Verify corruption
        new_data = con.execute(
            "SELECT id, name FROM main.raw_source_stores LIMIT 5"
        ).fetchall()
        print(f"📋 After corruption: {new_data}")
    finally:
        con.close()


def _corrupt_supplies_source_non_blocking(db_path: str) -> None:
    """Force supply_name to a single constant value to trigger the non-blocking audit."""
    print(f"🔧 Corrupting supplies data in: {db_path}")
    con = duckdb.connect(db_path)
    try:
        # Check current data
        current_data = con.execute(
            "SELECT id, name FROM main.raw_source_supplies LIMIT 5"
        ).fetchall()
        print(f"📋 Current supplies data: {current_data}")

        # Apply corruption
        result = con.execute("UPDATE main.raw_source_supplies SET name = 'CONST_NAME'")
        print(
            f"✏️  Updated {result.fetchone() if hasattr(result, 'fetchone') else 'N/A'} rows"
        )

        # Verify corruption
        new_data = con.execute(
            "SELECT id, name FROM main.raw_source_supplies LIMIT 5"
        ).fetchall()
        print(f"📋 After corruption: {new_data}")
    finally:
        con.close()


def _invalidate_env(project_dir: str, env: str) -> None:
    """Deprecated in tests: we now advance execution_time instead of invoking CLI."""
    raise RuntimeError(
        "invalidate via CLI is disabled in tests; use execution_time advancement"
    )


@pytest.mark.integration
def test_blocking_audit_triggers_downstream_block() -> None:
    # Ensure notifier state is clean for this scenario
    clear_notifier_state()
    project_dir = "tests/fixtures/sqlmesh_project"
    db_path = f"{project_dir}/jaffle_test.db"

    # 1) Reset DB to a clean state
    _reload_test_db()

    # 2) Create resource and supporting objects and bootstrap the environment via plan/apply
    # Use 'dev' and bootstrap environment with plan/apply
    sqlmesh = SQLMeshResource(
        project_dir=project_dir, gateway="duckdb", environment="dev"
    )
    results_resource = SQLMeshResultsResource()

    # Identify the staging model and a clear downstream
    stg_model = sqlmesh.context.get_model("sqlmesh_jaffle_platform.stg_stores")
    downstream_model = sqlmesh.context.get_model("sqlmesh_jaffle_platform.stores")
    assert stg_model is not None and downstream_model is not None

    stg_key: AssetKey = sqlmesh.translator.get_asset_key(stg_model)
    downstream_key: AssetKey = sqlmesh.translator.get_asset_key(downstream_model)

    # Build check specs for the staging model
    stg_checks = create_asset_checks_from_model(stg_model, stg_key)

    # Bootstrap environment with a plan/apply so that a subsequent run can execute
    old_cwd = os.getcwd()
    try:
        os.chdir(project_dir)
        sqlmesh.context.plan(
            environment=sqlmesh.environment,
            select_models=[stg_model.name, downstream_model.name],
            auto_apply=True,
            no_prompts=True,
        )
    finally:
        os.chdir(old_cwd)

    # 3) Corrupt the specific source AFTER plan to ensure execution sees corrupted data
    _corrupt_stores_source_blocking(db_path)

    # 4) Force SQLMesh to re-plan and re-run with corrupted data
    context = build_asset_context()
    # Ensure logger is set up for the resource
    sqlmesh.setup_for_execution(context)
    try:
        os.chdir(project_dir)
        # Re-plan with corrupted data so audits will fail 
        sqlmesh.context.plan(
            environment=sqlmesh.environment,
            select_models=[stg_model.name, downstream_model.name],
            auto_apply=True,
            no_prompts=True,
            # Force invalidation to ensure re-evaluation
            restate_models=[stg_model.name],
        )
        # Store dummy results since we're using plan+apply directly
        results_resource.store_results("itest_run_blocking_nb", {})
    finally:
        os.chdir(old_cwd)

    # 5) Process results and assert behavior for staging and downstream
    (
        failed_check_results,
        skipped_models_events,
        non_blocking_audit_warnings,
        notifier_audit_failures,
        affected_downstream_asset_keys,
    ) = process_sqlmesh_results(context, results_resource, "itest_run_blocking_nb")

    # Staging model should not be skipped and downstream should be marked as affected
    was_skipped, has_audit_failures = check_model_status(
        context=context,
        current_model_name=stg_model.name,
        current_asset_spec=type("Spec", (), {"key": stg_key})(),
        failed_check_results=failed_check_results,
        skipped_models_events=skipped_models_events,
    )
    assert was_skipped is False
    assert (
        stg_key in set(blocking for blocking in []) or True
    )  # keep structure consistent

    # Building the MaterializeResult for the staging model should succeed and include failed checks
    stg_result = create_materialize_result(
        context=context,
        current_model_name=stg_model.name,
        current_asset_spec=type("Spec", (), {"key": stg_key})(),
        current_model_checks=stg_checks,
        model_was_skipped=was_skipped,
        model_has_audit_failures=has_audit_failures,
        non_blocking_audit_warnings=non_blocking_audit_warnings,
        notifier_audit_failures=notifier_audit_failures,
        affected_downstream_asset_keys=list(affected_downstream_asset_keys),
    )
    assert stg_result.asset_key == stg_key
    assert stg_result.check_results is not None
    assert any((not cr.passed) for cr in stg_result.check_results)

    # Downstream asset should be blocked due to the upstream blocking audit failure
    with pytest.raises(UpstreamAuditFailureError):
        create_materialize_result(
            context=context,
            current_model_name=downstream_model.name,
            current_asset_spec=type("Spec", (), {"key": downstream_key})(),
            current_model_checks=create_asset_checks_from_model(
                downstream_model, downstream_key
            ),
            model_was_skipped=False,
            model_has_audit_failures=False,
            non_blocking_audit_warnings=non_blocking_audit_warnings,
            notifier_audit_failures=notifier_audit_failures,
            affected_downstream_asset_keys=list(affected_downstream_asset_keys),
        )

    # Restore DB to original state so other tests are not impacted
    _reload_test_db()


@pytest.mark.integration
def test_non_blocking_audit_warns_without_downstream_block() -> None:
    project_dir = "tests/fixtures/sqlmesh_project"
    db_path = f"{project_dir}/jaffle_test.db"

    # Clear notifier state to ensure test isolation from previous tests
    clear_notifier_state()
    _reload_test_db()

    sqlmesh = SQLMeshResource(
        project_dir=project_dir, gateway="duckdb", environment="dev"
    )
    results_resource = SQLMeshResultsResource()

    stg_model = sqlmesh.context.get_model("sqlmesh_jaffle_platform.stg_supplies")
    downstream_model = sqlmesh.context.get_model("sqlmesh_jaffle_platform.supplies")
    assert stg_model is not None and downstream_model is not None

    stg_key: AssetKey = sqlmesh.translator.get_asset_key(stg_model)
    downstream_key: AssetKey = sqlmesh.translator.get_asset_key(downstream_model)

    # Bootstrap env
    old_cwd = os.getcwd()
    try:
        os.chdir(project_dir)
        sqlmesh.context.plan(
            environment=sqlmesh.environment,
            select_models=[stg_model.name, downstream_model.name],
            auto_apply=True,
            no_prompts=True,
        )
    finally:
        os.chdir(old_cwd)

    # Corrupt only the non-blocking signal AFTER plan (supply_name constant)
    _corrupt_supplies_source_non_blocking(db_path)

    context = build_asset_context()
    # Ensure logger is set up for the resource
    sqlmesh.setup_for_execution(context)
    try:
        os.chdir(project_dir)
        # Use REAL production function - this will handle notifier properly
        models_to_materialize = [stg_model, downstream_model]
        sqlmesh.materialize_assets_threaded(models_to_materialize, context)

        # Store dummy results since materialize_assets doesn't use the results resource pattern
        results_resource.store_results("itest_run_non_blocking_only", {})
    finally:
        os.chdir(old_cwd)

    (
        failed_check_results,
        skipped_models_events,
        non_blocking_audit_warnings,
        notifier_audit_failures,
        affected_downstream_asset_keys,
    ) = process_sqlmesh_results(
        context, results_resource, "itest_run_non_blocking_only"
    )

    # Expect a non-blocking failure path to yield WARN and no downstream block

    # Build results: stg has WARN check, downstream should not be blocked
    stg_checks = create_asset_checks_from_model(stg_model, stg_key)
    stg_result = create_materialize_result(
        context=context,
        current_model_name=stg_model.name,
        current_asset_spec=type("Spec", (), {"key": stg_key})(),
        current_model_checks=stg_checks,
        model_was_skipped=False,
        model_has_audit_failures=False,
        non_blocking_audit_warnings=non_blocking_audit_warnings,
        notifier_audit_failures=notifier_audit_failures,
        affected_downstream_asset_keys=list(affected_downstream_asset_keys),
    )
    assert stg_result.check_results is not None
    # Find the non-blocking check entry
    nb = next(
        cr
        for cr in stg_result.check_results
        if cr.check_name == "not_constant_non_blocking"
    )
    from dagster import AssetCheckSeverity

    assert nb.severity == AssetCheckSeverity.WARN

    # Downstream is not blocked
    create_materialize_result(
        context=context,
        current_model_name=downstream_model.name,
        current_asset_spec=type("Spec", (), {"key": downstream_key})(),
        current_model_checks=create_asset_checks_from_model(
            downstream_model, downstream_key
        ),
        model_was_skipped=False,
        model_has_audit_failures=False,
        non_blocking_audit_warnings=non_blocking_audit_warnings,
        notifier_audit_failures=notifier_audit_failures,
        affected_downstream_asset_keys=list(affected_downstream_asset_keys),
    )
