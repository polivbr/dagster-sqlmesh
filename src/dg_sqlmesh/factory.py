from dagster import (
    multi_asset,
    AssetExecutionContext,
    RetryPolicy,
    schedule,
    define_asset_job,
    RunRequest,
    Definitions,
)
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import (
    get_asset_kinds,
    get_extra_keys,
    create_asset_specs,
    create_asset_checks,
    validate_external_dependencies,
)
import datetime
from .translator import SQLMeshTranslator
from typing import Optional, Dict, List, Any

def sqlmesh_assets_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    retry_policy: Optional[RetryPolicy] = None,
    owners: Optional[List[str]] = None,
):
    """
    Factory to create SQLMesh Dagster assets.
    
    Args:
        sqlmesh_resource: Configured SQLMesh resource
        name: Multi-asset name
        group_name: Default group for assets
        op_tags: Operation tags
        retry_policy: Retry policy
        owners: Asset owners
    """
    try:
        extra_keys = get_extra_keys()
        kinds = get_asset_kinds(sqlmesh_resource)

        # Create AssetSpec and AssetCheckSpec
        specs = create_asset_specs(sqlmesh_resource, extra_keys, kinds, owners, group_name)
        asset_checks = create_asset_checks(sqlmesh_resource)
    except Exception as e:
        raise ValueError(f"Failed to create SQLMesh assets: {e}") from e

    @multi_asset(
        name=name,
        specs=specs,
        check_specs=asset_checks,
        op_tags=op_tags,
        retry_policy=retry_policy,
        can_subset=True
    )
    def _sqlmesh_assets(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
        context.log.info("🚀 Starting SQLMesh materialization")
        
        # Log assets to be materialized (the actually selected ones)
        selected_asset_keys = context.selected_asset_keys
        context.log.info(f"📦 Assets to materialize: {len(selected_asset_keys)} assets")
        for i, asset_key in enumerate(selected_asset_keys, 1):
            context.log.info(f"   {i}. 🎯 {asset_key}")
        
        try:
            yield from sqlmesh.materialize_all_assets(context)
            context.log.info("✅ SQLMesh materialization completed")
        except Exception as e:
            context.log.error(f"❌ SQLMesh materialization failed: {e}")
            raise

    return _sqlmesh_assets


def sqlmesh_adaptive_schedule_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_adaptive_schedule",
):
    """
    Factory to create an adaptive Dagster schedule based on SQLMesh crons.
    
    Args:
        sqlmesh_resource: Configured SQLMesh resource
        name: Schedule name
    """
    
    # Get recommended schedule based on SQLMesh crons
    recommended_schedule = sqlmesh_resource.get_recommended_schedule()
    
    # Automatically create SQLMesh job with multi_asset (for AssetCheckResult)
    sqlmesh_assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)
    sqlmesh_job = define_asset_job(
        name="sqlmesh_job",
        selection=[sqlmesh_assets],
    )
    
    @schedule(
        job=sqlmesh_job,
        cron_schedule=recommended_schedule,
        name=name,
        description=f"Adaptive schedule based on SQLMesh crons (granularity: {recommended_schedule})"
    )
    def _sqlmesh_adaptive_schedule(context):
        return RunRequest(
            run_key=f"sqlmesh_adaptive_{datetime.datetime.now().isoformat()}",
            tags={"schedule": "sqlmesh_adaptive", "granularity": recommended_schedule}
        )
    
    return _sqlmesh_adaptive_schedule, sqlmesh_job, sqlmesh_assets


def sqlmesh_definitions_factory(
    *,
    project_dir: str = "sqlmesh_project",
    gateway: str = "postgres",
    concurrency_limit: int = 1,
    ignore_cron: bool = False,
    translator: Optional[SQLMeshTranslator] = None,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    retry_policy: Optional[RetryPolicy] = None,
    owners: Optional[List[str]] = None,
    schedule_name: str = "sqlmesh_adaptive_schedule",
):
    """
    All-in-one factory to create a complete SQLMesh integration with Dagster.
    
    Args:
        project_dir: SQLMesh project directory
        gateway: SQLMesh gateway (postgres, duckdb, etc.)
        concurrency_limit: Concurrency limit
        ignore_cron: Ignore crons (for tests)
        translator: Custom translator for asset keys
        name: Multi-asset name
        group_name: Default group for assets
        op_tags: Operation tags
        retry_policy: Retry policy
        owners: Asset owners
        schedule_name: Adaptive schedule name
    """
    
    # Parameter validation
    if concurrency_limit < 1:
        raise ValueError("concurrency_limit must be >= 1")
    
    # Robust default values
    op_tags = op_tags or {"sqlmesh": "true"}
    owners = owners or []
    
    # Create SQLMesh resource
    sqlmesh_resource = SQLMeshResource(
        project_dir=project_dir,
        gateway=gateway,
        translator=translator,
        concurrency_limit=concurrency_limit,
        ignore_cron=ignore_cron
    )
    
    # Validate external dependencies
    try:
        models = sqlmesh_resource.get_models()
        validation_errors = validate_external_dependencies(sqlmesh_resource, models)
        if validation_errors:
            raise ValueError(f"External dependencies validation failed:\n" + "\n".join(validation_errors))
    except Exception as e:
        raise ValueError(f"Failed to validate external dependencies: {e}") from e
    
    # Create SQLMesh assets
    sqlmesh_assets = sqlmesh_assets_factory(
        sqlmesh_resource=sqlmesh_resource,
        name=name,
        group_name=group_name,
        op_tags=op_tags,
        retry_policy=retry_policy,
        owners=owners,
    )
    
    # Create adaptive schedule and job
    sqlmesh_adaptive_schedule, sqlmesh_job, _ = sqlmesh_adaptive_schedule_factory(
        sqlmesh_resource=sqlmesh_resource,
        name=schedule_name
    )
    
    # Return complete Definitions
    return Definitions(
        assets=[sqlmesh_assets],
        jobs=[sqlmesh_job],
        schedules=[sqlmesh_adaptive_schedule],
        resources={
            "sqlmesh": sqlmesh_resource,
        },
    ) 