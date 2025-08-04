"""
Advanced usage examples for dg-sqlmesh.

This file demonstrates advanced features including:
- Custom resource configuration
- Asset selection and materialization
- Error handling and failed models processing
- Asset checks and audit results
"""

from dagster import (
    Definitions,
    MaterializeResult,
    AssetCheckResult,
    multi_asset,
    AssetExecutionContext,
    RetryPolicy,
)
from dg_sqlmesh import (
    SQLMeshResource,
    sqlmesh_assets_factory,
    sqlmesh_definitions_factory,
)


# Example 1: Custom resource with error handling
def example_custom_resource_with_error_handling():
    """Example showing how to use SQLMeshResource with custom error handling."""
    
    # Create custom resource
    sqlmesh_resource = SQLMeshResource(
        project_dir="tests/sqlmesh_project",
        gateway="duckdb",
        environment="dev",
        concurrency_limit=2,
        ignore_cron=True
    )
    
    # Create assets
    assets = sqlmesh_assets_factory(
        sqlmesh_resource=sqlmesh_resource,
        name="custom_sqlmesh_assets",
        group_name="custom_group",
        op_tags={"team": "data", "domain": "analytics"},
        retry_policy=RetryPolicy(max_retries=3),
        owners=["data-team", "analytics"]
    )
    
    # Create definitions
    defs = Definitions(
        assets=[assets],
        resources={"sqlmesh": sqlmesh_resource}
    )
    
    return defs


# Example 2: Manual asset materialization with error processing
@multi_asset(
    name="manual_sqlmesh_assets",
    group_name="manual_group",
    deps=["sqlmesh_resource"]
)
def manual_sqlmesh_assets(context: AssetExecutionContext, sqlmesh_resource: SQLMeshResource):
    """
    Example of manual asset materialization with custom error handling.
    
    This shows how to:
    1. Materialize specific assets
    2. Process failed models events
    3. Handle audit failures
    4. Yield both MaterializeResult and AssetCheckResult
    """
    
    # Get models to materialize
    models = sqlmesh_resource.get_models()
    selected_models = [model for model in models if "stg_" in model.name][:2]  # Just first 2 staging models
    
    try:
        # Materialize assets
        for result in sqlmesh_resource.materialize_all_assets(context):
            if isinstance(result, MaterializeResult):
                # Handle successful materialization
                context.log.info(f"✅ Materialized {result.asset_key}")
                yield result
            elif isinstance(result, AssetCheckResult):
                # Handle asset check results (audits)
                if result.passed:
                    context.log.info(f"✅ Audit passed: {result.check_name}")
                else:
                    context.log.error(f"❌ Audit failed: {result.check_name}")
                    # Log detailed audit information
                    metadata = result.metadata
                    context.log.error(f"   Model: {metadata.get('sqlmesh_model_name', 'unknown')}")
                    context.log.error(f"   Query: {metadata.get('audit_query', 'N/A')}")
                    context.log.error(f"   Message: {metadata.get('audit_message', 'N/A')}")
                    context.log.error(f"   Error type: {metadata.get('error_type', 'unknown')}")
                yield result
                
    except Exception as e:
        context.log.error(f"❌ Materialization failed: {e}")
        raise


# Example 3: Complete definitions with error handling
def example_complete_definitions_with_error_handling():
    """Example showing complete definitions with comprehensive error handling."""
    
    # Create resource with custom configuration
    sqlmesh_resource = SQLMeshResource(
        project_dir="tests/sqlmesh_project",
        gateway="duckdb",
        environment="dev",
        concurrency_limit=1,  # Conservative for error handling
        ignore_cron=True
    )
    
    # Create assets with error handling
    assets = sqlmesh_assets_factory(
        sqlmesh_resource=sqlmesh_resource,
        name="error_handling_assets",
        group_name="error_handling",
        op_tags={"team": "data", "error_handling": "enabled"},
        retry_policy=RetryPolicy(max_retries=2),
        owners=["data-team"]
    )
    
    # Create definitions
    defs = Definitions(
        assets=[assets, manual_sqlmesh_assets],
        resources={"sqlmesh": sqlmesh_resource}
    )
    
    return defs


# Example 4: Using the all-in-one factory with error handling
def example_all_in_one_with_error_handling():
    """Example using the all-in-one factory with error handling."""
    
    defs = sqlmesh_definitions_factory(
        project_dir="tests/sqlmesh_project",
        gateway="duckdb",
        environment="dev",
        name="error_handling_definitions",
        group_name="error_handling",
        op_tags={"team": "data", "error_handling": "enabled"},
        retry_policy=RetryPolicy(max_retries=2),
        owners=["data-team"],
        concurrency_limit=1,
        ignore_cron=True
    )
    
    return defs


if __name__ == "__main__":
    # Example usage
    print("🔧 Creating definitions with error handling...")
    
    # Option 1: Custom resource
    defs1 = example_custom_resource_with_error_handling()
    print("✅ Custom resource definitions created")
    
    # Option 2: Complete definitions
    defs2 = example_complete_definitions_with_error_handling()
    print("✅ Complete definitions with error handling created")
    
    # Option 3: All-in-one factory
    defs3 = example_all_in_one_with_error_handling()
    print("✅ All-in-one factory definitions created")
    
    print("\n📋 Available assets:")
    for asset_key in defs1.assets_by_key.keys():
        print(f"   - {asset_key}")
    
    print("\n🚀 Ready to materialize with error handling!")
    print("   Use: dagster dev -f advanced_usage.py")
