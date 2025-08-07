"""
Demo of SQLMesh Dagster Component usage.
"""

from pathlib import Path
from dagster import Definitions
from dagster import ComponentTree

from dg_sqlmesh import SQLMeshProjectComponent


def demo_basic_component():
    """Demonstrate basic component usage."""
    
    # Create a component with basic configuration
    component = SQLMeshProjectComponent(
        sqlmesh_config={
            "project_path": "tests/fixtures/sqlmesh_project",
            "gateway": "duckdb",
            "environment": "dev",
        },
        enable_schedule=False,
    )
    
    print("✅ Basic component created successfully")
    print(f"   Project: {component.sqlmesh_config.project_path}")
    print(f"   Gateway: {component.sqlmesh_config.gateway}")
    print(f"   Environment: {component.sqlmesh_config.environment}")
    print(f"   Translator: {type(component.translator).__name__}")
    
    return component


def demo_external_asset_mapping():
    """Demonstrate external asset mapping."""
    
    # Create a component with external asset mapping
    component = SQLMeshProjectComponent(
        sqlmesh_config={
            "project_path": "tests/fixtures/sqlmesh_project",
            "gateway": "duckdb",
            "environment": "dev",
        },
        external_asset_mapping="target/main/{node.name}",
        enable_schedule=False,
    )
    
    print("✅ Component with external asset mapping created successfully")
    print(f"   External asset mapping template: {component.external_asset_mapping}")
    print(f"   Translator type: {type(component.translator).__name__}")
    
    # Test the translator
    from dg_sqlmesh.components.sqlmesh_project.component import JinjaSQLMeshTranslator
    assert isinstance(component.translator, JinjaSQLMeshTranslator)
    
    # Test with a sample external FQN
    asset_key = component.translator.get_external_asset_key('"jaffle_db"."main"."raw_source_customers"')
    print(f"   Sample mapping: 'jaffle_db.main.raw_source_customers' -> {asset_key}")
    
    return component


def demo_with_custom_configuration():
    """Demonstrate component with custom configuration."""
    
    from dagster import RetryPolicy, Backoff
    
    # Create a component with custom configuration
    component = SQLMeshProjectComponent(
        sqlmesh_config={
            "project_path": "tests/fixtures/sqlmesh_project",
            "gateway": "duckdb",
            "environment": "dev",
        },
        default_group_name="custom_sqlmesh",
        op_tags={"team": "data", "env": "dev"},
        enable_schedule=False,
    )
    
    print("✅ Component with custom configuration created successfully")
    print(f"   Group: {component.default_group_name}")
    print(f"   Tags: {component.op_tags}")
    
    return component


def demo_build_definitions():
    """Demonstrate building Dagster definitions from component."""
    
    component = SQLMeshProjectComponent(
        sqlmesh_config={
            "project_path": "tests/fixtures/sqlmesh_project",
            "gateway": "duckdb",
            "environment": "dev",
        },
        enable_schedule=False,
    )
    
    # Build definitions
    defs = component.build_defs()
    
    print("✅ Definitions built successfully")
    print(f"   Number of assets: {len(defs.assets)}")
    print(f"   Number of resources: {len(defs.resources)}")
    print(f"   Number of jobs: {len(defs.jobs)}")
    print(f"   Number of schedules: {len(defs.schedules)}")
    
    return defs


if __name__ == "__main__":
    print("🚀 SQLMesh Dagster Component Demo")
    print("=" * 50)
    
    # Demo 1: Basic component
    print("\n1. Basic Component:")
    demo_basic_component()
    
    # Demo 2: External model key
    print("\n2. External Model Key Mapping:")
    demo_external_asset_mapping()
    
    # Demo 3: Custom configuration
    print("\n3. Custom Configuration:")
    demo_with_custom_configuration()
    
    # Demo 4: Build definitions
    print("\n4. Build Definitions:")
    demo_build_definitions()
    
    print("\n✅ All demos completed successfully!")
    print("\nThe SQLMesh Dagster Component is ready to use!")
