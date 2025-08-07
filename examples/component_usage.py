"""
Example usage of the SQLMesh Dagster component.

This example shows how to use the SQLMesh component both programmatically
and through YAML configuration.
"""

from dagster import Definitions
from dg_sqlmesh import SQLMeshProjectComponent


def example_programmatic_usage():
    """Example of using the SQLMesh component programmatically."""
    
    # Create the component
    component = SQLMeshProjectComponent(
        sqlmesh_config={
            "project_path": "sqlmesh_project",
            "gateway": "postgres",
            "environment": "prod",
        },
        concurrency_jobs_limit=1,
        default_group_name="sqlmesh",
        op_tags={"team": "data", "env": "prod"},
        enable_schedule=True,
    )
    
    # Build definitions
    defs = component.build_defs(None)  # No context needed for this example
    
    return defs


def example_yaml_usage():
    """
    Example of using the SQLMesh component through YAML configuration.
    
    Create a defs.yaml file with:
    
    ```yaml
    type: dg_sqlmesh.SQLMeshProjectComponent
    
    attributes:
      sqlmesh_config:
        project_path: "{{ project_root }}/sqlmesh_project"
        gateway: "postgres"
        environment: "prod"
      concurrency_jobs_limit: 1
      default_group_name: "sqlmesh"
      op_tags:
        team: "data"
        env: "prod"
      # schedule_name and enable_schedule are optional with defaults
      # schedule_name: "sqlmesh_adaptive_schedule"  # default value
      # enable_schedule: true  # default value (creates schedule but doesn't activate it)
      external_asset_mapping: "target/main/{node.name}"
    ```
    
    Then Dagster will automatically load the component and create the definitions.
    """
    pass


def example_with_retry_policy():
    """Example with retry policy configuration."""
    
    # Note: RetryPolicy is no longer supported in the component
    # Use the factory function directly if you need retry policy
    
    component = SQLMeshProjectComponent(
        sqlmesh_config={
            "project_path": "sqlmesh_project",
            "gateway": "postgres",
            "environment": "prod",
        },
        enable_schedule=False,
    )
    
    defs = component.build_defs(None)
    return defs


def example_multiple_projects():
    """Example with multiple SQLMesh projects."""
    
    # Staging project
    staging_component = SQLMeshProjectComponent(
        sqlmesh_config={
            "project_path": "staging_sqlmesh_project",
            "gateway": "postgres",
            "environment": "staging",
        },
        default_group_name="staging_sqlmesh",
        enable_schedule=False,
    )
    
    # Production project
    production_component = SQLMeshProjectComponent(
        project="production_sqlmesh_project",
        gateway="postgres",
        environment="prod",
        group_name="production_sqlmesh",
        enable_schedule=True,
    )
    
    # Combine definitions
    staging_defs = staging_component.build_defs(None)
    production_defs = production_component.build_defs(None)
    
    # Merge assets, resources, and schedules
    combined_defs = Definitions(
        assets=staging_defs.assets + production_defs.assets,
        resources={**staging_defs.resources, **production_defs.resources},
        schedules=staging_defs.schedules + production_defs.schedules,
    )
    
    return combined_defs


if __name__ == "__main__":
    # Example usage
    defs = example_programmatic_usage()
    print(f"Created {len(defs.assets)} SQLMesh assets")
    print(f"Created {len(defs.schedules)} schedules")
    print(f"Created {len(defs.resources)} resources")
