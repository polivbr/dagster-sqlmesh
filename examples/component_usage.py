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
        project="sqlmesh_project",
        gateway="postgres",
        environment="prod",
        concurrency_limit=1,
        name="sqlmesh_assets",
        group_name="sqlmesh",
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
      project: "{{ project_root }}/sqlmesh_project"
      gateway: "postgres"
      environment: "prod"
      concurrency_limit: 1
      name: "sqlmesh_assets"
      group_name: "sqlmesh"
      op_tags:
        team: "data"
        env: "prod"
      retry_policy:
        max_retries: 1
        delay: 30.0
        backoff: "exponential"
      schedule_name: "sqlmesh_adaptive_schedule"
      enable_schedule: true
      external_model_key: "target/main/{{ node.name }}"
    ```
    
    Then Dagster will automatically load the component and create the definitions.
    """
    pass


def example_with_retry_policy():
    """Example with retry policy configuration."""
    
    from dagster import RetryPolicy, Backoff
    
    component = SQLMeshProjectComponent(
        project="sqlmesh_project",
        gateway="postgres",
        environment="prod",
        retry_policy=RetryPolicy(
            max_retries=2,
            delay=10.0,
            backoff=Backoff.EXPONENTIAL,
        ),
        enable_schedule=False,
    )
    
    defs = component.build_defs(None)
    return defs


def example_multiple_projects():
    """Example with multiple SQLMesh projects."""
    
    # Staging project
    staging_component = SQLMeshProjectComponent(
        project="staging_sqlmesh_project",
        gateway="postgres",
        environment="staging",
        group_name="staging_sqlmesh",
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
