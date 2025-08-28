# Configuration Guide

This guide covers the main configuration options for dg-sqlmesh, from basic setup to advanced customization.

## Basic Configuration

### SQLMesh Project Directory

The most important configuration is specifying your SQLMesh project directory:

```python
from dg_sqlmesh import sqlmesh_definitions_factory

defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",  # Path to your SQLMesh project
    # ... other options
)
```

### Database Gateway

Specify your database connection:

```python
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",  # or "duckdb", "snowflake", etc.
    # ... other options
)
```

## Advanced Configuration

### External Asset Mapping

Map external SQLMesh sources to Dagster asset keys:

```python
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    external_asset_mapping="target/main/{node.name}",
    # ... other options
)
```

**Available template variables:**
- `{node.database}` - Database name
- `{node.schema}` - Schema name  
- `{node.name}` - Table name
- `{node.fqn}` - Full qualified name

### Concurrency Control

Limit concurrent SQLMesh operations:

```python
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    concurrency_limit=1,  # Only one SQLMesh operation at a time
    # ... other options
)
```

### Asset Grouping

Organize assets into logical groups:

```python
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    group_name="sqlmesh",  # Default group for all assets
    # ... other options
)
```

### Custom Tags

Add metadata to all assets:

```python
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    op_tags={"team": "data", "env": "prod"},
    # ... other options
)
```

### Retry Policy

Configure retry behavior for failed operations:

```python
from dagster import RetryPolicy, Backoff

defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=30.0,
        backoff=Backoff.EXPONENTIAL
    ),
    # ... other options
)
```

### Scheduling

Enable adaptive scheduling based on SQLMesh crons:

```python
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    enable_schedule=True,  # Creates adaptive schedule
    # ... other options
)
```

## Custom Translator

For advanced customization, create a custom translator:

```python
from dg_sqlmesh import SQLMeshTranslator
from dagster import AssetKey

class CustomTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> AssetKey:
        """Custom mapping for external assets."""
        parts = external_fqn.split('.')
        return AssetKey(['custom'] + parts[1:])
    
    def get_group_name(self, context, model) -> str:
        """Custom group naming logic."""
        if model.name.startswith("stg_"):
            return "staging"
        return "production"

# Use custom translator
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    translator=CustomTranslator(),
    # ... other options
)
```

## Environment Configuration

### Development vs Production

```python
import os

# Environment-specific configuration
env = os.getenv("ENVIRONMENT", "dev")

if env == "prod":
    defs = sqlmesh_definitions_factory(
        project_dir="sqlmesh_project",
        gateway="postgres",
        concurrency_limit=1,
        group_name="sqlmesh_prod",
        op_tags={"env": "prod", "team": "data"},
        enable_schedule=True,
    )
else:
    defs = sqlmesh_definitions_factory(
        project_dir="sqlmesh_project",
        gateway="duckdb",
        concurrency_limit=2,
        group_name="sqlmesh_dev",
        op_tags={"env": "dev", "team": "data"},
        enable_schedule=False,
    )
```

## Complete Example

Here's a complete configuration example:

```python
from dagster import RetryPolicy, Backoff
from dg_sqlmesh import sqlmesh_definitions_factory

defs = sqlmesh_definitions_factory(
    # Required
    project_dir="sqlmesh_project",
    gateway="postgres",
    
    # Asset configuration
    external_asset_mapping="target/main/{node.name}",
    concurrency_limit=1,
    group_name="sqlmesh",
    
    # Metadata
    op_tags={
        "team": "data",
        "env": "prod",
        "project": "analytics"
    },
    
    # Error handling
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=60.0,
        backoff=Backoff.EXPONENTIAL
    ),
    
    # Scheduling
    enable_schedule=True,
)
```

## Configuration Best Practices

1. **Start Simple**: Begin with basic configuration and add complexity as needed
2. **Environment Variables**: Use environment variables for sensitive configuration
3. **Concurrency**: Always set `concurrency_limit=1` for production SQLMesh
4. **Retry Policy**: Configure retries based on your database's behavior
5. **Tags**: Use consistent tagging for better organization and monitoring
6. **External Assets**: Map external sources to logical Dagster asset keys

## Next Steps

- Learn about [Core Concepts](../user-guide/core-concepts.md)
- See [Basic Usage Examples](../examples/basic-usage.md)
- Explore [Advanced Features](../user-guide/advanced-features.md)
