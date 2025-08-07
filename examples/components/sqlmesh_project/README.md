# SQLMesh Dagster Component

This component provides a declarative way to integrate SQLMesh projects with Dagster using YAML configuration.

## Overview

The SQLMesh component allows you to expose SQLMesh models as Dagster assets through a simple YAML configuration file (`defs.yaml`). This provides a clean separation between your SQLMesh project and your Dagster orchestration.

## Features

- **Declarative Configuration**: Configure SQLMesh integration through YAML
- **Automatic Asset Creation**: SQLMesh models become Dagster assets automatically
- **Audit Integration**: SQLMesh audits become Dagster asset checks
- **Adaptive Scheduling**: Automatic schedule creation based on SQLMesh crons
- **Custom Translators**: Support for custom translation functions
- **Scaffolding**: Generate new SQLMesh projects with `dagster scaffold`

## Quick Start

### 1. Scaffold a New SQLMesh Project

```bash
# Create a new SQLMesh project
dagster scaffold component dg_sqlmesh.SQLMeshProjectComponent --init

# Or scaffold with an existing project
dagster scaffold component dg_sqlmesh.SQLMeshProjectComponent --project-path path/to/your/sqlmesh_project
```

### 2. Configure the Component

Create a `defs.yaml` file in your Dagster project:

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
```

### 3. Use in Your Dagster Project

The component will automatically create:

- SQLMesh assets from your models
- Asset checks from your audits
- Adaptive schedule based on your crons
- SQLMesh resource for execution

## Configuration Options

### Required Parameters

- **project**: Path to your SQLMesh project directory

### Optional Parameters

- **gateway**: SQLMesh gateway (default: "postgres")
- **environment**: SQLMesh environment (default: "prod")
- **concurrency_limit**: Execution concurrency limit (default: 1)
- **name**: Name for the assets (default: "sqlmesh_assets")
- **group_name**: Group name for assets (default: "sqlmesh")
- **op_tags**: Tags to apply to assets
- **retry_policy**: Retry policy configuration
- **schedule_name**: Name for the adaptive schedule
- **enable_schedule**: Whether to enable scheduling (default: false)

### Retry Policy Configuration

```yaml
retry_policy:
  max_retries: 1
  delay: 30.0
  backoff: "exponential" # or "linear"
```

### Project Configuration

You can specify the project as a simple string or with detailed configuration:

```yaml
# Simple string
project: "{{ project_root }}/sqlmesh_project"

# Detailed configuration
project:
  project_dir: "{{ project_root }}/sqlmesh_project"
  gateway: "postgres"
  environment: "prod"
  concurrency_limit: 1
```

## Advanced Usage

### External Asset Key Mapping

You can configure how external assets (like Sling assets) are mapped to Dagster asset keys using Jinja2 templates:

```yaml
type: dg_sqlmesh.SQLMeshProjectComponent

attributes:
  project: "{{ project_root }}/sqlmesh_project"
  external_model_key: "target/main/{{ node.name }}"
```

#### Template Variables

The following variables are available in the Jinja2 template:

- **`{{ node.database }}`**: The database name (e.g., "jaffle_db")
- **`{{ node.schema }}`**: The schema name (e.g., "main")  
- **`{{ node.name }}`**: The table/view name (e.g., "raw_source_customers")
- **`{{ node.fqn }}`**: The full qualified name

#### Examples

```yaml
# Map to Sling format: target/main/table_name
external_model_key: "target/main/{{ node.name }}"

# Keep original structure but prefix with "sling"
external_model_key: "sling/{{ node.database }}/{{ node.schema }}/{{ node.name }}"

# Use only the table name
external_model_key: "{{ node.name }}"

# Custom mapping for specific database
external_model_key: "{% if node.database == 'jaffle_db' %}target/main/{{ node.name }}{% else %}{{ node.database }}/{{ node.schema }}/{{ node.name }}{% endif %}"
```

### Custom Translation Functions

You can provide custom translation functions for asset keys, groups, and tags:

```yaml
components:
  sqlmesh_project:
    module: dg_sqlmesh.SQLMeshProjectComponent
    config:
      project: "{{ project_root }}/sqlmesh_project"
      translation:
        fn: "my_translation_module.custom_translation_fn"
        # This function will receive the base value and model data
```

### Multiple SQLMesh Projects

You can configure multiple SQLMesh projects in the same Dagster instance:

```yaml
# defs.yaml
type: dg_sqlmesh.SQLMeshProjectComponent

attributes:
  project: "{{ project_root }}/staging_sqlmesh_project"
  environment: "staging"
  group_name: "staging_sqlmesh"

# defs_prod.yaml
type: dg_sqlmesh.SQLMeshProjectComponent

attributes:
  project: "{{ project_root }}/production_sqlmesh_project"
  environment: "prod"
  group_name: "production_sqlmesh"
```

## SQLMesh Project Structure

The component expects a standard SQLMesh project structure:

```
sqlmesh_project/
├── config.yaml          # SQLMesh configuration
├── models/              # SQL models
│   ├── stg/
│   ├── marts/
│   └── ...
├── seeds/               # Seed data
├── audits/              # Custom audits
└── macros/              # Custom macros
```

## Development Workflow

1. **Development**: Use SQLMesh CLI for model development

   ```bash
   sqlmesh plan dev
   sqlmesh apply dev
   sqlmesh run dev
   ```

2. **Production**: Use Dagster for orchestration
   - The component automatically runs `sqlmesh run prod`
   - Schedules handle execution based on crons
   - Asset checks provide monitoring

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure `dg-sqlmesh` is installed
2. **Project Path**: Verify the SQLMesh project path is correct
3. **Gateway Configuration**: Check your SQLMesh gateway settings
4. **Environment Issues**: Ensure the specified environment exists

### Debugging

Enable debug logging in your Dagster configuration:

```yaml
dagster:
  logging:
    level: DEBUG
```

## Migration from Direct Factory Usage

If you're currently using `sqlmesh_definitions_factory` directly, you can migrate to the component:

### Before (Python)

```python
from dg_sqlmesh import sqlmesh_definitions_factory

defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",
    enable_schedule=True,
)
```

### After (YAML)

```yaml
components:
  sqlmesh_project:
    module: dg_sqlmesh.SQLMeshProjectComponent
    config:
      project: "{{ project_root }}/sqlmesh_project"
      gateway: "postgres"
      enable_schedule: true
```

## Contributing

The component follows the same patterns as the dbt component. Key files:

- `component.py`: Main component implementation
- `scaffolder.py`: Scaffolding logic for new projects
- `defs.yaml`: Example configuration
- `README.md`: This documentation
