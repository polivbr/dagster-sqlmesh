# ADR 0012: SQLMesh Dagster Component

## Status

Accepted

## Context

The dg-sqlmesh library provides integration between SQLMesh and Dagster, but users currently need to use Python code to configure their SQLMesh projects. This requires:

1. **Python knowledge**: Users must understand Python to configure their projects
2. **Code maintenance**: Configuration is embedded in Python files
3. **Limited reusability**: Each project needs custom Python code
4. **Complex setup**: Users need to understand the factory functions and their parameters

The dagster-dbt library provides a declarative YAML-based component system that allows users to configure dbt projects without writing Python code. This approach is more user-friendly and follows modern DevOps practices.

## Decision

We will implement a **SQLMesh Dagster Component** that provides declarative YAML configuration for SQLMesh projects, similar to the dagster-dbt component.

### Key Design Decisions

#### 1. Component Structure

- **Component Class**: `SQLMeshProjectComponent` inheriting from `Component` and `Resolvable`
- **Scaffolder**: `SQLMeshProjectComponentScaffolder` for project scaffolding
- **YAML Format**: Following dagster-dbt pattern with `type:` and `attributes:`

#### 2. Configuration Grouping

- **SQLMesh Config**: Group SQLMesh-specific parameters under `sqlmesh_config`
  - `project_path`: Path to SQLMesh project
  - `gateway`: Database gateway (postgres, duckdb, etc.)
  - `environment`: SQLMesh environment (prod, dev, etc.)

#### 3. Parameter Naming

- **Clarity**: Rename parameters for better understanding
  - `concurrency_limit` → `concurrency_jobs_limit`
  - `group_name` → `default_group_name`
  - Remove redundant `name` parameter (fixed to "sqlmesh_assets")

#### 4. Default Values

- **Smart Defaults**: Provide sensible defaults for optional parameters
  - `schedule_name`: "sqlmesh_adaptive_schedule"
  - `enable_schedule`: `True` (creates but doesn't activate)
  - `concurrency_jobs_limit`: `1`
  - `default_group_name`: "sqlmesh"

#### 5. External Asset Mapping

- **Jinja2 Templating**: Support for flexible external asset mapping
- **Parameter**: `external_asset_mapping` for direct factory usage
- **Conflict Resolution**: Custom translator takes priority over external mapping
- **Template Format**: `{node.name}` in YAML, converted to `{{ node.name }}` internally

#### 6. ADR Compliance

- **Retry Policy**: Excluded from component config (per ADR-0004)
- **Component Discovery**: Register with Dagster component registry
- **Documentation**: Comprehensive docstrings and UI documentation

## Consequences

### Positive

1. **User Experience**: Declarative configuration reduces complexity
2. **Consistency**: Follows established dagster-dbt patterns
3. **Maintainability**: YAML configuration is easier to version control
4. **Reusability**: Components can be shared across projects
5. **Documentation**: Better UI documentation in Dagster interface
6. **Flexibility**: Jinja2 templating for external asset mapping

### Negative

1. **Breaking Changes**: Existing users need to migrate their configuration
2. **Complexity**: Additional abstraction layer
3. **Learning Curve**: Users need to understand YAML structure
4. **Limitations**: Some advanced features require Python code

### Migration Impact

Users with existing Python-based configurations need to:

1. **Update Parameter Names**: Follow new naming convention
2. **Restructure Configuration**: Group SQLMesh parameters
3. **Update Examples**: Modify documentation and examples
4. **Test Compatibility**: Ensure existing functionality works

### Technical Considerations

1. **Dagster Components API**: Limited by current API capabilities

   - `Callable` types not supported in `Annotated` fields
   - `RetryPolicy` not "model compliant"
   - Complex nested objects require simplification

2. **Dependency Management**: Added `jinja2>=3.0.0` dependency

3. **Entry Points**: Added Dagster component registry entry point

## Implementation Details

### Component Registration

```python
# In __init__.py
DagsterLibraryRegistry.register("dg-sqlmesh", __version__)

# In pyproject.toml
[project.entry-points]
"dagster_dg_cli.registry_modules" = { "dg_sqlmesh" = "dg_sqlmesh" }
```

### YAML Structure

```yaml
type: dg_sqlmesh.SQLMeshProjectComponent

attributes:
  sqlmesh_config:
    project_path: "{{ project_root }}/sqlmesh_project"
    gateway: "postgres"
    environment: "prod"
  concurrency_jobs_limit: 1
  default_group_name: "sqlmesh"
  external_asset_mapping: "target/main/{node.name}"
```

### External Asset Mapping

```python
# Factory function enhancement
def sqlmesh_definitions_factory(
    # ... existing parameters
    external_asset_mapping: Optional[str] = None,
):
    # Conflict resolution logic
    if translator and external_asset_mapping:
        warnings.warn("CONFLICT DETECTED: Custom translator takes priority")

    if external_asset_mapping and not translator:
        translator = JinjaSQLMeshTranslator(external_asset_mapping)
```

### Jinja2 Template Processing

```python
class JinjaSQLMeshTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, fqdn: str) -> AssetKey:
        # Convert {node.name} to {{ node.name }} for Jinja2
        template = self.external_asset_mapping_template.replace(
            "{node.name}", "{{ node.name }}"
        )
        # Render template with node context
        return AssetKey(template.render(node=node_info).split("/"))
```

## Testing Strategy

1. **Unit Tests**: Comprehensive testing of `JinjaSQLMeshTranslator`
2. **Integration Tests**: Component discovery and registration
3. **Factory Tests**: External asset mapping functionality
4. **Migration Tests**: Ensure backward compatibility

## Documentation

1. **Component Documentation**: Detailed docstrings for UI
2. **Migration Guide**: Step-by-step migration instructions
3. **Examples**: Updated examples with new YAML structure
4. **Changelog**: Comprehensive release notes

## Future Considerations

1. **Advanced Templating**: More complex Jinja2 templates
2. **Custom Translators**: Support for custom Python translators
3. **Component Extensions**: Additional component types
4. **Validation**: Enhanced YAML validation and error messages

## References

- [ADR-0004: Retry Policy Management](0004-retry-policy-management.md)
- [dagster-dbt Component Documentation](https://docs.dagster.io/integrations/dbt/dbt-components)
- [Dagster Components API](https://docs.dagster.io/concepts/components)
- [SQLMesh Documentation](https://sqlmesh.com/)
