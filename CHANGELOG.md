# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.0] - 2025-08-12

### Added

- SQLMesh notifier integration with custom in-memory target `CapturingNotifier`
  - Structured capture of audit failures (blocking and non-blocking)
  - Run/apply lifecycle events (start/end/failure)
- ADR-0013: SQLMesh notifier adoption and rationale

### Changed

- Asset check metadata standardized via `build_audit_check_metadata(...)` for pass/fail
- Downstream short-circuiting when upstream blocking audits fail to reduce run time

### Removed

- Legacy custom console and all related event-processing code
- YAML `retry_policy` exposure in examples and component specs (kept disabled per ADR-0004)

### Documentation

- Updated component docs and examples to remove retry policy and reference ADR-0004
- Added notifier notes and usage details

## [1.5.0] - 2025-08-13

### Added

- Support for non-blocking audits surfaced as Dagster checks with WARN severity (suffix `_non_blocking`)
- Integration tests covering blocking vs non-blocking audits and downstream blocking behavior
- `CapturingNotifier.clear()` and `notifier_service.clear_notifier_state()` for per-run isolation

### Changed

- Refactor: slimmer `resource.py`, consolidated helpers in `sqlmesh_asset_check_utils.py` and `sqlmesh_asset_execution_utils.py`
- Job selection uses `required_multi_asset_neighbors()` to include all checks for non-subsettable execution blocks
- Logs and docstrings standardized in English across modules

### Fixed

- Prevent cross-run contamination by clearing notifier state between runs
- Remove CLI `invalidate` in tests to avoid DuckDB locks; rely on `execution_time` advancement

### Documentation

- ADR-0003 updated: rationale for `required_multi_asset_neighbors()` selection strategy
- ADR-0013 updated: explicit mention of `notifier_service` and singleton registration

## [1.3.0] - 2025-01-27

### Added

- **SQLMesh Dagster Component**: New declarative YAML configuration system
  - `SQLMeshProjectComponent` for easy SQLMesh project integration
  - Support for Jinja2 templating in external asset mapping
  - Automatic schedule creation with configurable defaults
  - Component discovery and registration with Dagster

### Enhanced

- **External Asset Mapping**: New `external_asset_mapping` parameter
  - Direct parameter support in `sqlmesh_definitions_factory`
  - Jinja2 templating for flexible asset key mapping
  - Conflict resolution between custom translators and external mapping
  - Comprehensive test coverage for mapping scenarios

### Improved

- **Component Configuration**: Restructured YAML specification
  - Grouped SQLMesh parameters under `sqlmesh_config`
  - Renamed parameters for clarity (`concurrency_limit` → `concurrency_jobs_limit`)
  - Added default values for optional parameters
  - Removed redundant `name` parameter

### Changed

- **Default Values**: Updated component defaults
  - `schedule_name` defaults to `"sqlmesh_adaptive_schedule"`
  - `enable_schedule` defaults to `True` (creates but doesn't activate)
  - `concurrency_jobs_limit` defaults to `1`
  - `default_group_name` defaults to `"sqlmesh"`

### Documentation

- **Component Documentation**: Enhanced UI documentation
  - Detailed docstring for `SQLMeshProjectComponent`
  - Comprehensive parameter descriptions and examples
  - External asset mapping usage guide
  - Migration guide from direct factory usage

### Testing

- **Test Coverage**: Comprehensive test suite
  - Unit tests for `JinjaSQLMeshTranslator`
  - Integration tests for external asset mapping
  - Component discovery and registration tests
  - Cleaned up obsolete test files

### Technical

- **Dependencies**: Added `jinja2>=3.0.0` for templating support
- **Entry Points**: Added Dagster component registry entry point
- **Code Organization**: Moved dagster-dbt example to `code_example/`

### Breaking Changes

- **Component API**: Updated parameter names and structure
  - `project` → `sqlmesh_config.project_path`
  - `gateway` → `sqlmesh_config.gateway`
  - `environment` → `sqlmesh_config.environment`
  - `group_name` → `default_group_name`
  - `concurrency_limit` → `concurrency_jobs_limit`
  - Removed `name` parameter (fixed to `"sqlmesh_assets"`)
  - Removed `retry_policy` (per ADR-0004)

### Migration Guide

To migrate from direct factory usage to the new component:

```yaml
# Old format
project: "{{ project_root }}/sqlmesh_project"
gateway: "postgres"
environment: "prod"
concurrency_limit: 1
group_name: "sqlmesh"

# New format
sqlmesh_config:
  project_path: "{{ project_root }}/sqlmesh_project"
  gateway: "postgres"
  environment: "prod"
concurrency_jobs_limit: 1
default_group_name: "sqlmesh"
```

## [1.2.2] - 2025-01-20

### Added

- Initial release of dg-sqlmesh
- Core SQLMesh integration with Dagster
- Asset factory functions
- Resource management
- Translator system for SQLMesh to Dagster mapping
- Comprehensive test suite
- Documentation and examples
