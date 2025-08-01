# Test Plan for dg-sqlmesh

## 🎯 **Objective**

Implement comprehensive test coverage for dg-sqlmesh by following patterns from `dagster-dbt_tests` while using our unique SQLMesh project.

## 📁 **Test Structure**

```
tests/
├── conftest.py                    # Main fixtures and configuration
├── sqlmesh_project/               # Our SQLMesh test project (existing)
│   ├── config.yaml               # DuckDB configuration
│   ├── external_models.yaml      # External models
│   ├── models/                   # SQLMesh models
│   ├── audits/                   # Audits
│   └── tests/                    # SQLMesh tests
├── core/                         # Core component tests
│   ├── test_factory.py           # Factory function tests
│   ├── test_resource.py          # SQLMeshResource tests
│   ├── test_translator.py        # SQLMeshTranslator tests
│   ├── test_asset_utils.py       # Asset utilities tests
│   └── test_event_console.py     # Event console tests
├── integration/                   # Integration tests
│   ├── test_asset_execution.py   # Asset execution tests
│   ├── test_schedules.py         # Schedule tests
│   └── test_definitions.py       # Complete definitions tests
└── utils/                        # Test utilities
    ├── test_data_loader.py       # Test data loading
    └── test_helpers.py           # Helper functions
```

## 🚀 **Implementation Plan**

### **Phase 1 : Base Infrastructure** ✅

- [x] Existing SQLMesh test project
- [x] Create `conftest.py` with main fixtures
- [x] Implement `test_factory.py` (17 tests passing)
- [x] Implement `test_resource.py` (20 tests passing)

### **Phase 2 : Core Tests** 🔄

- [ ] Implement `test_translator.py`
- [ ] Implement `test_asset_utils.py`
- [ ] Implement `test_event_console.py`

### **Phase 3 : Integration Tests** ⏳

- [ ] Implement `test_asset_execution.py`
- [ ] Implement `test_schedules.py`
- [ ] Implement `test_definitions.py`

### **Phase 4 : Advanced Tests** ⏳

- [ ] Performance and cache tests
- [ ] Error and exception tests
- [ ] Metadata and configuration tests

## 🧪 **Detailed Tests**

### **Factory Tests** (`test_factory.py`)

#### Basic Tests

- [ ] `test_sqlmesh_assets_factory_basic()` - Basic asset creation
- [ ] `test_sqlmesh_assets_factory_with_selection()` - Asset selection
- [ ] `test_sqlmesh_assets_factory_with_exclude()` - Asset exclusion
- [ ] `test_sqlmesh_definitions_factory()` - Complete definitions factory
- [ ] `test_sqlmesh_adaptive_schedule_factory()` - Adaptive schedule factory

#### Parameter Tests

- [ ] `test_factory_partitions_def()` - Partitioning tests
- [ ] `test_factory_io_manager_key()` - IO manager key tests
- [ ] `test_factory_retry_policy()` - Retry policy tests
- [ ] `test_factory_op_tags()` - Operation tags tests

### **Resource Tests** (`test_resource.py`)

#### Configuration Tests

- [ ] `test_sqlmesh_resource_creation()` - Resource creation
- [ ] `test_sqlmesh_resource_project_dir()` - Project configuration
- [ ] `test_sqlmesh_resource_gateway()` - Gateway configuration
- [ ] `test_sqlmesh_resource_concurrency()` - Concurrency limit

#### Execution Tests

- [ ] `test_sqlmesh_resource_execution()` - Command execution
- [ ] `test_sqlmesh_resource_cache()` - Cache behavior
- [ ] `test_sqlmesh_resource_errors()` - Error handling
- [ ] `test_sqlmesh_resource_context()` - Context management

### **Translator Tests** (`test_translator.py`)

#### Mapping Tests

- [ ] `test_translator_asset_keys()` - Asset key mapping
- [ ] `test_translator_external_assets()` - External assets
- [ ] `test_translator_normalization()` - Key normalization
- [ ] `test_translator_customization()` - Translator customization

### **Integration Tests** (`test_asset_execution.py`)

#### Execution Tests

- [ ] `test_asset_materialization()` - Asset materialization
- [ ] `test_asset_dependencies()` - Asset dependencies
- [ ] `test_asset_selection_execution()` - Execution with selection
- [ ] `test_asset_partitioned_execution()` - Partitioned execution

### **Schedule Tests** (`test_schedules.py`)

#### Adaptive Schedule Tests

- [ ] `test_adaptive_schedule_creation()` - Schedule creation
- [ ] `test_cron_parsing()` - SQLMesh cron parsing
- [ ] `test_schedule_execution()` - Schedule execution

## 🔧 **Fixture Configuration**

### **conftest.py** - Main Fixtures

```python
# Fixtures for SQLMesh project
@pytest.fixture(scope="session")
def sqlmesh_project_path() -> Path:
    return Path("tests/sqlmesh_project")

@pytest.fixture(scope="session")
def sqlmesh_context(sqlmesh_project_path: Path):
    from sqlmesh import Context
    return Context(path=sqlmesh_project_path)

@pytest.fixture(scope="session")
def sqlmesh_resource(sqlmesh_project_path: Path):
    from dg_sqlmesh import SQLMeshResource
    return SQLMeshResource(project_dir=sqlmesh_project_path)

# Fixtures for manifests
@pytest.fixture(scope="session")
def sqlmesh_manifest(sqlmesh_context):
    # Generate SQLMesh manifest
    pass
```

## 📊 **Success Metrics**

- [ ] **Code coverage** : > 90%
- [ ] **Unit tests** : All core components
- [ ] **Integration tests** : Complete workflows
- [ ] **Performance tests** : Cache and optimizations
- [ ] **Error tests** : Exception handling

## 🎯 **Priorities**

1. **High priority** : Factory and resource tests (Phase 1-2)
2. **Medium priority** : Integration tests (Phase 3)
3. **Low priority** : Advanced tests (Phase 4)

## 📝 **Notes**

- Use existing SQLMesh project as base
- Follow patterns from `dagster-dbt_tests`
- Maintain consistency with code standards
- All tests in English (code and documentation)
- Use DuckDB for tests (already configured)
