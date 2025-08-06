# ADR-0010: Function Extraction and Modularity

## Status

**Accepted** - 2025-01-27

## Context

During development, we encountered several large functions that were difficult to read, test, and maintain:

1. **`model_asset` function** (270+ lines): Complex asset execution logic mixed with SQLMesh integration
2. **`_process_failed_models_events` function** (100+ lines): Error handling and audit processing
3. **`create_all_asset_specs` function** (80+ lines): Asset specification creation logic

These functions violated the Single Responsibility Principle and made testing difficult. We needed a strategy to improve code maintainability without changing external behavior.

## Decision

We implemented a **Function Extraction and Modularity Pattern** that:

1. **Extracts complex logic into utility functions**: Break down large functions into smaller, focused functions
2. **Maintains external API compatibility**: No changes to public interfaces
3. **Improves testability**: Each extracted function can be tested independently
4. **Enhances readability**: Clear separation of concerns
5. **Follows the Single Responsibility Principle**: Each function has one clear purpose

## Implementation

### Before: Large `model_asset` Function

```python
def model_asset(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
    # 270+ lines of mixed logic:
    # - SQLMesh execution
    # - Result processing
    # - Error handling
    # - Audit processing
    # - Metadata creation
    # - Return value construction
```

### After: Modular Approach

```python
def model_asset(context: AssetExecutionContext, sqlmesh: SQLMeshResource, sqlmesh_results: SQLMeshResultsResource):
    # Main orchestration logic (25 lines)
    run_id = context.run_id
    
    if sqlmesh_results.has_results(run_id):
        return process_sqlmesh_results(context, sqlmesh_results, run_id)
    else:
        results = execute_sqlmesh_materialization(context, sqlmesh, sqlmesh_results, run_id, context.selected_asset_keys)
        return process_sqlmesh_results(context, sqlmesh_results, run_id)
```

### Extracted Utility Functions

```python
# sqlmesh_asset_execution_utils.py
def execute_sqlmesh_materialization(context, sqlmesh, sqlmesh_results, run_id, selected_asset_keys):
    """Handles SQLMesh execution for all selected assets."""

def process_sqlmesh_results(context, sqlmesh_results, run_id):
    """Processes shared SQLMesh results for individual assets."""

def check_model_status(context, current_model_name, current_asset_spec, failed_check_results, skipped_models_events):
    """Determines if model was skipped or has audit failures."""

def handle_audit_failures(context, current_model_name, current_asset_spec, current_model_checks, failed_check_results):
    """Handles audit failures and creates appropriate results."""

def handle_successful_execution(context, current_model_name, current_asset_spec, current_model_checks, evaluation_events):
    """Handles successful execution and creates results."""

def create_materialize_result(context, current_model_name, current_asset_spec, current_model_checks, model_was_skipped, model_has_audit_failures, failed_check_results, evaluation_events):
    """Creates the final MaterializeResult with appropriate metadata."""
```

## Rationale

### Why Function Extraction?

1. **Maintainability**: Smaller functions are easier to understand and modify
2. **Testability**: Each function can be unit tested independently
3. **Reusability**: Extracted functions can be reused in different contexts
4. **Debugging**: Easier to isolate and fix issues in specific functions
5. **Code Review**: Smaller functions are easier to review

### Why Maintain External API?

1. **Backward Compatibility**: Existing code continues to work
2. **Gradual Migration**: Can refactor without breaking changes
3. **Risk Mitigation**: Reduces risk of introducing bugs
4. **User Experience**: No impact on end users

### Alternative Approaches Considered

1. **Class-based Refactoring**: ❌ Would require major API changes
2. **Module-level Refactoring**: ❌ Too coarse-grained
3. **No Refactoring**: ❌ Code would become unmaintainable
4. **Complete Rewrite**: ❌ Too risky, would break existing functionality

## Consequences

### Positive

- ✅ **Improved Maintainability**: Smaller, focused functions
- ✅ **Better Testability**: Each function can be tested independently
- ✅ **Enhanced Readability**: Clear separation of concerns
- ✅ **Easier Debugging**: Issues can be isolated to specific functions
- ✅ **Backward Compatibility**: No breaking changes to public API
- ✅ **Code Reusability**: Extracted functions can be reused

### Negative

- ⚠️ **Increased Complexity**: More files and functions to manage
- ⚠️ **Learning Curve**: New developers need to understand the modular structure
- ⚠️ **Import Dependencies**: More complex import structure
- ⚠️ **Function Call Overhead**: Slight performance impact from additional function calls

### Trade-offs

1. **Complexity vs Maintainability**: We prioritize maintainability over simplicity
2. **Performance vs Readability**: We accept minimal performance impact for better code organization
3. **Learning Curve vs Long-term Benefits**: We accept short-term complexity for long-term maintainability

## Implementation Guidelines

### When to Extract Functions

1. **Function exceeds 50 lines**: Consider extraction
2. **Multiple responsibilities**: Each function should have one clear purpose
3. **Complex conditional logic**: Extract into separate functions
4. **Repeated code patterns**: Extract into utility functions
5. **Difficult to test**: Extract to enable unit testing

### Extraction Patterns

1. **Data Processing**: Extract data transformation logic
2. **Error Handling**: Extract error processing logic
3. **Validation**: Extract validation logic
4. **Formatting**: Extract output formatting logic
5. **Configuration**: Extract configuration logic

### File Organization

```
src/dg_sqlmesh/
├── factory.py                    # Main factory functions
├── resource.py                   # SQLMesh resource
├── translator.py                 # Translation logic
├── sqlmesh_asset_utils.py       # Asset creation utilities
├── sqlmesh_asset_execution_utils.py  # Execution utilities
├── sqlmesh_asset_check_utils.py # Check utilities
└── sqlmesh_event_console.py     # Event handling
```

## Related ADRs

- [ADR-0001](./0001-individual-assets-vs-multi-asset.md) - Individual Assets vs Multi-Asset Pattern
- [ADR-0002](./0002-shared-sqlmesh-execution.md) - Shared SQLMesh Execution per Dagster Run

## Future Considerations

1. **Further Modularization**: Continue extracting complex logic as needed
2. **Interface Design**: Consider creating interfaces for utility functions
3. **Performance Optimization**: Profile and optimize extracted functions
4. **Documentation**: Ensure all extracted functions are well-documented 