# ADR-0009: Shared State Management Pattern

## Status

**Accepted** - 2025-01-27

## Context

In a Dagster run with multiple SQLMesh assets, we need to ensure that:
1. Only one SQLMesh execution occurs per run (for efficiency and consistency)
2. All assets in the same run share the same execution results
3. Each asset can access the results of the shared execution
4. The state is properly isolated between different runs

The challenge is that Dagster assets are designed to be independent, but SQLMesh execution is most efficient when done once per run with all selected models.

## Decision

We implemented a **Shared State Management Pattern** using a dedicated `SQLMeshResultsResource` that:

1. **Stores execution results per run**: Uses the Dagster `run_id` as the key for storing results
2. **Shares state between assets**: All assets in the same run can access the same results
3. **Ensures single execution**: Only the first asset in a run triggers SQLMesh execution
4. **Provides thread-safe access**: Uses proper resource management for concurrent access

## Implementation

### SQLMeshResultsResource

```python
class SQLMeshResultsResource(ConfigurableResource):
    """Resource pour partager les résultats SQLMesh entre les assets d'un même run."""

    def __init__(self):
        super().__init__()
        self._results = {}

    def store_results(self, run_id: str, results: Dict[str, Any]) -> None:
        """Stocke les résultats SQLMesh pour un run donné."""
        self._results[run_id] = results

    def get_results(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Récupère les résultats SQLMesh pour un run donné."""
        return self._results.get(run_id)

    def has_results(self, run_id: str) -> bool:
        """Vérifie si des résultats existent pour un run donné."""
        return run_id in self._results
```

### Usage in Assets

```python
def model_asset(context: AssetExecutionContext, sqlmesh: SQLMeshResource, sqlmesh_results: SQLMeshResultsResource):
    run_id = context.run_id
    
    # Check if results already exist for this run
    if sqlmesh_results.has_results(run_id):
        # Use existing results
        results = sqlmesh_results.get_results(run_id)
        return process_sqlmesh_results(context, sqlmesh_results, run_id)
    else:
        # First asset in run - execute SQLMesh
        results = execute_sqlmesh_materialization(context, sqlmesh, sqlmesh_results, run_id, context.selected_asset_keys)
        return process_sqlmesh_results(context, sqlmesh_results, run_id)
```

## Rationale

### Why Shared State?

1. **Efficiency**: SQLMesh execution is expensive, so we want to do it once per run
2. **Consistency**: All assets in the same run should see the same execution results
3. **Dependency Management**: SQLMesh handles dependencies internally, so we need to respect that
4. **Resource Optimization**: Avoid multiple SQLMesh context initializations

### Why SQLMeshResultsResource?

1. **Dagster Integration**: Uses Dagster's resource system for proper lifecycle management
2. **Thread Safety**: ConfigurableResource provides thread-safe access
3. **Run Isolation**: Each run has its own isolated state
4. **Type Safety**: Pydantic provides validation and type safety

### Alternative Approaches Considered

1. **Global Variables**: ❌ Not thread-safe, no run isolation
2. **Database Storage**: ❌ Overkill, adds complexity
3. **File-based Storage**: ❌ Performance issues, cleanup complexity
4. **Dagster Metadata**: ❌ Limited size, not designed for large results

## Consequences

### Positive

- ✅ **Efficient Execution**: Single SQLMesh execution per run
- ✅ **Consistent Results**: All assets see the same execution state
- ✅ **Proper Isolation**: Each run has its own state
- ✅ **Thread Safety**: Safe for concurrent access
- ✅ **Dagster Integration**: Uses standard Dagster patterns

### Negative

- ⚠️ **Memory Usage**: Results stored in memory for the duration of the run
- ⚠️ **Complexity**: Additional resource to manage
- ⚠️ **Debugging**: Harder to debug individual asset execution
- ⚠️ **State Persistence**: Results lost after run completion

### Trade-offs

1. **Memory vs Performance**: We prioritize performance over memory usage
2. **Complexity vs Efficiency**: We accept additional complexity for better efficiency
3. **Debugging vs Consistency**: We prioritize consistency over individual debugging

## Related ADRs

- [ADR-0002](./0002-shared-sqlmesh-execution.md) - Shared SQLMesh Execution per Dagster Run
- [ADR-0005](./0005-custom-sqlmesh-console.md) - Custom SQLMesh Console for Event Capture

## Future Considerations

1. **Result Cleanup**: Implement automatic cleanup of old results
2. **Size Limits**: Add size limits to prevent memory issues
3. **Persistence**: Consider persistent storage for long-running processes
4. **Monitoring**: Add metrics for resource usage and performance 