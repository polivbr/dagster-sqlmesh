# ADR-0001: Individual Assets vs Multi-Asset Pattern

## Status

**Accepted** - 2025-08-05

## Context

SQLMesh models need to be exposed as Dagster assets for orchestration. The initial approach used `@multi_asset` to group all SQLMesh models into a single asset definition.

## Decision

**Use individual `@asset` definitions for each SQLMesh model instead of `@multi_asset`.**

## Rationale

### Problems with Multi-Asset Approach

1. **No Granular Control**: Cannot materialize or fail individual assets independently
2. **All-or-Nothing Execution**: When one model fails, the entire multi-asset fails
3. **Limited Dagster UI Integration**: Individual model status not visible in UI
4. **No Selective Materialization**: Cannot materialize specific models without running all

### Benefits of Individual Assets

1. **Granular Control**: Each SQLMesh model becomes a separate Dagster asset
2. **Independent Success/Failure**: Individual assets can succeed or fail independently
3. **Better UI Experience**: Each model visible as separate asset in Dagster UI
4. **Selective Materialization**: Can materialize specific models or groups
5. **Proper Dependency Management**: Dagster's native dependency resolution works correctly

## Implementation

```python
# Instead of one multi-asset
@multi_asset(
    specs=[AssetSpec("model1"), AssetSpec("model2")]
)
def sqlmesh_assets():
    # All models in one function
    pass

# Use individual assets
@asset(key="model1")
def model1_asset():
    pass

@asset(key="model2")
def model2_asset():
    pass
```

## Consequences

### Positive

- ✅ **Granular control** over individual model success/failure
- ✅ **Better Dagster UI integration** with individual asset visibility
- ✅ **Proper dependency management** between models
- ✅ **Selective materialization** capabilities
- ✅ **Individual asset checks** and audit results

### Negative

- ⚠️ **More complex asset creation** (loop to create individual assets)
- ⚠️ **Potential performance overhead** (more asset definitions)
- ⚠️ **Shared execution complexity** (need SQLMeshResultsResource)

## Related Decisions

- [ADR-0002: Shared SQLMesh Execution](./0002-shared-sqlmesh-execution.md)
- [ADR-0003: Asset Check Integration](./0003-asset-check-integration.md) 