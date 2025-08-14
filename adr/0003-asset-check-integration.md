# ADR-0003: Asset Check Integration for SQLMesh Audits

## Status

**Accepted** - 2025-08-05

## Context

SQLMesh models can have audits that validate data quality. These audits need to be exposed in Dagster's UI as asset checks. The challenge is to distinguish between:

1. **Materialization failures** (model didn't execute)
2. **Audit failures** (model executed but audits failed)

## Decision

**Use Dagster AssetCheckResult to represent SQLMesh audits, with proper failure handling.**

## Rationale

### Materialization vs Audit Failure

SQLMesh distinguishes between:

- **Model execution failure**: Model didn't materialize (upstream failure, syntax error, etc.)
- **Audit failure**: Model materialized successfully but audits failed

### Dagster Integration Strategy

1. **Materialization Success + Non-Blocking Audit Failure**: Asset materializes (green) with failed checks (WARN severity). Downstream assets CONTINUE. No blocking.
2. **Materialization Failure**: Asset fails (red) and downstream assets fail
3. **Audit Success**: Asset materializes (green) with passed checks
4. **Blocking Audit Failure**: When an audit marked as blocking fails, the upstream asset is treated as failing for orchestration purposes and downstream assets are blocked.

## Implementation

### Asset Check Creation

```python
# Create asset checks from SQLMesh audits
model_checks = create_asset_checks_from_model(model, asset_key)

@asset(
    check_specs=model_checks,  # Dagster asset checks
    # ... other parameters
)
def model_asset():
    # ... execution logic
```

### Failure Handling Logic

```python
# Check if model was skipped due to upstream failure
if model_was_skipped:
    raise Exception(f"Model {model_name} was skipped due to upstream failures")

# Check if model materialized but audits failed
elif model_has_audit_failures:
    # Asset materializes successfully but with failed checks
    return MaterializeResult(
        asset_key=asset_key,
        check_results=[
            AssetCheckResult(
                check_name=check.name,
                passed=False,
                metadata={
                    "audit_message": audit_message,
                    "sqlmesh_audit_name": check.name,
                    "error_details": f"SQLMesh audit '{check.name}' failed: {audit_message}"
                }
            )
            for check in model_checks
        ]
    )
else:
    # Full success - materialization and audits passed
    return MaterializeResult(
        asset_key=asset_key,
        check_results=[
            AssetCheckResult(
                check_name=check.name,
                passed=True
            )
            for check in model_checks
        ]
    )
```

### Selection strategy for non-subsettable execution sets

Some `AssetsDefinition` instances behave as non-subsettable execution blocks when they include both the asset and its `AssetCheckSpec`s. In these cases, Dagster may raise `DagsterInvalidSubsetError` if only a subset of the checks (or assets) is selected by an ephemeral job (e.g., UI "materialize all").

To guarantee that all checks targeting each selected asset are included, we expand the selection to required neighbors within the same non-subsettable block:

```python
from dagster import AssetSelection

selected_assets = AssetSelection.assets(*(key for ad in sqlmesh_assets for key in ad.keys))
safe_selection = selected_assets.required_multi_asset_neighbors()
```

This ensures:

- All checks that belong to the same non-subsettable execution set are included alongside the selected assets
- Ephemeral jobs do not attempt illegal subsetting of checks, avoiding `DagsterInvalidSubsetError`
- The behavior is stable regardless of how the job is triggered (UI or code)

## Architecture Diagram

```mermaid
graph TD
    A[SQLMesh Model] --> B[SQLMesh Audits]
    B --> C[Dagster Asset Checks]

    D[Asset Execution] --> E{Model Skipped?}
    E -->|Yes| F[Raise Exception]
    E -->|No| G{Model Materialized?}

    G -->|No| H[Asset Failed]
    G -->|Yes| I{Audits Passed?}

    I -->|No| J[Asset Success + Failed Checks (Non-Blocking)]
    I -->|Yes| K[Asset Success + Passed Checks]

    F --> L[Downstream Assets Fail]
    H --> L
    J --> N[Downstream Assets Continue]
    K --> N[Downstream Assets Continue]

    style J fill:#fff3e0
    style K fill:#e8f5e8
    style F fill:#ffebee
    style H fill:#ffebee
```

## Consequences

### Positive

- ✅ **Proper failure distinction** - Materialization vs audit failures
- ✅ **Dagster UI integration** - Asset checks visible in UI
- ✅ **Downstream failure propagation** - Blocking audit failures affect downstream; non-blocking audit failures do not
- ✅ **Detailed error messages** - Audit failure details in metadata
- ✅ **Non-blocking checks** - Assets can succeed with failed checks

### Negative

- ⚠️ **Complex failure logic** - Need to distinguish failure types
- ⚠️ **UI complexity** - Users must understand asset vs check status

## Current Limitations

## Related Decisions

- [ADR-0001: Individual Assets vs Multi-Asset Pattern](./0001-individual-assets-vs-multi-asset.md)
- [ADR-0002: Shared SQLMesh Execution](./0002-shared-sqlmesh-execution.md)
- [ADR-0004: Retry Policy Management](./0004-retry-policy-management.md)
