# ADR-0014: dg-sqlmesh refactoring phases (0→9)

## Status

Accepted — 2025-08-13

## Context

The `dg-sqlmesh` module had grown organically, concentrating orchestration, execution, and audit logic in a few large files. Goals were to:

- Preserve public API stability
- Improve separation of concerns and readability
- Enable unit-first testing per helper function
- Adopt SQLMesh's notifier for reliable audit capture (blocking and non-blocking)
- Keep a single SQLMesh execution per Dagster run and share results across assets

## Decisions

- Consolidate around a notifier-only flow; remove legacy console paths
- Map SQLMesh audits to Dagster AssetChecks with consistent metadata
- Use ERROR severity for blocking audit failures and WARN for non-blocking
- Block downstream assets upon upstream blocking audit failures
- Ensure per-run isolation by clearing notifier state between runs
- Use `required_multi_asset_neighbors()` to avoid check subsetting issues in Dagster jobs
- Keep public factories/resources/translators stable

## Refactoring Phases Summary

- Phase 0 — Hygiene

  - Translate logs/docstrings to English; remove dead imports/paths

- Phase 1 — Execution utilities split

  - Extract micro-functions from execution flow; unit-test each helper

- Phase 2 — Slim `resource.py`

  - Move notifier creation/registration into `notifier_service`
  - Delegate audit serialization/deduplication to dedicated helpers

- Phase 3 — Utilities consolidation

  - Centralize audit metadata and conversion in `sqlmesh_asset_check_utils.py`
  - Remove duplicated helpers and legacy console code paths

- Phase 4 — Logging and exceptions

  - Standardize messages and explicit exceptions (no silent failures)

- Phase 5 — Types and naming

  - Reinforce type hints and consistent naming

- Phase 6 — Public API check

  - Keep `sqlmesh_definitions_factory`, `sqlmesh_assets_factory`, `SQLMeshResource`, `SQLMeshTranslator` stable

- Phase 7 — Tests

  - Unit tests per helper; integration tests for blocking/non-blocking audits and downstream blocking behavior

- Phase 8 — Docs and ADRs

  - Update docs, add notifier ADR; document selection rationale below

- Phase 9 — Utilities re-organization
  - Split large utilities into domain-focused modules (no logic changes):
    - `execution_selection.py` (selection/orchestration)
    - `execution_notifier.py` (notifier access and summaries)
    - `execution_downstream.py` (blocking and downstream computation)
    - `execution_check_results.py` (check results assembly)
    - `execution_results_payload.py` (shared results structure)
  - Preserve function names and call sites; adjust imports only

## Rationale — Asset selection in Dagster jobs

Some `AssetsDefinition`s that include both an asset and its `AssetCheckSpec`s behave as non-subsettable execution blocks. Ephemeral jobs may attempt to subset checks independently, leading to `DagsterInvalidSubsetError`. We expand the selection to required neighbors to guarantee all checks for each selected asset are included:

```python
from dagster import AssetSelection

selected_assets = AssetSelection.assets(*(key for ad in sqlmesh_assets for key in ad.keys))
safe_selection = selected_assets.required_multi_asset_neighbors()
```

This avoids subsetting errors and ensures checks follow selected assets.

## Non-Regression Guarantees

- Blocking audit failure → AssetCheck severity=ERROR and downstream block
- Non-blocking audit failure → AssetCheck severity=WARN; downstream continues
- Unified metadata for PASS/FAIL across checks
- One SQLMesh execution per run; results shared via resource
- Notifier-based capture; no legacy console

## Risks and Mitigations

- Risk: behavioral drift during extraction → Mitigation: unit tests per helper and integration tests for key flows
- Risk: hidden coupling with previous imports → Mitigation: keep shim functions where tests monkeypatch module symbols

## Consequences

- Clearer module boundaries and easier unit testing
- Deterministic per-run behavior (no notifier state leakage)
- Stable public API with improved internals

## References

- ADR-0003: Asset check integration for SQLMesh audits
- ADR-0013: SQLMesh notifier integration
- Source modules: `resource.py`, `sqlmesh_asset_execution_utils.py`, `sqlmesh_asset_check_utils.py`
