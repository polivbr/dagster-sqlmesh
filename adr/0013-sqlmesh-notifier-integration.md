# ADR-0013: Adopt SQLMesh notifier with a custom in-memory target (CapturingNotifier)

- Status: Accepted
- Date: 2025-08-12
- Authors: dg-sqlmesh maintainers
- Supersedes: Legacy custom console event handling
- Related: ADR-0004 Retry policy management

## Context

We initially relied on a custom SQLMesh "console" to capture execution events (e.g., `LogFailedModels`, `LogWarning`, and progress updates). This approach presented several issues:

- Console payloads are loosely structured and differ across SQLMesh versions, making parsing brittle.
- Non-blocking audits often surfaced via warnings, with missing or empty `errors` lists, which led to side effects and extra logic to distinguish blocking vs. non-blocking.
- We needed rich, consistent metadata (audit name, SQL query, arguments, blocking flag, failure count) for Dagster asset checks, which was cumbersome to reconstruct from console events.

SQLMesh provides a native notification system with structured events and explicit audit failure objects (`AuditError`). This is a better fit for robust audit handling.

## Decision

Adopt SQLMesh's notification targets and introduce a custom in-memory target `CapturingNotifier` that captures structured events for use in Dagster:

- Implement `CapturingNotifier` by extending `BaseNotificationTarget` and subscribing to:
  - `AUDIT_FAILURE` (both blocking and non-blocking audits)
  - Run lifecycle: `RUN_START`, `RUN_END`, `RUN_FAILURE`
  - Apply lifecycle: `APPLY_START`, `APPLY_END`, `APPLY_FAILURE`
- Remove all logic and references to the legacy custom console.
- Standardize Dagster `AssetCheckResult` metadata via a single utility `build_audit_check_metadata(...)` for both pass/fail cases.
- Enforce blocking semantics:
  - Blocking audit failure → `AssetCheckResult(passed=False, severity=ERROR)` and block downstream assets.
  - Non-blocking audit failure → `AssetCheckResult(passed=False, severity=WARN)` without blocking downstream.
- Short-circuit downstream assets impacted by blocking failures to avoid long, unnecessary runs.
- Keep retries disabled per ADR-0004 via Dagster tags; the component does not expose any retry policy.

## Implementation

### Notifier

File: `src/dg_sqlmesh/notifier.py`

```python
class CapturingNotifier(BaseNotificationTarget):
    type_: Literal["capturing"] = Field(alias="type", default="capturing")
    notify_on: FrozenSet[NotificationEvent] = frozenset({
        NotificationEvent.AUDIT_FAILURE,
        NotificationEvent.RUN_START,
        NotificationEvent.RUN_END,
        NotificationEvent.RUN_FAILURE,
        NotificationEvent.APPLY_START,
        NotificationEvent.APPLY_END,
        NotificationEvent.APPLY_FAILURE,
    })

    # Private in-memory stores
    _audit_failures: list[dict[str, Any]] = PrivateAttr(default_factory=list)
    _run_events: list[dict[str, Any]] = PrivateAttr(default_factory=list)
    _apply_events: list[dict[str, Any]] = PrivateAttr(default_factory=list)

    def notify_audit_failure(self, audit_error: AuditError, *_, **__) -> None:
        details = extract_failed_audit_details(audit_error)
        self._audit_failures.append({
            "model": details.get("model_name"),
            "audit": details.get("name"),
            "args": details.get("args", {}),
            "count": details.get("count", 0),
            "sql": details.get("sql"),
            "blocking": details.get("blocking", True),
        })
```

Key notes:

- Uses `PrivateAttr(default_factory=list)` to stay compatible with Pydantic frozen models.
- `notify_on` is explicitly typed to satisfy Pydantic field overrides.
- We derive the `blocking` flag with best-effort logic (`is_audit_blocking_from_error`) if needed.

### Integration

File: `src/dg_sqlmesh/resource.py`

- Register `CapturingNotifier` in the SQLMesh `Context` and expose a getter (`_get_or_create_notifier`).
- Map captured notifier audit failures to Dagster checks and compute downstream assets to block.

File: `src/dg_sqlmesh/sqlmesh_asset_execution_utils.py`

- Use notifier failures to build `AssetCheckResult`s with correct severities (ERROR for blocking, WARN for non-blocking).
- Build PASS `AssetCheckResult`s for audits not reported as failures.
- Use `build_audit_check_metadata(...)` to generate consistent, JSON-serializable metadata across pass/fail.
- Immediately short-circuit downstream assets impacted by blocking failures by raising `UpstreamAuditFailureError`.

File: `src/dg_sqlmesh/sqlmesh_asset_check_utils.py`

- Centralize metadata construction in `build_audit_check_metadata(...)` for both pass/fail.
- Provide helpers to extract audit details from `AuditError` and from model definitions.

### Usage

At runtime (resource):

- The resource instantiates/attaches `CapturingNotifier` to the SQLMesh `Context` automatically.
- After a single SQLMesh run (shared by all assets in the selection), we retrieve failures via `notifier.get_audit_failures()`.
- We produce Dagster check results accordingly and compute `affected_downstream_asset_keys` for blocking propagation.

Direct SQLMesh usage (if needed):

```python
from sqlmesh import Context
from dg_sqlmesh.notifier import CapturingNotifier

ctx = Context(paths=".", gateway="postgres")
ctx.notification_targets.append(CapturingNotifier())
ctx._register_notification_targets()
```

### Captured audit failure schema

Each audit failure record has the following keys:

```json
{
  "model": "<model_name>",
  "audit": "<audit_name>",
  "args": {"...": "..."},
  "count": <int>,
  "sql": "<audit_query_sql>",
  "blocking": true | false
}
```

This structured shape allows us to construct high-quality `AssetCheckResult` metadata without fragile parsing.

## Alternatives considered

1. Keep the custom console approach

   - Rejected: brittle parsing, inconsistent payloads, lack of structured data for audits, and maintenance cost.

2. Combine console and notifier

   - Rejected: increases surface area and potential for side effects; notifier already provides the relevant structured data for failures.

3. Parse logs (stdout/stderr) for audit info
   - Rejected: highly brittle and version-dependent; not sustainable.

## Consequences

- Reliability: Higher. We leverage structured `AuditError` objects for failures.
- Simplicity: Cleaner code paths; console logic is removed.
- Observability: Rich, consistent metadata in Dagster for both successes and failures via a single builder.
- Performance: Downstream short-circuiting reduces run duration after blocking failures.
- Backwards compatibility: Requires SQLMesh notifier support; console-based code removed.

## Testing

- Unit tests validate metadata construction (`build_audit_check_metadata`), audit SQL extraction, and blocking detection.
- Integration tests cover end-to-end flow: notifier capture → check results → downstream blocking.

## Migration

- Remove any references to the legacy console (`sqlmesh_event_console.py` has been deleted).
- Ensure components and examples do not expose retry policy knobs; retries remain disabled via tags (see ADR-0004).
- Use notifier failures to derive check severities and metadata; successes are inferred by subtraction (audits not reported as failures are treated as pass).
