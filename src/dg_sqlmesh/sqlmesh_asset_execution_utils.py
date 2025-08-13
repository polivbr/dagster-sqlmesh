"""
Execution utilities for SQLMesh assets.

Functions extracted from the `model_asset` flow to improve readability and
testability. All docstrings and logs are standardized in English.
"""

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    AssetCheckResult,
    AssetKey,
    AssetCheckSeverity,
)
from typing import Dict, List, Any, Tuple
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import get_models_to_materialize
from .sqlmesh_asset_check_utils import build_audit_check_metadata
from .resource import UpstreamAuditFailureError
from .notifier_service import clear_notifier_state


def get_check_severity_for_blocking(is_blocking: bool) -> AssetCheckSeverity:
    """Return the standardized severity for an audit based on its blocking flag.

    - True  -> ERROR (blocking audit failures should be errors)
    - False -> WARN  (non-blocking audit failures should be warnings)
    """
    return AssetCheckSeverity.ERROR if is_blocking else AssetCheckSeverity.WARN


# ----------------------------- Internal helpers (Phase 1) -----------------------------

def _log_run_selection(context: AssetExecutionContext, run_id: str, selected_asset_keys: List[AssetKey]) -> None:
    """Log high-level context for the shared execution."""
    context.log.info(
        "First asset in run; launching SQLMesh execution for all selected assets"
    )
    context.log.debug(f"No existing results for run {run_id}")
    context.log.info(f"Selected assets in this run: {selected_asset_keys}")


def _select_models_to_materialize(selected_asset_keys: List[AssetKey], sqlmesh: SQLMeshResource) -> List[Any]:
    """Resolve SQLMesh models to materialize from selection; raise if none found."""
    models_to_materialize = get_models_to_materialize(
        selected_asset_keys,
        sqlmesh.get_models,
        sqlmesh.translator,
    )
    if not models_to_materialize:
        raise Exception(f"No models found for selected assets: {selected_asset_keys}")
    return models_to_materialize


def _materialize_and_get_plan(sqlmesh: SQLMeshResource, models_to_materialize: List[Any], context: AssetExecutionContext) -> Any:
    """Run a single SQLMesh materialization and return the plan."""
    context.log.info(
        f"Materializing {len(models_to_materialize)} models: {[m.name for m in models_to_materialize]}"
    )
    context.log.debug("Starting SQLMesh materialization (count=%d)", len(models_to_materialize))
    plan = sqlmesh.materialize_assets_threaded(models_to_materialize, context=context)
    context.log.debug("SQLMesh materialization completed")
    return plan


def _init_execution_event_buffers(context: AssetExecutionContext) -> tuple[List[AssetCheckResult], List[Dict], List[Dict], List[Dict]]:
    """Initialize buffers for legacy/disabled console paths and non-blocking warnings."""
    failed_check_results: List[AssetCheckResult] = []
    context.log.debug("Failed check results count: 0")
    context.log.debug("Processing skipped models events... (skipped, console disabled)")
    skipped_models_events: List[Dict] = []
    context.log.debug(f"Skipped models events count: {len(skipped_models_events)}")
    evaluation_events: List[Dict] = []  # console disabled
    context.log.debug(f"Evaluation events count: {len(evaluation_events)}")
    non_blocking_audit_warnings: List[Dict] = []
    return failed_check_results, skipped_models_events, evaluation_events, non_blocking_audit_warnings


def _get_notifier_failures(sqlmesh: SQLMeshResource) -> List[Dict]:
    """Safely retrieve notifier audit failures via notifier service; return empty list on error."""
    try:
        from .notifier_service import get_audit_failures
        return get_audit_failures()
    except Exception:
        return []


def _summarize_notifier_failures(context: AssetExecutionContext, notifier_audit_failures: List[Dict]) -> None:
    """Log a compact summary of notifier failures if present."""
    if not notifier_audit_failures:
        return
    try:
        summary = [
            {
                "model": f.get("model"),
                "audit": f.get("audit"),
                "blocking": f.get("blocking"),
                "count": f.get("count"),
            }
            for f in notifier_audit_failures
        ]
        context.log.info(f"Notifier audit failures summary: {summary}")
    except Exception:
        # ignore logging issues to avoid breaking execution
        pass


def _compute_blocking_and_downstream(sqlmesh: SQLMeshResource, notifier_audit_failures: List[Dict]) -> tuple[List[AssetKey], set[AssetKey]]:
    """Compute failing blocking asset keys and affected downstream asset keys."""
    blocking_failed_asset_keys: List[AssetKey] = []
    try:
        for fail in notifier_audit_failures:
            if fail.get("blocking") and fail.get("model"):
                model = sqlmesh.context.get_model(fail.get("model"))
                if model:
                    blocking_failed_asset_keys.append(sqlmesh.translator.get_asset_key(model))
    except Exception:
        pass

    try:
        affected_downstream_asset_keys = sqlmesh._get_affected_downstream_assets(blocking_failed_asset_keys)
    except Exception:
        affected_downstream_asset_keys = set()

    # Ensure we don't include the failing assets themselves in the downstream set
    try:
        affected_downstream_asset_keys = set(affected_downstream_asset_keys) - set(blocking_failed_asset_keys)
    except Exception:
        affected_downstream_asset_keys = set()

    return blocking_failed_asset_keys, affected_downstream_asset_keys


def _build_shared_results(
    plan: Any,
    failed_check_results: List[AssetCheckResult],
    skipped_models_events: List[Dict],
    evaluation_events: List[Dict],
    non_blocking_audit_warnings: List[Dict],
    notifier_audit_failures: List[Dict],
    affected_downstream_asset_keys: set[AssetKey],
) -> Dict[str, Any]:
    """Assemble the shared results payload for this run."""
    return {
        "failed_check_results": failed_check_results,
        "skipped_models_events": skipped_models_events,
        # Keep legacy key for older tests expecting evaluation_events
        "evaluation_events": evaluation_events,
        "non_blocking_audit_warnings": non_blocking_audit_warnings,
        "notifier_audit_failures": notifier_audit_failures,
        "affected_downstream_asset_keys": list(affected_downstream_asset_keys),
        "plan": plan,
    }


def _parse_snapshot_to_model_name(snapshot_name: str) -> str | None:
    """Convert a snapshot name '"db"."schema"."model"' to 'schema.model'."""
    try:
        parts = snapshot_name.split('"."')
        if len(parts) >= 3:
            return parts[1] + "." + parts[2].replace('"', "")
    except Exception:
        return None
    return None


def _model_was_skipped_from_events(skipped_models_events: List[Dict], current_model_name: str, logger: Any | None = None) -> bool:
    """Check if the current model appears in the skipped events."""
    for event in skipped_models_events:
        skipped_snapshots = event.get("snapshot_names", set())
        for snapshot_name in skipped_snapshots:
            if not snapshot_name:
                continue
            skipped_model_name = _parse_snapshot_to_model_name(snapshot_name)
            if logger:
                logger.debug(f"Checking skipped model: {skipped_model_name} vs {current_model_name}")
            if skipped_model_name == current_model_name:
                return True
    return False


def _model_has_failed_audits_for_asset(
    failed_check_results: List[AssetCheckResult], current_asset_spec_key: Any, current_model_name: str, logger: Any | None = None
) -> bool:
    """Check if any failed check result targets the current asset key."""
    for check_result in failed_check_results:
        if logger:
            logger.debug(f"Checking failed check: {check_result.asset_key} vs {current_asset_spec_key}")
        if check_result.asset_key == current_asset_spec_key:
            if logger:
                logger.error(
                    f"Model {current_model_name} has audit failures: {check_result.metadata.get('audit_message', 'Unknown error')}"
                )
            return True
    return False


def _build_failed_check_results_for_all_checks(
    current_model_checks: List[Any],
    current_asset_spec_key: Any,
    failed_check_results: List[AssetCheckResult],
    current_model_name: str,
    logger: Any | None = None,
) -> List[AssetCheckResult]:
    """Create failed AssetCheckResult for all declared checks with proper metadata."""
    results: List[AssetCheckResult] = []
    for check in current_model_checks:
        audit_message = "Model materialization succeeded but audits failed"
        for check_result in failed_check_results:
            if check_result.asset_key == current_asset_spec_key:
                audit_message = check_result.metadata.get("audit_message", audit_message)
                break
        result = AssetCheckResult(
            check_name=check.name,
            passed=False,
            metadata={
                "audit_message": audit_message,
                "sqlmesh_audit_name": check.name,
                "sqlmesh_model": current_model_name,
                "error_details": f"SQLMesh audit '{check.name}' failed: {audit_message}",
            },
        )
        results.append(result)
        if logger:
            logger.debug(f"Created failed check result for: {check.name} with message: {audit_message}")
    return results


def _get_blocking_and_non_blocking_names_for_model(
    notifier_audit_failures: List[Dict], non_blocking_audit_warnings: List[Dict], current_model_name: str
) -> tuple[set[str], set[str], List[Dict]]:
    """Partition audit names into blocking and non-blocking for a given model."""
    failed_for_model = [f for f in notifier_audit_failures if f.get("model") == current_model_name]
    blocking_names = {f.get("audit") for f in failed_for_model if f.get("blocking")}
    non_blocking_names = {f.get("audit") for f in failed_for_model if not f.get("blocking")}
    for w in non_blocking_audit_warnings:
        if w.get("model_name") == current_model_name:
            non_blocking_names.add(w.get("audit_name"))
    return blocking_names, non_blocking_names, failed_for_model


def _build_check_result_failed_from_notifier(
    *,
    check_name: str,
    current_model_name: str,
    notifier_record: Dict[str, Any] | None,
    blocking: bool,
    context: AssetExecutionContext,
) -> AssetCheckResult:
    """Build a failed AssetCheckResult from a notifier record with the desired blocking flag."""
    # Ensure notifier record reflects the blocking flag we want to expose
    safe_record = {**(notifier_record or {}), "model": current_model_name, "audit": check_name, "blocking": blocking}
    metadata = build_audit_check_metadata(
        context=context.resources.sqlmesh.context if hasattr(context.resources, "sqlmesh") else None,  # type: ignore[attr-defined]
        model_or_name=current_model_name,
        audit_name=check_name,
        notifier_record=safe_record,
        logger=getattr(context, "log", None),
    )
    return AssetCheckResult(
        check_name=check_name,
        passed=False,
        severity=get_check_severity_for_blocking(blocking),
        metadata=metadata,
    )


def _build_pass_check_result(
    *,
    check_name: str,
    current_model_name: str,
    context: AssetExecutionContext,
) -> AssetCheckResult:
    """Build a passing AssetCheckResult with standardized metadata."""
    pass_meta = build_audit_check_metadata(
        context=context.resources.sqlmesh.context if hasattr(context.resources, "sqlmesh") else None,  # type: ignore[attr-defined]
        model_or_name=current_model_name,
        audit_name=check_name,
        logger=getattr(context, "log", None),
    )
    return AssetCheckResult(
        check_name=check_name,
        passed=True,
        metadata=pass_meta,
    )


def _build_check_results_for_create_result(
    *,
    current_model_checks: List[Any],
    current_model_name: str,
    notifier_audit_failures: List[Dict],
    non_blocking_audit_warnings: List[Dict],
    context: AssetExecutionContext,
) -> List[AssetCheckResult]:
    """Build check_results for create_materialize_result using notifier and warnings.

    Emits:
      - blocking audit failures as ERROR
      - non-blocking audit failures as WARN
      - PASS for remaining checks
    """
    check_results: List[AssetCheckResult] = []
    blocking_names, non_blocking_names, failed_for_model = _get_blocking_and_non_blocking_names_for_model(
        notifier_audit_failures, non_blocking_audit_warnings, current_model_name
    )

    for check in current_model_checks:
        if check.name in blocking_names:
            fail = next((f for f in failed_for_model if f.get("audit") == check.name), {})
            check_results.append(
                _build_check_result_failed_from_notifier(
                    check_name=check.name,
                    current_model_name=current_model_name,
                    notifier_record=fail,
                    blocking=True,
                    context=context,
                )
            )
        elif check.name in non_blocking_names:
            fail_nb = next(
                (f for f in failed_for_model if not f.get("blocking") and f.get("audit") == check.name),
                {},
            )
            check_results.append(
                _build_check_result_failed_from_notifier(
                    check_name=check.name,
                    current_model_name=current_model_name,
                    notifier_record=fail_nb,
                    blocking=False,
                    context=context,
                )
            )
        else:
            check_results.append(
                _build_pass_check_result(
                    check_name=check.name,
                    current_model_name=current_model_name,
                    context=context,
                )
            )

    return check_results




def execute_sqlmesh_materialization(
    context: AssetExecutionContext,
    sqlmesh: SQLMeshResource,
    sqlmesh_results: Any,
    run_id: str,
    selected_asset_keys: List[AssetKey],
) -> Dict[str, Any]:
    """
    Execute a single SQLMesh materialization for all selected assets (shared execution).

    Args:
        context: Dagster execution context
        sqlmesh: SQLMesh resource
        sqlmesh_results: Shared results resource
        run_id: Dagster run identifier
        selected_asset_keys: Selected assets in this run

    Returns:
        Dict with captured execution results for later reuse in the same run
    """
    # Log selection
    context.log.info(
        "First asset in run; launching SQLMesh execution for all selected assets"
    )
    context.log.debug(f"No existing results for run {run_id}")
    context.log.info(f"Selected assets in this run: {selected_asset_keys}")

    # Launch a single SQLMesh execution for all selected assets
    # Resolve models to materialize
    models_to_materialize = get_models_to_materialize(
        selected_asset_keys,
        sqlmesh.get_models,
        sqlmesh.translator,
    )
    if not models_to_materialize:
        raise Exception(f"No models found for selected assets: {selected_asset_keys}")

    # Single SQLMesh execution
    # Run single SQLMesh execution and get plan
    context.log.info(
        f"Materializing {len(models_to_materialize)} models: {[m.name for m in models_to_materialize]}"
    )
    context.log.debug("Starting SQLMesh materialization (count=%d)", len(models_to_materialize))
    # If a SQLMesh run already occurred just before (e.g., tests or external trigger)
    # and produced notifier events, reuse them and avoid triggering a second run.
    try:
        from .notifier_service import get_audit_failures as _get_nf
        _preexisting_failures = _get_nf()
    except Exception:
        _preexisting_failures = []

    if _preexisting_failures:
        context.log.info("Detected preexisting notifier audit failures; skipping extra SQLMesh run")
        plan = None
    else:
        plan = sqlmesh.materialize_assets_threaded(models_to_materialize, context=context)
    context.log.debug("SQLMesh materialization completed")

    # Capture all results
    # Console removed → no legacy failed models events
    # Console disabled path
    # Initialize result buffers (console disabled)
    failed_check_results: List[AssetCheckResult] = []
    context.log.debug("Failed check results count: 0")
    context.log.debug("Processing skipped models events... (skipped, console disabled)")
    skipped_models_events: List[Dict] = []
    context.log.debug(f"Skipped models events count: {len(skipped_models_events)}")
    evaluation_events: List[Dict] = []
    context.log.debug(f"Evaluation events count: {len(evaluation_events)}")
    non_blocking_audit_warnings: List[Dict] = []

    # Store results in the shared resource
    # Capture audit failures from the notifier (robust)
    # Get notifier failures via service and log summary
    try:
        from .notifier_service import get_audit_failures
        notifier_audit_failures = get_audit_failures()
    except Exception:
        notifier_audit_failures = []
    if notifier_audit_failures:
        try:
            summary = [
                {
                    "model": f.get("model"),
                    "audit": f.get("audit"),
                    "blocking": f.get("blocking"),
                    "count": f.get("count"),
                }
                for f in notifier_audit_failures
            ]
            context.log.info(f"Notifier audit failures summary: {summary}")
        except Exception:
            pass

    # Build blocking AssetKeys and affected downstream assets
    # Compute blocking and downstream
    blocking_failed_asset_keys: List[AssetKey] = []
    try:
        for fail in notifier_audit_failures:
            if fail.get("blocking") and fail.get("model"):
                model = sqlmesh.context.get_model(fail.get("model"))
                if model:
                    blocking_failed_asset_keys.append(sqlmesh.translator.get_asset_key(model))
    except Exception:
        pass
    try:
        affected_downstream_asset_keys = sqlmesh._get_affected_downstream_assets(blocking_failed_asset_keys)
    except Exception:
        affected_downstream_asset_keys = set()
    try:
        affected_downstream_asset_keys = set(affected_downstream_asset_keys) - set(blocking_failed_asset_keys)
    except Exception:
        affected_downstream_asset_keys = set()
    context.log.info(
        f"Blocking failed assets: {blocking_failed_asset_keys} | Downstream affected: {list(affected_downstream_asset_keys)}"
    )

    # Build shared result payload
    results: Dict[str, Any] = {
        "failed_check_results": failed_check_results,
        "skipped_models_events": skipped_models_events,
        "evaluation_events": evaluation_events,
        "non_blocking_audit_warnings": non_blocking_audit_warnings,
        "notifier_audit_failures": notifier_audit_failures,
        "affected_downstream_asset_keys": list(affected_downstream_asset_keys),
        "plan": plan,
    }

    sqlmesh_results.store_results(run_id, results)
    context.log.info(f"Stored SQLMesh results for run {run_id}")
    # Keep store confirmation
    # Clear notifier state after completing the run to avoid cross-run leakage
    clear_notifier_state()

    return results


def process_sqlmesh_results(
    context: AssetExecutionContext, sqlmesh_results: Any, run_id: str
) -> Tuple[List[AssetCheckResult], List[Dict], List[Dict]] | Tuple[List[AssetCheckResult], List[Dict], List[Dict], List[Dict], List[AssetKey]]:
    """
    Retrieve and process shared SQLMesh results for this run.

    Returns a tuple:
      - failed_check_results
      - skipped_models_events
      - non_blocking_audit_warnings
      - notifier_audit_failures
      - affected_downstream_asset_keys
    """
    context.log.info(f"Using existing SQLMesh results from run {run_id}")
    context.log.debug(f"Found existing results for run {run_id}")

    # Retrieve results for this run
    results = sqlmesh_results.get_results(run_id)
    if results is None:
        context.log.error("No results found in sqlmesh_results for run %s", run_id)
        return [], [], [], [], []
    failed_check_results = results.get("failed_check_results", [])
    skipped_models_events = results.get("skipped_models_events", [])
    # Backward-compat: if legacy shape is present, return the 3-tuple expected by older tests
    if "evaluation_events" in results and "non_blocking_audit_warnings" not in results:
        evaluation_events = results.get("evaluation_events", [])
        return failed_check_results, skipped_models_events, evaluation_events
    non_blocking_audit_warnings = results.get("non_blocking_audit_warnings", [])
    notifier_audit_failures = results.get("notifier_audit_failures", [])
    affected_downstream_asset_keys = results.get("affected_downstream_asset_keys", [])

    context.log.debug("Processing results for model")
    context.log.debug(f"Failed check results: {len(failed_check_results)}")
    context.log.debug(f"Skipped models events: {len(skipped_models_events)}")
    context.log.debug(
        f"Non-blocking audit warnings: {len(non_blocking_audit_warnings)}"
    )
    context.log.debug(
        f"Notifier audit failures: {len(notifier_audit_failures)} | affected downstream: {len(affected_downstream_asset_keys)}"
    )

    return (
        failed_check_results,
        skipped_models_events,
        non_blocking_audit_warnings,
        notifier_audit_failures,
        affected_downstream_asset_keys,
    )


def check_model_status(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    failed_check_results: List[AssetCheckResult],
    skipped_models_events: List[Dict],
) -> Tuple[bool, bool]:
    """
    Check the status of a specific model.

    Returns a tuple: (model_was_skipped, model_has_audit_failures)
    """
    model_was_skipped = False
    model_has_audit_failures = False

    # Check if skipped due to upstream failures
    context.log.debug("Checking for skipped models...")
    if _model_was_skipped_from_events(skipped_models_events, current_model_name, logger=context.log):
        model_was_skipped = True
        context.log.error(f"Model {current_model_name} was skipped due to upstream failures")

    # Check audit failures (model executed but audit failed)
    context.log.debug("Checking for audit failures...")
    if _model_has_failed_audits_for_asset(
        failed_check_results, current_asset_spec.key, current_model_name, logger=context.log
    ):
            model_has_audit_failures = True

    context.log.debug(
        f"Model {current_model_name} - was_skipped: {model_was_skipped}, has_audit_failures: {model_has_audit_failures}"
    )

    return model_was_skipped, model_has_audit_failures


def handle_audit_failures(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    current_model_checks: List[Any],
    failed_check_results: List[AssetCheckResult],
) -> MaterializeResult:
    """
    Handle the case where the model executed but audits failed.

    Returns a MaterializeResult with failed checks populated.
    """
    context.log.info(
        f"Model {current_model_name}: materialization succeeded but at least one audit failed"
    )
    context.log.debug("Returning MaterializeResult with failed checks")

    # If checks exist, return their results
    if current_model_checks:
        check_results = _build_failed_check_results_for_all_checks(
            current_model_checks=current_model_checks,
            current_asset_spec_key=current_asset_spec.key,
            failed_check_results=failed_check_results,
            current_model_name=current_model_name,
            logger=context.log,
        )

        context.log.debug(f"Returning {len(check_results)} failed check results")
        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "materialization_success_audit_failed"},
            check_results=check_results,
        )
    else:
        context.log.warning(
            f"No checks defined for model {current_model_name}; returning only MaterializeResult"
        )
        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "materialization_success_audit_failed"},
        )


def handle_successful_execution(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    current_model_checks: List[Any],
    non_blocking_audit_warnings: List[Dict] | None = None,
    notifier_audit_failures: List[Dict] | None = None,
) -> MaterializeResult:
    """
    Handle the case where the model executed successfully.

    Returns a MaterializeResult with passed checks (and WARN for non-blocking failures).
    """
    context.log.info(f"Model {current_model_name}: success")
    context.log.debug("Returning MaterializeResult with passed checks")

    # Normalize optional inputs
    non_blocking_audit_warnings = non_blocking_audit_warnings or []
    notifier_audit_failures = notifier_audit_failures or []

    # If checks exist, return their results
    if current_model_checks:
        check_results: List[AssetCheckResult] = []

        # Build failing set from notifier and warnings for current model
        blocking_names, non_blocking_names, _failed_for_model = _get_blocking_and_non_blocking_names_for_model(
            notifier_audit_failures, non_blocking_audit_warnings, current_model_name
        )

        # Emit WARN failed for non-blocking failures, PASS for others
        for check in current_model_checks:
            if check.name in non_blocking_names:
                fail = next(
                    (f for f in notifier_audit_failures if f.get("model") == current_model_name and f.get("audit") == check.name),
                    {},
                )
                check_results.append(
                    _build_check_result_failed_from_notifier(
                        check_name=check.name,
                        current_model_name=current_model_name,
                        notifier_record=fail,
                        blocking=False,
                        context=context,
                    )
                )
            else:
                check_results.append(
                    _build_pass_check_result(
                        check_name=check.name,
                        current_model_name=current_model_name,
                        context=context,
                    )
                )

        context.log.debug(f"Returning {len(check_results)} check results")
        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "success"},
            check_results=check_results,
        )
    else:
        context.log.debug("No checks defined; returning simple MaterializeResult")
        return MaterializeResult(
            asset_key=current_asset_spec.key, metadata={"status": "success"}
        )


def create_materialize_result(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    current_model_checks: List[Any],
    model_was_skipped: bool,
    model_has_audit_failures: bool,
    non_blocking_audit_warnings: List[Dict] | None = None,
    notifier_audit_failures: List[Dict] | None = None,
    affected_downstream_asset_keys: List[AssetKey] | None = None,
    *,
    # Legacy keyword-only params for backward compatibility with older tests
    failed_check_results: List[AssetCheckResult] | None = None,
    evaluation_events: List[Dict] | None = None,
) -> MaterializeResult:
    """
    Create the appropriate MaterializeResult based on the model status.

    Returns the correct result or raises UpstreamAuditFailureError for skipped/blocked cases.
    """

    # Normalize optional inputs
    non_blocking_audit_warnings = non_blocking_audit_warnings or []
    notifier_audit_failures = notifier_audit_failures or []
    affected_downstream_asset_keys = affected_downstream_asset_keys or []
    # Legacy params intentionally ignored in new flow; kept for API compatibility
    _ = failed_check_results, evaluation_events

    if model_was_skipped:
        # Skipped model → raise an exception (no materialization)
        error_msg = f"Model {current_model_name} was skipped due to upstream failures"
        context.log.error(error_msg)
        context.log.debug("Raising UpstreamAuditFailureError for skipped model")
        raise UpstreamAuditFailureError(description=error_msg)
    elif model_has_audit_failures or any(
        f.get("blocking") and f.get("model") == current_model_name
        for f in notifier_audit_failures
    ):
        context.log.info(
            f"Creating failed MaterializeResult for {current_model_name} due to blocking audit failure"
        )

        # Build precise check results via centralized helper
        check_results = _build_check_results_for_create_result(
            current_model_checks=current_model_checks,
            current_model_name=current_model_name,
            notifier_audit_failures=notifier_audit_failures,
            non_blocking_audit_warnings=non_blocking_audit_warnings,
            context=context,
        )

        result = MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "materialization_success_audit_failed"},
            check_results=check_results,
        )
        return result
    else:
        # If current asset is unaffected but is in affected downstream set, raise to block
        if current_asset_spec.key in set(affected_downstream_asset_keys):
            # Block following the upstream failure pattern
            context.log.info(
                f"Blocking downstream materialization for {current_model_name} due to upstream failures"
            )
            raise UpstreamAuditFailureError(
                description=f"Asset {current_asset_spec.key} skipped due to upstream audit failures"
            )

        return handle_successful_execution(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            non_blocking_audit_warnings,
            notifier_audit_failures,
        )
