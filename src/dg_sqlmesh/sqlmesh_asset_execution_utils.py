"""
Execution utilities for SQLMesh assets.

Functions extracted from the `model_asset` flow to improve readability and
testability. All docstrings and logs are standardized in English.
"""

import json
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
    """Safely retrieve notifier audit failures; return empty list on error."""
    try:
        notifier = sqlmesh._get_or_create_notifier()
        return notifier.get_audit_failures()
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
    _log_run_selection(context, run_id, selected_asset_keys)

    # Launch a single SQLMesh execution for all selected assets
    models_to_materialize = _select_models_to_materialize(selected_asset_keys, sqlmesh)

    # Single SQLMesh execution
    plan = _materialize_and_get_plan(sqlmesh, models_to_materialize, context)

    # Capture all results
    # Console removed → no legacy failed models events
    # Console disabled path
    failed_check_results, skipped_models_events, evaluation_events, non_blocking_audit_warnings = _init_execution_event_buffers(context)

    # Store results in the shared resource
    # Capture audit failures from the notifier (robust)
    notifier_audit_failures = _get_notifier_failures(sqlmesh)
    _summarize_notifier_failures(context, notifier_audit_failures)

    # Build blocking AssetKeys and affected downstream assets
    blocking_failed_asset_keys, affected_downstream_asset_keys = _compute_blocking_and_downstream(sqlmesh, notifier_audit_failures)
    context.log.info(
        f"Blocking failed assets: {blocking_failed_asset_keys} | Downstream affected: {list(affected_downstream_asset_keys)}"
    )

    results = _build_shared_results(
        plan,
        failed_check_results,
        skipped_models_events,
        evaluation_events,
        non_blocking_audit_warnings,
        notifier_audit_failures,
        affected_downstream_asset_keys,
    )

    sqlmesh_results.store_results(run_id, results)
    context.log.info(f"Stored SQLMesh results for run {run_id}")
    # Keep store confirmation

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
    for event in skipped_models_events:
        skipped_snapshots = event.get("snapshot_names", set())
        context.log.debug(f"🔍 Skipped snapshots: {skipped_snapshots}")

        for snapshot_name in skipped_snapshots:
            if snapshot_name:
                parts = snapshot_name.split('"."')
                if len(parts) >= 3:
                    skipped_model_name = parts[1] + "." + parts[2].replace('"', "")
                    context.log.debug(
                        f"Checking skipped model: {skipped_model_name} vs {current_model_name}"
                    )
                    if skipped_model_name == current_model_name:
                        model_was_skipped = True
                        context.log.error(
                            f"Model {current_model_name} was skipped due to upstream failures"
                        )
                        break
        if model_was_skipped:
            break

    # Check audit failures (model executed but audit failed)
    context.log.debug("Checking for audit failures...")
    for check_result in failed_check_results:
        context.log.debug(
            f"Checking failed check: {check_result.asset_key} vs {current_asset_spec.key}"
        )
        if check_result.asset_key == current_asset_spec.key:
            model_has_audit_failures = True
            context.log.error(
                f"Model {current_model_name} has audit failures: {check_result.metadata.get('audit_message', 'Unknown error')}"
            )
            break

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
        check_results = []

        # Create failed AssetCheckResult for each declared check
        for check in current_model_checks:
            # Use the specific error message for this check if available
            audit_message = "Model materialization succeeded but audits failed"
            for check_result in failed_check_results:
                if check_result.asset_key == current_asset_spec.key:
                    audit_message = check_result.metadata.get(
                        "audit_message", audit_message
                    )
                    break

            check_result = AssetCheckResult(
                check_name=check.name,
                passed=False,
                metadata={
                    "audit_message": audit_message,
                    "sqlmesh_audit_name": check.name,
                    "sqlmesh_model": current_model_name,
                    "error_details": f"SQLMesh audit '{check.name}' failed: {audit_message}",
                },
            )
            check_results.append(check_result)
            context.log.debug(
                f"Created failed check result for: {check.name} with message: {audit_message}"
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
        check_results = []

        # Notifier-only: build from notifier

        if not check_results:
            # Build failing set from notifier non-blocking
            nb_audits_for_model = {
                w.get("audit_name")
                for w in non_blocking_audit_warnings
                if w.get("model_name") == current_model_name
            }
            for fail in notifier_audit_failures:
                if not fail.get("blocking") and fail.get("model") == current_model_name:
                    nb_audits_for_model.add(fail.get("audit"))

            # Build audit details lookup from SQLMesh model for PASS metadata
            audit_details_by_name: Dict[str, Dict] = {}
            try:
                sqlmesh_model = context.resources.sqlmesh.context.get_model(current_model_name)  # type: ignore[attr-defined]
                if sqlmesh_model and hasattr(sqlmesh_model, "audits_with_args"):
                    for audit_obj, audit_args in sqlmesh_model.audits_with_args:
                        try:
                            from .sqlmesh_asset_check_utils import extract_audit_details

                            details = extract_audit_details(
                                audit_obj, audit_args, sqlmesh_model, logger=getattr(context, "log", None)
                            )
                            audit_details_by_name[details["name"]] = details
                        except Exception:
                            continue
            except Exception:
                pass

            # Emit WARN failed for non-blocking failures, PASS for others
            for check in current_model_checks:
                if check.name in nb_audits_for_model:
                    # fetch details for richer metadata
                    fail = next(
                        (f for f in notifier_audit_failures if f.get("model") == current_model_name and f.get("audit") == check.name),
                        {},
                    )
                    # Standardize WARN failure metadata
                    warn_meta = build_audit_check_metadata(
                        context=context.resources.sqlmesh.context if hasattr(context.resources, "sqlmesh") else None,  # type: ignore[attr-defined]
                        model_or_name=current_model_name,
                        audit_name=check.name,
                        notifier_record=fail,
                        logger=getattr(context, "log", None),
                    )
                    check_results.append(
                        AssetCheckResult(
                            check_name=check.name,
                            passed=False,
                            severity=get_check_severity_for_blocking(False),
                            metadata=warn_meta,
                        )
                    )
                else:
                    # Build PASS metadata via centralized builder
                    pass_meta = build_audit_check_metadata(
                        context=context.resources.sqlmesh.context if hasattr(context.resources, "sqlmesh") else None,  # type: ignore[attr-defined]
                        model_or_name=current_model_name,
                        audit_name=check.name,
                        logger=getattr(context, "log", None),
                    )
                    check_results.append(
                        AssetCheckResult(
                            check_name=check.name,
                            passed=True,
                            metadata=pass_meta,
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

        # Build precise check results: only the failing audits should fail
        failed_for_model = [
            f for f in notifier_audit_failures if f.get("model") == current_model_name
        ]
        blocking_names = {f.get("audit") for f in failed_for_model if f.get("blocking")}
        non_blocking_names = {f.get("audit") for f in failed_for_model if not f.get("blocking")}

        # Merge legacy console non-blocking warnings
        for w in non_blocking_audit_warnings:
            if w.get("model_name") == current_model_name:
                non_blocking_names.add(w.get("audit_name"))

        check_results: List[AssetCheckResult] = []
        for check in current_model_checks:
            if check.name in blocking_names:
                fail = next(
                    (f for f in failed_for_model if f.get("audit") == check.name),
                    {},
                )
                metadata = build_audit_check_metadata(
                    context=getattr(context.resources, "sqlmesh").context if hasattr(context, "resources") and hasattr(context.resources, "sqlmesh") else None,  # type: ignore[attr-defined]
                    model_or_name=current_model_name,
                    audit_name=check.name,
                    notifier_record=fail,
                    logger=getattr(context, "log", None),
                )
                check_results.append(
                    AssetCheckResult(
                        check_name=check.name,
                        passed=False,
                        severity=get_check_severity_for_blocking(True),
                        metadata=metadata,
                    )
                )
            elif check.name in non_blocking_names:
                # Build a synthetic notifier record to guarantee blocking=False in metadata
                fail_nb = next(
                    (
                        f
                        for f in failed_for_model
                        if not f.get("blocking") and f.get("audit") == check.name
                    ),
                    {},
                )
                nb_notifier_record = {
                    "model": current_model_name,
                    "audit": check.name,
                    "blocking": False,
                    **fail_nb,
                }
                metadata = build_audit_check_metadata(
                    context=getattr(context.resources, "sqlmesh").context if hasattr(context, "resources") and hasattr(context.resources, "sqlmesh") else None,  # type: ignore[attr-defined]
                    model_or_name=current_model_name,
                    audit_name=check.name,
                    notifier_record=nb_notifier_record,
                    logger=getattr(context, "log", None),
                )
                check_results.append(
                    AssetCheckResult(
                        check_name=check.name,
                        passed=False,
                        severity=get_check_severity_for_blocking(False),
                        metadata=metadata,
                    )
                )
            else:
                # Ensure every declared check_spec emits an output (PASS for non failing checks)
                pass_meta = build_audit_check_metadata(
                    context=getattr(context.resources, "sqlmesh").context if hasattr(context, "resources") and hasattr(context.resources, "sqlmesh") else None,  # type: ignore[attr-defined]
                    model_or_name=current_model_name,
                    audit_name=check.name,
                    logger=getattr(context, "log", None),
                )
                check_results.append(
                    AssetCheckResult(
                        check_name=check.name,
                        passed=True,
                        metadata=pass_meta,
                    )
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
