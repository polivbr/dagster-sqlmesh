from __future__ import annotations

from typing import Any, Dict, List, Set
from dagster import AssetCheckResult, AssetKey


def _init_execution_event_buffers() -> tuple[
    List[AssetCheckResult], List[Dict], List[Dict], List[Dict]
]:
    failed_check_results: List[AssetCheckResult] = []
    skipped_models_events: List[Dict] = []
    evaluation_events: List[Dict] = []
    non_blocking_audit_warnings: List[Dict] = []
    return (
        failed_check_results,
        skipped_models_events,
        evaluation_events,
        non_blocking_audit_warnings,
    )


def _build_shared_results(
    plan: Any,
    failed_check_results: List[AssetCheckResult],
    skipped_models_events: List[Dict],
    evaluation_events: List[Dict],
    non_blocking_audit_warnings: List[Dict],
    notifier_audit_failures: List[Dict],
    affected_downstream_asset_keys: Set[AssetKey],
) -> Dict[str, Any]:
    return {
        "failed_check_results": failed_check_results,
        "skipped_models_events": skipped_models_events,
        "evaluation_events": evaluation_events,
        "non_blocking_audit_warnings": non_blocking_audit_warnings,
        "notifier_audit_failures": notifier_audit_failures,
        "affected_downstream_asset_keys": list(affected_downstream_asset_keys),
        "plan": plan,
    }
