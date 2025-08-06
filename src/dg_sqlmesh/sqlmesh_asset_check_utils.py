# Utility functions for SQLMesh AssetCheckSpec creation

from dagster import AssetCheckSpec, AssetKey
from typing import List, Dict, Any
from sqlmesh.core.model.definition import ExternalModel


def create_asset_checks_from_model(model, asset_key: AssetKey) -> List[AssetCheckSpec]:
    """
    Creates AssetCheckSpec for audits of a SQLMesh model.

    Args:
        model: SQLMesh model
        asset_key: Dagster AssetKey associated with the model

    Returns:
        List of AssetCheckSpec for model audits
    """
    asset_checks = []

    # Get model audits
    audits_with_args = (
        model.audits_with_args if hasattr(model, "audits_with_args") else []
    )

    for audit_obj, audit_args in audits_with_args:
        asset_checks.append(
            AssetCheckSpec(
                name=audit_obj.name,
                asset=asset_key,
                description=f"Triggered by sqlmesh audit {audit_obj.name} on model {model.name}",
                blocking=False,  # SQLMesh handles blocking itself with audits
                metadata={
                    "audit_query": str(audit_obj.query.sql()),
                    "audit_blocking": getattr(
                        audit_obj, "blocking", True
                    ),  # ← Keep original info in metadata
                    "audit_args": audit_args,
                },
            )
        )

    return asset_checks


def create_all_asset_checks(models, translator) -> List[AssetCheckSpec]:
    """
    Creates all AssetCheckSpec for all SQLMesh models.

    Args:
        models: List of SQLMesh models
        translator: SQLMeshTranslator to map models to AssetKey

    Returns:
        List of all AssetCheckSpec
    """
    all_checks = []

    for model in models:
        # Ignore external models
        if isinstance(model, ExternalModel):
            continue

        asset_key = translator.get_asset_key(model)
        model_checks = create_asset_checks_from_model(model, asset_key)
        all_checks.extend(model_checks)

    return all_checks


def safe_extract_audit_query(model, audit_obj, audit_args, logger=None):
    """
    Safely extracts audit query with fallback.

    Args:
        model: SQLMesh model
        audit_obj: SQLMesh audit object (should not be an AuditError)
        audit_args: Audit arguments
        logger: Optional logger for warnings

    Returns:
        str: SQL query or "N/A" if extraction fails
    """
    try:
        return model.render_audit_query(audit_obj, **audit_args).sql()
    except Exception as e:
        if logger:
            logger.warning(f"⚠️ Error rendering audit query: {e}")
        try:
            return audit_obj.query.sql()
        except Exception as e2:
            if logger:
                logger.warning(f"⚠️ Error extracting base query: {e2}")
            return "N/A"


def extract_audit_details(audit_obj, audit_args, model, logger=None) -> Dict[str, Any]:
    """
    Extracts all useful information from an audit object.
    This function is moved from the console to follow the separation of concerns pattern.

    Args:
        audit_obj: SQLMesh audit object
        audit_args: Audit arguments
        model: SQLMesh model
        logger: Optional logger for warnings

    Returns:
        dict: Audit details including name, SQL, blocking status, etc.
    """
    # Use utility function for SQL extraction
    sql_query = safe_extract_audit_query(
        model=model, audit_obj=audit_obj, audit_args=audit_args, logger=logger
    )

    return {
        "name": getattr(audit_obj, "name", "unknown"),
        "sql": sql_query,
        "blocking": getattr(audit_obj, "blocking", False),
        "skip": getattr(audit_obj, "skip", False),
        "arguments": audit_args,
    }


def extract_successful_audit_results(
    event, translator, logger=None
) -> list[dict[str, Any]]:
    """
    Extract successful audit results from UpdateSnapshotEvaluationProgress event.

    Only processes events where num_audits_passed > 0 and num_audits_failed = 0
    to avoid conflicts with failed audit processing in the resource.

    Args:
        event: UpdateSnapshotEvaluationProgress event
        translator: SQLMeshTranslator instance for asset key conversion
        logger: Optional logger for warnings

    Returns:
        List of audit result dictionaries with model_name, asset_key, audit_details, batch_idx
    """
    # Only process successful audits (no failures)
    if not (
        event.num_audits_passed
        and event.num_audits_passed > 0
        and event.num_audits_failed == 0
    ):
        return []

    model_name = event.snapshot.name if hasattr(event.snapshot, "name") else "unknown"
    if logger:
        logger.info(
            f"✅ AUDITS RESULTS for model '{model_name}': {event.num_audits_passed} passed, {event.num_audits_failed} failed"
        )

    audit_results = []

    # Check if snapshot has model with audits
    if (
        hasattr(event.snapshot, "model")
        and hasattr(event.snapshot.model, "audits_with_args")
        and event.snapshot.model.audits_with_args
    ):
        for audit_obj, audit_args in event.snapshot.model.audits_with_args:
            try:
                # Use translator to get asset_key
                asset_key = (
                    translator.get_asset_key(event.snapshot.model)
                    if translator
                    else None
                )

                audit_result = {
                    "model_name": event.snapshot.model.name,
                    "asset_key": asset_key,
                    "audit_details": extract_audit_details(
                        audit_obj, audit_args, event.snapshot.model, logger
                    ),
                    "batch_idx": event.batch_idx,
                }
                audit_results.append(audit_result)
            except Exception as e:
                if logger:
                    logger.warning(f"⚠️ Error capturing audit: {e}")
                continue

    return audit_results
