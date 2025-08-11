# Utility functions for SQLMesh AssetCheckSpec creation

from dagster import AssetCheckSpec, AssetKey
from typing import List, Dict, Any, Tuple, Optional
from sqlmesh.core.model.definition import ExternalModel
from sqlglot import exp
import re
import json


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


def extract_non_blocking_audit_warning(event, logger=None) -> Dict[str, Any] | None:
    """
    Extract a structured record for a non-blocking audit warning from a LogWarning event.

    Returns a dict with keys: audit_name, model_name, raw_message.
    Returns None if required info cannot be extracted.
    """
    # Prefer structured args when available
    unknown_args = getattr(event, "unknown_args", {})
    audit_name = None
    model_name = None

    # Try to resolve audit name from structured objects
    if isinstance(unknown_args, dict):
        audit_obj = unknown_args.get("audit")
        if audit_obj is not None:
            audit_name = getattr(audit_obj, "name", None)
        if audit_name is None:
            audit_name = unknown_args.get("audit_name")

        model_obj = unknown_args.get("model")
        snapshot_obj = unknown_args.get("snapshot")
        if model_obj is None and snapshot_obj is not None:
            model_obj = getattr(snapshot_obj, "model", None)
        if model_obj is not None:
            model_name = getattr(model_obj, "name", None)

    # Fallback: parse from free text
    long_message = getattr(event, "long_message", None)
    if audit_name is None and long_message:
        try:
            match = re.search(r"(?i)audit\s+['\"]?([\w\-\.]+)['\"]?\s+failed", long_message)
            if match:
                audit_name = match.group(1)
        except Exception as e:
            if logger:
                logger.debug(f"extract_non_blocking_audit_warning: audit regex error: {e}")

    # Fallback 2: pattern like 'xxx_non_blocking' audit error: ...
    if audit_name is None and long_message:
        try:
            text = long_message or ""
            match = re.search(r"[\"'`]([\w\-.]+)[\"'`]\s+audit\s+error", text, flags=re.IGNORECASE)
            if match:
                audit_name = match.group(1)
        except Exception as e:
            if logger:
                logger.debug(f"extract_non_blocking_audit_warning: audit error-regex error: {e}")

    # Fallback 3: any token ending with _non_blocking (covers all non-blocking audit variants)
    if audit_name is None and long_message:
        try:
            text = long_message or ""
            match = re.search(r"([\w\-.]+_non_blocking)", text)
            if match:
                audit_name = match.group(1)
        except Exception as e:
            if logger:
                logger.debug(f"extract_non_blocking_audit_warning: suffix _non_blocking regex error: {e}")

    if model_name is None and long_message:
        try:
            # Patterns: on model schema.model OR quoted catalog.schema.model
            match = re.search(r"on model\s+([\w\.]+)", long_message)
            if match:
                model_name = match.group(1)
            else:
                match2 = re.search(r'"[^\"]+"\."([^\"]+)"\."([^\"]+)"', long_message)
                if match2:
                    schema, model = match2.groups()
                    # Normalize SQLMesh physical identifiers to logical schema.model
                    # Example: schema="sqlmesh__sqlmesh_jaffle_platform", model="sqlmesh_jaffle_platform__stg_supplies__618589405"
                    if schema.startswith("sqlmesh__"):
                        schema = schema.split("sqlmesh__", 1)[1]
                    if "__" in model:
                        parts = model.split("__")
                        if len(parts) >= 2:
                            model = parts[1]
                    model_name = f"{schema}.{model}"
        except Exception as e:
            if logger:
                logger.debug(f"extract_non_blocking_audit_warning: model regex error: {e}")

    # Fallback 2: extract logical schema.model from SQLMesh physical identifiers in long_message
    # Example quoted identifiers: "jaffle_db"."sqlmesh__sqlmesh_jaffle_platform"."sqlmesh_jaffle_platform__stg_supplies__618589405"
    if model_name is None and long_message:
        try:
            m = re.search(
                r'"[^\"]+"\."sqlmesh__[^\"]+"\."([^\"]+)"',
                long_message,
            )
            if m:
                table_id = m.group(1)  # e.g., sqlmesh_jaffle_platform__stg_supplies__618589405
                parts = table_id.split("__")
                if len(parts) >= 2:
                    logical_schema = parts[0]
                    logical_model = parts[1]
                    model_name = f"{logical_schema}.{logical_model}"
        except Exception as e:
            if logger:
                logger.debug(
                    f"extract_non_blocking_audit_warning: snapshot-derived model regex error: {e}"
                )

    if not audit_name or not model_name:
        return None

    return {
        "audit_name": audit_name,
        "model_name": model_name,
        "raw_message": long_message or "",
    }


def is_audit_blocking_from_error(audit_error) -> bool:
    """
    Determine if the failed audit was blocking by inspecting the model's audits_with_args.
    Returns True if blocking, False if explicitly set to non-blocking, defaults to True if unknown.
    """
    model = getattr(audit_error, "model", None)
    audit_name = getattr(audit_error, "audit_name", None)
    if not model or not audit_name:
        return True  # conservative default

    try:
        for audit, args in getattr(model, "audits_with_args", []):
            if getattr(audit, "name", None) == audit_name:
                # explicit override via args
                if isinstance(args, dict) and "blocking" in args:
                    val = args["blocking"]
                    # If it's a SQLGlot expression, treat only exp.false() as False
                    if isinstance(val, exp.Expression):
                        return val != exp.false()
                    # If it's a Python bool or truthy value
                    return bool(val)
                # otherwise use the audit's default blocking
                return bool(getattr(audit, "blocking", True))
    except Exception:
        pass

    return True  # conservative default


def extract_failed_audit_details(audit_error, logger=None) -> Dict[str, Any]:
    """
    Extract structured information from an AuditError for building AssetCheckResult.

    Returns dict with keys:
      - name (str): audit_name
      - model_name (str | None)
      - sql (str)
      - blocking (bool)
      - count (int)
      - args (dict)
    """
    audit_name = getattr(audit_error, "audit_name", "unknown")
    model = getattr(audit_error, "model", None)
    # Prefer built-in property if available, fallback to model.name
    model_name = getattr(audit_error, "model_name", None) or getattr(model, "name", None)

    # Prefer the API on the error object itself
    sql_text = "N/A"
    try:
        if hasattr(audit_error, "sql"):
            # Many SQLMesh versions expose a convenience .sql(pretty=True)
            sql_text = audit_error.sql(pretty=True)  # type: ignore[attr-defined]
        elif hasattr(audit_error, "query"):
            sql_text = audit_error.query.sql(getattr(audit_error, "adapter_dialect", None))
    except Exception as e:  # pragma: no cover - defensive
        if logger:
            logger.warning(f"⚠️ Failed to extract audit SQL: {e}")
        sql_text = "N/A"

    blocking = is_audit_blocking_from_error(audit_error)
    count = int(getattr(audit_error, "count", 0) or 0)
    args = dict(getattr(audit_error, "audit_args", {}) or {})

    return {
        "name": audit_name,
        "model_name": model_name,
        "sql": sql_text,
        "blocking": blocking,
        "count": count,
        "args": args,
    }


def find_audit_on_model(model, audit_name: str) -> Optional[Tuple[Any, Dict[str, Any]]]:
    """
    Locate an audit object and its args on a SQLMesh model by name.
    Returns (audit_obj, audit_args) or None if not found.
    """
    try:
        for audit_obj, audit_args in getattr(model, "audits_with_args", []) or []:
            if getattr(audit_obj, "name", None) == audit_name:
                return audit_obj, (audit_args or {})
    except Exception:
        return None
    return None


def build_audit_check_metadata(
    *,
    context=None,
    model_or_name=None,
    audit_name: str,
    audit_error: Any | None = None,
    notifier_record: Dict[str, Any] | None = None,
    logger=None,
) -> Dict[str, Any]:
    """
    Centralized builder for AssetCheckResult/AssetCheckSpec metadata.

    Inputs can be any combination of:
      - model_or_name (SQLMesh model or 'schema.model' string) with optional context to resolve model
      - audit_error (SQLMesh AuditError) for failure cases
      - notifier_record (dict from notifier) for failure cases

    Returns metadata with standardized keys:
      - sqlmesh_model_name, audit_query, audit_args (json), audit_blocking (bool), audit_count (int, optional), audit_message (optional)
    """
    model = None
    model_name = None

    # Resolve model / model_name
    try:
        if model_or_name is not None and hasattr(model_or_name, "name"):
            model = model_or_name
            model_name = getattr(model, "name", None)
        elif isinstance(model_or_name, str):
            model_name = model_or_name
            if context is not None and hasattr(context, "get_model"):
                try:
                    model = context.get_model(model_name)
                except Exception:
                    model = None
    except Exception:
        model = None

    # Seed fields from inputs
    sql_text = None
    args: Dict[str, Any] = {}
    blocking: Optional[bool] = None
    count: Optional[int] = None
    message: Optional[str] = None

    if audit_error is not None:
        details = extract_failed_audit_details(audit_error, logger=logger)
        model_name = model_name or details.get("model_name")
        sql_text = details.get("sql")
        args = details.get("args", {})
        blocking = details.get("blocking")
        count = details.get("count")
        message = str(audit_error)

    if notifier_record is not None:
        model_name = model_name or notifier_record.get("model")
        sql_text = sql_text or notifier_record.get("sql")
        args = notifier_record.get("args", args)
        if blocking is None:
            blocking = notifier_record.get("blocking")
        count = notifier_record.get("count", count)

    # If still missing, derive from the model's audit definition
    if (sql_text is None or blocking is None or not args) and model is not None:
        found = find_audit_on_model(model, audit_name)
        if found is not None:
            audit_obj, audit_args = found
            try:
                details = extract_audit_details(
                    audit_obj, audit_args, model, logger=logger
                )
                sql_text = sql_text or details.get("sql")
                args = args or details.get("arguments", {})
                if blocking is None:
                    blocking = details.get("blocking")
            except Exception:
                pass

    # Final defaults
    sql_text = sql_text or "N/A"
    if blocking is None:
        blocking = True

    metadata: Dict[str, Any] = {
        "sqlmesh_model_name": model_name,
        "audit_query": sql_text,
        "audit_args": json.dumps(args or {}, default=str),
        "audit_blocking": bool(blocking),
    }
    if count is not None:
        metadata["audit_count"] = int(count)
    if message:
        metadata["audit_message"] = message

    return metadata
