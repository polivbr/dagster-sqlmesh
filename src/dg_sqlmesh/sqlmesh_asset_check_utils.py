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
        # Build standardized metadata via central utility
        pass_meta = build_audit_check_metadata(
            model_or_name=model,
            audit_name=audit_obj.name,
        )

        asset_checks.append(
            AssetCheckSpec(
                name=audit_obj.name,
                asset=asset_key,
                description=f"Triggered by sqlmesh audit {audit_obj.name} on model {model.name}",
                blocking=False,  # SQLMesh handles blocking itself with audits
                metadata=pass_meta,
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
    if message:
        metadata["audit_message"] = message

    return metadata
