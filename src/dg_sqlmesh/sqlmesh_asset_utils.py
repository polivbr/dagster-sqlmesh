from dagster import AssetSpec, AssetCheckSpec
from sqlmesh.core.model.definition import ExternalModel
from sqlmesh.utils.date import now
from typing import Any
from datetime import datetime
from .sqlmesh_asset_check_utils import create_all_asset_checks

def get_models_to_materialize(selected_asset_keys, get_models_func, translator):
    """
    Returns SQLMesh models to materialize, excluding external models.
    """
    all_models = get_models_func()
    
    # Filter external models
    internal_models = []
    for model in all_models:
        # Check if it's an ExternalModel
        if not isinstance(model, ExternalModel):
            internal_models.append(model)
    
    # If specific assets are selected, filter by AssetKey
    if selected_asset_keys:
        assetkey_to_model = translator.get_assetkey_to_model(internal_models)
        models_to_materialize = []
        
        for asset_key in selected_asset_keys:
            if asset_key in assetkey_to_model:
                models_to_materialize.append(assetkey_to_model[asset_key])
        
        return models_to_materialize
    
    # Otherwise, return all internal models
    return internal_models


def get_model_partitions_from_plan(plan, translator, asset_key, snapshot) -> dict:
    """Returns partition information for an asset using the plan."""
    # Convert AssetKey to SQLMesh model
    model = snapshot.model if snapshot else None
    
    if model:
        partitioned_by = getattr(model, "partitioned_by", [])
        # Extract partition column names
        partition_columns = [col.name for col in partitioned_by] if partitioned_by else []
        
        # Use intervals from plan snapshot (which is categorized)
        intervals = getattr(snapshot, "intervals", [])
        grain = getattr(model, "grain", [])
        is_partitioned = len(partition_columns) > 0
        
        return {
            "partitioned_by": partition_columns, 
            "intervals": intervals, 
            "partition_columns": partition_columns, 
            "grain": grain, 
            "is_partitioned": is_partitioned
        }
    
    return {"partitioned_by": [], "intervals": []}


def get_model_from_asset_key(context, translator, asset_key) -> Any:
    """Converts a Dagster AssetKey to the corresponding SQLMesh model."""
    # Use inverse mapping from translator
    all_models = list(context.models.values())
    assetkey_to_model = translator.get_assetkey_to_model(all_models)
    
    return assetkey_to_model.get(asset_key)

def get_topologically_sorted_asset_keys(context, translator, selected_asset_keys) -> list:
    """
    Returns the selected_asset_keys sorted in topological order according to the SQLMesh DAG.
    context: SQLMesh Context
    translator: SQLMeshTranslator instance
    """
    models = list(context.models.values())
    assetkey_to_model = translator.get_assetkey_to_model(models)
    fqn_to_assetkey = {model.fqn: translator.get_asset_key(model) for model in models}
    selected_fqns = set(model.fqn for key, model in assetkey_to_model.items() if key in selected_asset_keys)
    topo_fqns = context.dag.sorted
    ordered_asset_keys = [
        fqn_to_assetkey[fqn]
        for fqn in topo_fqns
        if fqn in selected_fqns and fqn in fqn_to_assetkey
    ]
    return ordered_asset_keys


def has_breaking_changes(plan, logger, context=None) -> bool:
    """
    Returns True if the given SQLMesh plan contains breaking changes
    (any directly or indirectly modified models).
    Logs the concerned models, using context.log if available.
    """
    directly_modified = getattr(plan, "directly_modified", set())
    indirectly_modified = getattr(plan, "indirectly_modified", set())

    directly = list(directly_modified)
    indirectly = [item for sublist in indirectly_modified.values() for item in sublist]

    has_changes = bool(directly or indirectly)

    if has_changes:
        msg = (
            f"Breaking changes detected in plan {getattr(plan, 'plan_id', None)}! "
            f"Directly modified models: {directly} | Indirectly modified models: {indirectly}"
        )
        if context and hasattr(context, "log"):
            context.log.error(msg)
        else:
            logger.error(msg)
    else:
        info_msg = f"No breaking changes detected in plan {getattr(plan, 'plan_id', None)}."
        if context and hasattr(context, "log"):
            context.log.info(info_msg)
        else:
            logger.info(info_msg)

    return has_changes 


def has_breaking_changes_with_message(plan, logger, context=None) -> tuple[bool, str]:
    """
    Returns (True, message) if the given SQLMesh plan contains breaking changes
    (any directly or indirectly modified models).
    Logs the concerned models, using context.log if available.
    """
    directly_modified = getattr(plan, "directly_modified", set())
    indirectly_modified = getattr(plan, "indirectly_modified", set())

    directly = list(directly_modified)
    indirectly = [item for sublist in indirectly_modified.values() for item in sublist]

    has_changes = bool(directly or indirectly)

    if has_changes:
        msg = (
            f"Breaking changes detected in plan {getattr(plan, 'plan_id', None)}! "
            f"Directly modified models: {directly} | Indirectly modified models: {indirectly}"
        )
        if context and hasattr(context, "log"):
            context.log.error(msg)
        else:
            logger.error(msg)
        return True, msg
    else:
        info_msg = f"No breaking changes detected in plan {getattr(plan, 'plan_id', None)}."
        if context and hasattr(context, "log"):
            context.log.info(info_msg)
        else:
            logger.info(info_msg)
        return False, info_msg


def get_asset_kinds(sqlmesh_resource) -> set:
    """
    Returns asset kinds with SQL dialect.
    """
    translator = sqlmesh_resource.translator
    context = sqlmesh_resource.context
    dialect = translator._get_context_dialect(context)
    return {"sqlmesh", dialect}


def get_asset_tags(translator, context, model) -> dict:
    """
    Returns tags for an asset.
    """
    return translator.get_tags(context, model)


def get_asset_metadata(translator, model, code_version, extra_keys, owners) -> dict:
    """
    Returns metadata for an asset.
    """
    metadata = {}
    
    # Base metadata
    if code_version:
        metadata["code_version"] = code_version
    
    # Table metadata with column descriptions
    table_metadata = translator.get_table_metadata(model)
    metadata.update(table_metadata)
    
    # Add column descriptions if available
    column_descriptions = get_column_descriptions_from_model(model)
    if column_descriptions:
        metadata["column_descriptions"] = column_descriptions
    
    # Additional metadata
    if extra_keys:
        serialized_metadata = translator.serialize_metadata(model, extra_keys)
        metadata.update(serialized_metadata)
    
    # Owners
    if owners:
        metadata["owners"] = owners
    
    return metadata


def format_partition_metadata(model_partitions: dict) -> dict:
    """
    Formats partition metadata to make it more readable.
    
    Args:
        model_partitions: Dict with raw partition info from SQLMesh
    
    Returns:
        Dict with formatted metadata
    """
    formatted_metadata = {}
    
    # Partition columns (use partitioned_by which is more standard)
    if model_partitions.get("partitioned_by"):
        formatted_metadata["partition_columns"] = model_partitions["partitioned_by"]
    
    # Intervals converted to readable datetime
    if model_partitions.get("intervals"):
        readable_intervals = []
        intervals = model_partitions["intervals"]
        
        for interval in intervals:
            if len(interval) == 2:
                start_ts, end_ts = interval
                # Convert Unix timestamps (milliseconds) to datetime
                start_dt = datetime.fromtimestamp(start_ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
                end_dt = datetime.fromtimestamp(end_ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
                readable_intervals.append({
                    "start": start_dt,
                    "end": end_dt,
                    "start_timestamp": start_ts,
                    "end_timestamp": end_ts
                })
        
        # Use Python object directly (Dagster can handle it)
        formatted_metadata["partition_intervals"] = readable_intervals
    
    # Grain (if present and not empty)
    if model_partitions.get("grain") and model_partitions["grain"]:
        formatted_metadata["partition_grain"] = model_partitions["grain"]
    
    return formatted_metadata


def get_column_descriptions_from_model(model) -> dict:
    """
    Extracts column_descriptions from a SQLMesh model and formats them for Dagster.
    """
    column_descriptions = {}
    
    # Try to access column_descriptions from model
    if hasattr(model, 'column_descriptions') and model.column_descriptions:
        column_descriptions = model.column_descriptions
    
    # Try to access via SQLMesh model
    elif hasattr(model, 'model') and hasattr(model.model, 'column_descriptions'):
        column_descriptions = model.model.column_descriptions
    
    return column_descriptions


def safe_extract_audit_query(model, audit_obj, audit_args, logger=None):
    """
    Safely extracts audit query with fallback.
    
    Args:
        model: SQLMesh model
        audit_obj: SQLMesh audit object
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


def analyze_sqlmesh_crons_using_api(context):
    """
    Analyzes all SQLMesh model crons and returns the recommended Dagster schedule.
    
    Args:
        context: SQLMesh Context
    
    Returns:
        str: Recommended Dagster cron expression
    """
    try:
        models = context.models.values()
        
        # Collect intervals from models with cron
        intervals = []
        for model in models:
            if hasattr(model, 'cron') and model.cron:
                intervals.append(model.interval_unit.seconds)
        
        if not intervals:
            return "0 */6 * * *"  # Default: every 6h
        
        # Find finest granularity
        finest_interval = min(intervals)
        
        # Return recommended Dagster schedule
        return get_dagster_schedule_from_interval(finest_interval)
        
    except Exception as e:
        # Fallback in case of error
        return "0 */6 * * *"  # Default: every 6h


def get_dagster_schedule_from_interval(interval_seconds):
    """
    Converts an interval in seconds to a Dagster cron expression.
    
    Args:
        interval_seconds: Interval in seconds
    
    Returns:
        str: Dagster cron expression
    """
    # Mapping of intervals to cron expressions
    if interval_seconds <= 300:  # <= 5 minutes
        return "*/5 * * * *"
    elif interval_seconds <= 900:  # <= 15 minutes
        return "*/15 * * * *"
    elif interval_seconds <= 1800:  # <= 30 minutes
        return "*/30 * * * *"
    elif interval_seconds <= 3600:  # <= 1 hour
        return "0 * * * *"
    elif interval_seconds <= 21600:  # <= 6 hours
        return "0 */6 * * *"
    elif interval_seconds <= 86400:  # <= 1 day
        return "0 0 * * *"
    else:
        return "0 0 * * 0"  # Every week




def validate_external_dependencies(sqlmesh_resource, models) -> list:
    """
    Validates that all external dependencies can be properly mapped.
    Returns a list of validation errors.
    """
    translator = sqlmesh_resource.translator
    context = sqlmesh_resource.context
    errors = []
    for model in models:
        # Ignore external models in validation
        if isinstance(model, ExternalModel):
            continue
            
        external_deps = translator.get_external_dependencies(context, model)
        for dep_str in external_deps:
            try:
                translator.get_external_asset_key(dep_str)
            except Exception as e:
                errors.append(f"Failed to map external dependency '{dep_str}' for model '{model.name}': {e}")
    return errors

def create_all_asset_specs(
    models,
    sqlmesh_resource,
    extra_keys,
    kinds,
    owners,
    group_name
) -> list[AssetSpec]:
    """
    Creates all AssetSpec for all SQLMesh models.
    
    Args:
        models: List of SQLMesh models
        sqlmesh_resource: SQLMeshResource
        extra_keys: Additional keys for metadata
        kinds: Asset kinds
        owners: Asset owners
        group_name: Default group name
    
    Returns:
        List of all AssetSpec
    """
    translator = sqlmesh_resource.translator
    context = sqlmesh_resource.context
    specs = []
    for model in models:
        asset_key = translator.get_asset_key(model)
        code_version = str(getattr(model, "data_hash", "")) if hasattr(model, "data_hash") and getattr(model, "data_hash") else None
        metadata = get_asset_metadata(translator, model, code_version, extra_keys, owners)
        tags = get_asset_tags(translator, context, model)
        deps = translator.get_model_deps_with_external(context, model)
        final_group_name = translator.get_group_name_with_fallback(context, model, group_name)
        
        spec = AssetSpec(
            key=asset_key,
            deps=deps,
            code_version=code_version,
            metadata=metadata,
            kinds=kinds,
            tags=tags,
            group_name=final_group_name,
        )
        specs.append(spec)
    return specs


def create_asset_specs(
    sqlmesh_resource,
    extra_keys,
    kinds,
    owners,
    group_name
) -> list[AssetSpec]:
    """
    Creates all AssetSpec for all SQLMesh models.
    
    Args:
        sqlmesh_resource: SQLMeshResource
        extra_keys: Additional keys for metadata
        kinds: Asset kinds
        owners: Asset owners
        group_name: Default group name
    
    Returns:
        List of all AssetSpec
    """
    models = [model for model in sqlmesh_resource.get_models() if not isinstance(model, ExternalModel)]
    return create_all_asset_specs(models, sqlmesh_resource, extra_keys, kinds, owners, group_name)


def get_extra_keys() -> list[str]:
    """
    Returns additional keys for SQLMesh asset metadata.
    
    Returns:
        List of additional keys
    """
    return ["cron", "tags", "kind", "dialect", "query", "partitioned_by", "clustered_by"]


def create_asset_checks(
    sqlmesh_resource
) -> list[AssetCheckSpec]:
    """
    Creates all AssetCheckSpec for all SQLMesh models.
    
    Args:
        sqlmesh_resource: SQLMeshResource
    
    Returns:
        List of all AssetCheckSpec
    """
    models = [model for model in sqlmesh_resource.get_models() if not isinstance(model, ExternalModel)]
    return create_all_asset_checks(models, sqlmesh_resource.translator) 