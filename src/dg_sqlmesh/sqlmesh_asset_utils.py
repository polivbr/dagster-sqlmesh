from datetime import datetime
from typing import Any, List, Optional

from dagster import AssetSpec, AssetCheckSpec
from sqlmesh.core.model.definition import ExternalModel

from .sqlmesh_asset_check_utils import (
    create_asset_checks_from_model,
)


def get_downstream_models(
    context,
    model,
    selected_models: Optional[List[str]] = None,
) -> List[Any]:
    """
    Get downstream models for a single model, optionally filtered by selected models.

    Args:
        context: SQLMesh Context object
        model: SQLMesh model object
        selected_models: Optional list of model names to filter downstream models.
                        If None, returns all downstream models.

    Returns:
        List of downstream models
    """
    # Get all downstream model names
    downstream_names = context.dag.downstream(model.fqn)

    # Filter by selected_models if provided
    if selected_models is not None:
        downstream_names = [
            name for name in downstream_names if name in selected_models
        ]

    # Convert names to Model objects
    downstream_models = []
    for downstream_name in downstream_names:
        try:
            downstream_model = context.get_model(downstream_name)
            downstream_models.append(downstream_model)
        except Exception as e:
            # Use context logger if available, otherwise print
            if hasattr(context, "logger") and context.logger:
                context.logger.warning(
                    f"Could not load downstream model '{downstream_name}': {e}"
                )
            else:
                print(
                    f"Warning: Could not load downstream model '{downstream_name}': {e}"
                )

    return downstream_models


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
        partition_columns = (
            [col.name for col in partitioned_by] if partitioned_by else []
        )

        # Use intervals from plan snapshot (which is categorized)
        intervals = getattr(snapshot, "intervals", [])
        grain = getattr(model, "grain", [])
        is_partitioned = len(partition_columns) > 0

        return {
            "partitioned_by": partition_columns,
            "intervals": intervals,
            "partition_columns": partition_columns,
            "grain": grain,
            "is_partitioned": is_partitioned,
        }

    return {"partitioned_by": [], "intervals": []}


def get_model_from_asset_key(context, translator, asset_key) -> Any:
    """Converts a Dagster AssetKey to the corresponding SQLMesh model."""
    # Use inverse mapping from translator
    all_models = list(context.models.values())
    assetkey_to_model = translator.get_assetkey_to_model(all_models)

    return assetkey_to_model.get(asset_key)


def get_topologically_sorted_asset_keys(
    context, translator, selected_asset_keys
) -> list:
    """
    Returns the selected_asset_keys sorted in topological order according to the SQLMesh DAG.
    context: SQLMesh Context
    translator: SQLMeshTranslator instance
    """
    models = list(context.models.values())
    assetkey_to_model = translator.get_assetkey_to_model(models)
    fqn_to_assetkey = {model.fqn: translator.get_asset_key(model) for model in models}
    selected_fqns = set(
        model.fqn
        for key, model in assetkey_to_model.items()
        if key in selected_asset_keys
    )
    topo_fqns = context.dag.sorted
    ordered_asset_keys = [
        fqn_to_assetkey[fqn]
        for fqn in topo_fqns
        if fqn in selected_fqns and fqn in fqn_to_assetkey
    ]
    return ordered_asset_keys


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

    # Return metadata as-is; Dagster handles serialization of supported types
    return metadata


def format_partition_metadata(model_partitions: dict) -> dict:
    """
    Formats partition metadata to make it more readable.

    Args:
        model_partitions: Dict with raw partition info from SQLMesh

    Returns:
        Dict with formatted metadata (compatible with Dagster 1.11.4)
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
                start_dt = datetime.fromtimestamp(start_ts / 1000).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                end_dt = datetime.fromtimestamp(end_ts / 1000).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                readable_intervals.append(
                    {
                        "start": start_dt,
                        "end": end_dt,
                        "start_timestamp": start_ts,
                        "end_timestamp": end_ts,
                    }
                )

        # Ensure we return a simple list of dicts (JSON-serializable)
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
    if hasattr(model, "column_descriptions") and model.column_descriptions:
        column_descriptions = model.column_descriptions

    # Try to access via SQLMesh model
    elif hasattr(model, "model") and hasattr(model.model, "column_descriptions"):
        column_descriptions = model.model.column_descriptions

    # Ensure we return a dict, not None or Mock
    if column_descriptions and not hasattr(column_descriptions, "_mock_name"):
        return column_descriptions
    else:
        return {}


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
            if hasattr(model, "cron") and model.cron:
                intervals.append(model.interval_unit.seconds)

        if not intervals:
            return "0 */6 * * *"  # Default: every 6h

        # Find finest granularity
        finest_interval = min(intervals)

        # Return recommended Dagster schedule
        return get_dagster_schedule_from_interval(finest_interval)

    except Exception:
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
                errors.append(
                    f"Failed to map external dependency '{dep_str}' for model '{model.name}': {e}"
                )
    return errors


def create_all_asset_specs(
    models, sqlmesh_resource, extra_keys, kinds, owners, group_name
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
    specs = []
    for model in models:
        spec = _create_single_asset_spec(
            model, sqlmesh_resource, extra_keys, kinds, owners, group_name
        )
        specs.append(spec)
    return specs


def create_asset_specs_and_checks(
    sqlmesh_resource, extra_keys, kinds, owners, group_name
) -> tuple[list[AssetSpec], list[AssetCheckSpec]]:
    """
    Creates all AssetSpec and AssetCheckSpec for all SQLMesh models in a single pass.
    This is more efficient than separate functions as it avoids multiple loops over models.

    Args:
        sqlmesh_resource: SQLMeshResource
        extra_keys: Additional keys for metadata
        kinds: Asset kinds
        owners: Asset owners
        group_name: Default group name

    Returns:
        Tuple of (AssetSpec list, AssetCheckSpec list)
    """
    models = _get_internal_models(sqlmesh_resource)
    translator = sqlmesh_resource.translator
    context = sqlmesh_resource.context

    specs = []
    checks = []

    for model in models:
        # Extract common model information once
        model_info = _extract_model_info(
            model, translator, context, extra_keys, owners, group_name
        )

        # Create AssetSpec
        spec = _create_asset_spec_from_info(model_info, kinds)
        specs.append(spec)

        # Create AssetCheckSpec for each audit
        model_checks = create_asset_checks_from_model(model, model_info["asset_key"])
        checks.extend(model_checks)

    return specs, checks


def _create_single_asset_spec(
    model, sqlmesh_resource, extra_keys, kinds, owners, group_name
) -> AssetSpec:
    """
    Creates a single AssetSpec for a SQLMesh model.
    """
    translator = sqlmesh_resource.translator
    context = sqlmesh_resource.context

    model_info = _extract_model_info(
        model, translator, context, extra_keys, owners, group_name
    )
    return _create_asset_spec_from_info(model_info, kinds)


def _extract_model_info(
    model, translator, context, extra_keys, owners, group_name
) -> dict:
    """
    Extracts all necessary information from a SQLMesh model for creating AssetSpec.
    """
    asset_key = translator.get_asset_key(model)
    code_version = _extract_code_version(model)
    metadata = get_asset_metadata(translator, model, code_version, extra_keys, owners)
    tags = get_asset_tags(translator, context, model)
    deps = translator.get_model_deps_with_external(context, model)
    final_group_name = translator.get_group_name_with_fallback(
        context, model, group_name
    )

    return {
        "asset_key": asset_key,
        "code_version": code_version,
        "metadata": metadata,
        "tags": tags,
        "deps": deps,
        "group_name": final_group_name,
    }


def _create_asset_spec_from_info(model_info: dict, kinds: set) -> AssetSpec:
    """
    Creates an AssetSpec from extracted model information.
    """
    return AssetSpec(
        key=model_info["asset_key"],
        deps=model_info["deps"],
        code_version=model_info["code_version"],
        metadata=model_info["metadata"],
        kinds=kinds,
        tags=model_info["tags"],
        group_name=model_info["group_name"],
    )


def _extract_code_version(model) -> Optional[str]:
    """
    Extracts code version from a SQLMesh model.
    """
    if hasattr(model, "data_hash") and getattr(model, "data_hash"):
        return str(getattr(model, "data_hash"))
    return None


def _get_internal_models(sqlmesh_resource) -> list:
    """
    Gets all internal (non-external) models from SQLMesh resource.
    """
    return [
        model
        for model in sqlmesh_resource.get_models()
        if not isinstance(model, ExternalModel)
    ]


def create_asset_specs(
    sqlmesh_resource, extra_keys, kinds, owners, group_name
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
    models = [
        model
        for model in sqlmesh_resource.get_models()
        if not isinstance(model, ExternalModel)
    ]
    return create_all_asset_specs(
        models, sqlmesh_resource, extra_keys, kinds, owners, group_name
    )


def get_extra_keys() -> list[str]:
    """
    Returns additional keys for SQLMesh asset metadata.

    Returns:
        List of additional keys
    """
    return [
        "cron",
        "tags",
        "kind",
        "dialect",
        "query",
        "partitioned_by",
        "clustered_by",
    ]
