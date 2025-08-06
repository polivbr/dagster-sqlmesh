import threading
import anyio
import logging
import datetime
from typing import Any
from pydantic import PrivateAttr
from dagster import (
    AssetKey,
    AssetCheckResult,
    MaterializeResult,
    DataVersion,
    ConfigurableResource,
    InitResourceContext,
)
from sqlmesh import Context
from sqlmesh.core.console import set_console
from .translator import SQLMeshTranslator
from .sqlmesh_asset_utils import (
    get_models_to_materialize,
    get_topologically_sorted_asset_keys,
    format_partition_metadata,
    get_model_partitions_from_plan,
    analyze_sqlmesh_crons_using_api,
    get_model_from_asset_key,
)
from .sqlmesh_event_console import SQLMeshEventCaptureConsole
from sqlmesh.utils.errors import (
    SQLMeshError,
    PlanError,
    ConflictingPlanError,
    NodeAuditsErrors,
    CircuitBreakerError,
    NoChangesPlanError,
    UncategorizedPlanError,
    AuditError,
    PythonModelEvalError,
    SignalEvalError,
)
import json


class UpstreamAuditFailureError(Exception):
    """
    Custom exception for upstream audit failures that should be handled gracefully.
    This exception should not trigger retries or be re-raised by the factory.
    """

    pass


def convert_unix_timestamp_to_readable(timestamp):
    """
    Converts a Unix timestamp to a readable date.

    Args:
        timestamp: Unix timestamp in milliseconds (int or float)

    Returns:
        str: Date in "YYYY-MM-DD HH:MM:SS" format or None if timestamp is None
    """
    if timestamp is None:
        return None

    try:
        # Convert milliseconds to seconds
        timestamp_seconds = timestamp / 1000
        dt = datetime.datetime.fromtimestamp(timestamp_seconds)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        # Fallback if conversion fails
        return str(timestamp)


# Global lock for SQLMesh console singleton
_console_lock = threading.Lock()


class SQLMeshResource(ConfigurableResource):
    """
    Dagster resource for interacting with SQLMesh.
    Manages SQLMesh context, caching and orchestrates materialization.
    """

    project_dir: str
    gateway: str = "postgres"
    environment: str = "prod"
    concurrency_limit: int = 1

    # Private attribute for Dagster logger (not subject to Pydantic immutability)
    _logger: Any = PrivateAttr(default=None)

    # Singleton for SQLMesh console (lazy initialized)

    def __init__(self, **kwargs):
        # Extract translator before calling super().__init__
        translator = kwargs.pop("translator", None)
        super().__init__(**kwargs)

        # Store translator for later use
        if translator:
            self._translator_instance = translator

        # Create SQLMesh console at initialization
        self._console = self._get_or_create_console()

        # Configure translator in console after creation
        if hasattr(self, "_translator_instance") and self._translator_instance:
            self._console._translator = self._translator_instance

    def __del__(self):
        pass  # Simplified cleanup

    @property
    def logger(self):
        """Returns the logger for this resource."""
        return logging.getLogger(__name__)

    @classmethod
    def _get_or_create_console(cls) -> "SQLMeshEventCaptureConsole":
        """Creates or returns the singleton instance of the SQLMesh event console."""
        # Initialize class variables lazily
        if not hasattr(cls, "_console_instance"):
            cls._console_instance = None

        if cls._console_instance is None:
            with _console_lock:
                if cls._console_instance is None:  # Double-check pattern
                    cls._console_instance = SQLMeshEventCaptureConsole(
                        log_override=logging.getLogger(__name__),
                    )
                    set_console(cls._console_instance)
        return cls._console_instance

    @property
    def context(self) -> Context:
        """
        Returns the SQLMesh context. Cached for performance.
        """
        if not hasattr(self, "_context_cache"):
            # Configure custom console before creating context
            console = self._get_or_create_console()
            console._logger = self._logger  # Update logger

            self._context_cache = Context(
                paths=self.project_dir,
                gateway=self.gateway,
            )
        return self._context_cache

    @property
    def translator(self) -> SQLMeshTranslator:
        """
        Returns a SQLMeshTranslator instance for mapping AssetKeys and models.
        Cached for performance.
        """
        if not hasattr(self, "_translator_cache"):
            # Use translator provided as parameter or create a new one
            self._translator_cache = (
                getattr(self, "_translator_instance", None) or SQLMeshTranslator()
            )
        return self._translator_cache

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # Store Dagster logger in private attribute
        self._logger = context.log

        # Configure console with Dagster logger
        if hasattr(self, "_console") and self._console:
            self._console._logger = self._logger

    def get_models(self):
        """
        Returns all SQLMesh models. Cached for performance.
        """
        if not hasattr(self, "_models_cache"):
            self._models_cache = list(self.context.models.values())
        return self._models_cache

    def get_recommended_schedule(self):
        """
        Analyzes SQLMesh crons and returns the recommended Dagster schedule.

        Returns:
            str: Recommended Dagster cron expression
        """
        return analyze_sqlmesh_crons_using_api(self.context)

    def _serialize_audit_args(self, audit_args):
        """Serialize audit arguments to JSON-compatible format"""
        try:
            return json.dumps(audit_args, default=str)
        except Exception:
            return "{}"

    def _deduplicate_asset_check_results(
        self, asset_check_results: list[AssetCheckResult]
    ) -> list[AssetCheckResult]:
        """
        Deduplicate AssetCheckResult events to prevent conflicts.
        If an asset has both successful and failed audits, prioritize failed ones.
        """
        if not asset_check_results:
            return []

        # Group by asset_key and check_name
        grouped_results = {}
        for result in asset_check_results:
            key = (result.asset_key, result.check_name)
            if key not in grouped_results:
                grouped_results[key] = result
            else:
                # If we have both passed=True and passed=False for same check, prioritize failed
                if not result.passed and grouped_results[key].passed:
                    grouped_results[key] = result
                    if self._logger:
                        self._logger.warning(
                            f"⚠️ Conflicting audit results for {result.asset_key}.{result.check_name}: prioritizing failed result"
                        )

        return list(grouped_results.values())

    def materialize_assets(self, models, context=None):
        """
        Materializes specified SQLMesh assets with robust error handling.
        """
        model_names = [model.name for model in models]
        self._prepare_console()
        try:
            plan = self._create_sqlmesh_plan(model_names)
            self._run_sqlmesh_plan(model_names)
            return plan
        except CircuitBreakerError:
            self._log_and_raise(
                "Run interrupted: environment changed during execution."
            )
        except (
            PlanError,
            ConflictingPlanError,
            NoChangesPlanError,
            UncategorizedPlanError,
        ) as e:
            self._log_and_raise(f"Planning error: {e}")
        except (AuditError, NodeAuditsErrors) as e:
            self._log_and_raise(f"Audit error: {e}")
        except (PythonModelEvalError, SignalEvalError) as e:
            self._log_and_raise(f"Model or signal execution error: {e}")
        except SQLMeshError as e:
            self._log_and_raise(f"SQLMesh error: {e}")
        except Exception as e:
            self._log_and_raise(f"Unexpected error: {e}")

    def _prepare_console(self):
        set_console(self._console)
        self._console.clear_events()

    def _create_sqlmesh_plan(self, model_names):
        return self.context.plan(
            select_models=model_names,
            auto_apply=False,  # never apply the plan, we will juste need it for metadata collection
            no_prompts=True,
        )

    def _run_sqlmesh_plan(self, model_names):
        self.context.run(
            environment=self.environment,
            select_models=model_names,
            execution_time=datetime.datetime.now(),
        )

    def _log_and_raise(self, message):
        self._logger.error(message)
        raise

    def materialize_assets_threaded(self, models, context=None):
        """
        Synchronous wrapper for Dagster that uses anyio.
        """

        def run_materialization():
            try:
                return self.materialize_assets(models, context)
            except Exception as e:
                self._logger.error(f"Materialization failed: {e}")
                raise

        return anyio.run(anyio.to_thread.run_sync, run_materialization)

    def _extract_model_info(self, error) -> tuple[str, Any, Any]:
        """
        Extract model name, model object, and asset key from error.
        Returns (model_name, model, asset_key)
        """
        model_name = "unknown"
        model = None
        asset_key = None

        try:
            # Handle different error types
            if hasattr(error, "node") and error.node:
                # Standard SQLMesh error with node attribute
                model_name = (
                    error.node[0]
                    if isinstance(error.node, (list, tuple))
                    else str(error.node)
                )
            elif hasattr(error, "__str__"):
                # Try to extract from error message (e.g., NodeExecutionFailedError)
                error_str = str(error)
                # Look for pattern like "Execution failed for node ('"db"."schema"."model"', ...)"
                import re

                match = re.search(r'"([^"]+)"\."([^"]+)"\."([^"]+)"', error_str)
                if match:
                    db, schema, model = match.groups()
                    model_name = f"{schema}.{model}"
                else:
                    # Fallback: try to extract any quoted model name
                    match = re.search(r'"([^"]+\.[^"]+)"', error_str)
                    if match:
                        model_name = match.group(1)
            else:
                model_name = str(error)

            # Try to get model and asset key
            if model_name != "unknown":
                model = self.context.get_model(model_name)
                if model:
                    asset_key = self.translator.get_asset_key(model)
        except Exception as e:
            if self._logger:
                self._logger.warning(f"⚠️ Error converting model name to asset key: {e}")

        return model_name, model, asset_key

    def _create_failed_audit_check_result(
        self, audit_error, model_name, asset_key
    ) -> AssetCheckResult | None:
        """
        Create AssetCheckResult for a failed audit.
        Returns None if audit details cannot be extracted (just logs the error).
        """
        try:
            audit_sql = audit_error.sql()
            audit_name = getattr(audit_error, "audit_name", "unknown")
            audit_args = getattr(audit_error, "audit_args", {})
            audit_message = str(audit_error)
            audit_blocking = getattr(audit_error, "blocking", False)
            serialized_args = self._serialize_audit_args(audit_args)

            # Log failed audit
            if self._logger:
                self._logger.warning(
                    f"❌ AUDIT FAILED for model '{model_name}': {audit_name} - {audit_message}"
                )

            return AssetCheckResult(
                passed=False,  # Failed audit
                asset_key=asset_key,
                check_name=audit_name,
                metadata={
                    "sqlmesh_model_name": model_name,
                    "audit_query": audit_sql,
                    "audit_blocking": audit_blocking,
                    "audit_message": audit_message,
                    "audit_args": serialized_args,
                    "error_type": "audit_failure",
                },
            )
        except Exception as audit_e:
            if self._logger:
                self._logger.warning(
                    f"⚠️ Failed to extract audit details for {model_name}: {audit_e}"
                )
            return None

    def _create_general_error_check_result(
        self, error, model_name, asset_key, error_type: str, message: str
    ) -> AssetCheckResult:
        """
        Create AssetCheckResult for general errors (non-audit).
        """
        # Log general error
        if self._logger:
            self._logger.warning(
                f"❌ MODEL ERROR for model '{model_name}': {error_type} - {message}"
            )

        return AssetCheckResult(
            passed=False,
            asset_key=asset_key,
            check_name="model_execution_error",
            metadata={
                "sqlmesh_model_name": model_name,
                "audit_query": "N/A",
                "audit_blocking": False,
                "audit_message": message,
                "audit_args": {},
                "error_type": error_type,
            },
        )

    def _process_failed_models_events(self) -> list[AssetCheckResult]:
        """
        Process failed models events and convert them to AssetCheckResult.
        """
        failed_models_events = self._console.get_failed_models_events()
        asset_check_results = []

        for event in failed_models_events:
            errors = event.get("errors", [])
            for error in errors:
                try:
                    model_name, model, asset_key = self._extract_model_info(error)
                    self._process_single_error(
                        error, model_name, asset_key, asset_check_results
                    )
                except Exception as e:
                    self._log_failed_error_processing(e)
        return asset_check_results

    def _process_single_error(self, error, model_name, asset_key, asset_check_results):
        # Process audit errors if present
        if hasattr(error, "__cause__") and error.__cause__:
            if isinstance(error.__cause__, NodeAuditsErrors):
                self._process_audit_errors(
                    error.__cause__, model_name, asset_key, asset_check_results
                )
            else:
                general_result = self._create_general_error_check_result(
                    error,
                    model_name,
                    asset_key,
                    "general_execution_error",
                    str(error.__cause__),
                )
                asset_check_results.append(general_result)
        else:
            general_result = self._create_general_error_check_result(
                error, model_name, asset_key, "general_model_failure", str(error)
            )
            asset_check_results.append(general_result)

    def _process_audit_errors(
        self, audits_errors, model_name, asset_key, asset_check_results
    ):
        for audit_error in audits_errors.errors:
            audit_result = self._create_failed_audit_check_result(
                audit_error, model_name, asset_key
            )
            if audit_result is not None:
                asset_check_results.append(audit_result)

    def _log_failed_error_processing(self, exception):
        if self._logger:
            self._logger.warning(f"⚠️ Failed to process error: {exception}")

    def _get_failed_blocking_checks(
        self, asset_check_results: list[AssetCheckResult]
    ) -> dict[AssetKey, list[AssetCheckResult]]:
        """
        Extract failed checks that are blocking from AssetCheckResult list.

        Args:
            asset_check_results: List of all AssetCheckResult

        Returns:
            Dict mapping AssetKey to list of failed blocking checks
        """
        failed_blocking = {}
        for check_result in asset_check_results:
            if not check_result.passed:
                # Check if this audit is blocking from metadata
                metadata = check_result.metadata
                audit_blocking = metadata.get(
                    "audit_blocking", True
                )  # Default to blocking
                if audit_blocking:
                    asset_key = check_result.asset_key
                    if asset_key not in failed_blocking:
                        failed_blocking[asset_key] = []
                    failed_blocking[asset_key].append(check_result)
        return failed_blocking

    def _get_affected_downstream_assets(
        self, failed_asset_keys: list[AssetKey]
    ) -> set[AssetKey]:
        """
        Get all downstream assets that should be blocked due to upstream failures.

        Args:
            failed_asset_keys: List of AssetKeys that failed

        Returns:
            Set of AssetKeys that should be blocked
        """
        affected_assets = set()

        for failed_asset_key in failed_asset_keys:
            # Get SQLMesh model from asset key
            failed_model = get_model_from_asset_key(
                self.context, self.translator, failed_asset_key
            )
            if failed_model:
                # Get downstream models using our new utility
                from .sqlmesh_asset_utils import get_downstream_models

                downstream_models = get_downstream_models(
                    context=self.context,
                    model=failed_model,
                    selected_models=None,  # All downstream
                )

                # Convert to AssetKeys
                for downstream_model in downstream_models:
                    downstream_asset_key = self.translator.get_asset_key(
                        downstream_model
                    )
                    affected_assets.add(downstream_asset_key)

        return affected_assets

    def materialize_all_assets(self, context):
        """
        Materializes all selected assets and yields results.
        """

        selected_asset_keys = context.selected_asset_keys
        models_to_materialize = get_models_to_materialize(
            selected_asset_keys,
            self.get_models,
            self.translator,
        )

        # Create and apply plan
        plan = self.materialize_assets_threaded(models_to_materialize, context=context)

        # Extract categorized snapshots directly from plan
        assetkey_to_snapshot = {}
        for snapshot in plan.snapshots.values():
            model = snapshot.model
            asset_key = self.translator.get_asset_key(model)
            assetkey_to_snapshot[asset_key] = snapshot

        # Sort asset keys in topological order
        ordered_asset_keys = get_topologically_sorted_asset_keys(
            self.context, self.translator, selected_asset_keys
        )

        # Process all audit results (successful and failed)
        successful_audit_results = []
        audit_results = self._console.get_audit_results()
        for audit_result in audit_results:
            audit_details = audit_result["audit_details"]
            asset_key = audit_result["asset_key"]

            # Successful audits are always passed=True
            passed = True

            # Serialize audit arguments to JSON-compatible format
            serialized_args = self._serialize_audit_args(audit_details["arguments"])

            successful_audit_results.append(
                AssetCheckResult(
                    passed=passed,
                    asset_key=asset_key,
                    check_name=audit_details["name"],
                    metadata={
                        "sqlmesh_model_name": audit_result[
                            "model_name"
                        ],  # ← SQLMesh model name
                        "audit_query": audit_details["sql"],
                        "audit_blocking": audit_details["blocking"],
                        "audit_dialect": getattr(audit_details, "dialect", "unknown"),
                        "audit_args": serialized_args,
                        "error_type": "audit_success",
                    },
                )
            )

        # Process failed models/audits
        failed_models_results = self._process_failed_models_events()

        # Combine and deduplicate all AssetCheckResult events
        all_asset_check_results = successful_audit_results + failed_models_results
        deduplicated_results = self._deduplicate_asset_check_results(
            all_asset_check_results
        )

        # Get failed blocking checks and affected downstream assets
        failed_blocking_checks = self._get_failed_blocking_checks(deduplicated_results)
        failed_asset_keys = list(failed_blocking_checks.keys())
        affected_downstream = self._get_affected_downstream_assets(failed_asset_keys)

        # Group AssetCheckResult by asset_key for MaterializeResult
        asset_check_results_by_key = {}
        for asset_check_result in deduplicated_results:
            asset_key = asset_check_result.asset_key
            if asset_key not in asset_check_results_by_key:
                asset_check_results_by_key[asset_key] = []
            asset_check_results_by_key[asset_key].append(asset_check_result)

        # Create MaterializeResult (skip affected downstream assets)
        for asset_key in ordered_asset_keys:
            if asset_key in affected_downstream:
                # Skip affected downstream assets - don't yield MaterializeResult
                if self._logger:
                    self._logger.warning(
                        f"⏭️ Skipping materialization of {asset_key} due to upstream failures: "
                        f"{[str(key) for key in failed_asset_keys]}"
                    )
                # Raise custom exception that will be handled gracefully by factory
                raise UpstreamAuditFailureError(
                    f"Asset {asset_key} skipped due to upstream audit failures: "
                    f"{[str(key) for key in failed_asset_keys]}"
                )
            else:
                # Normal MaterializeResult for unaffected assets
                snapshot = assetkey_to_snapshot.get(asset_key)
                if snapshot:
                    snapshot_version = getattr(snapshot, "version", None)
                    model_partitions = get_model_partitions_from_plan(
                        plan, self.translator, asset_key, snapshot
                    )
                    # Prepare base metadata
                    metadata = {
                        "dagster-sqlmesh/snapshot_version": snapshot_version,
                        "dagster-sqlmesh/snapshot_timestamp": convert_unix_timestamp_to_readable(
                            getattr(snapshot, "created_ts", None)
                        )
                        if snapshot
                        else None,
                        "dagster-sqlmesh/model_name": asset_key.path[-1]
                        if asset_key.path
                        else None,
                    }

                    # Add partition metadata if model is partitioned
                    if model_partitions and model_partitions.get(
                        "is_partitioned", False
                    ):
                        metadata["dagster-sqlmesh/partitions"] = (
                            format_partition_metadata(model_partitions)
                        )

                    # Get check results for this asset
                    check_results = asset_check_results_by_key.get(asset_key, [])

                    yield MaterializeResult(
                        asset_key=asset_key,
                        metadata=metadata,
                        data_version=DataVersion(str(snapshot_version))
                        if snapshot_version
                        else None,
                        check_results=check_results,
                    )

        # Clean console events after emitting all AssetCheckResult
        self._console.clear_events()
