import threading
import anyio
import logging
import datetime
from typing import Any
from pydantic import PrivateAttr
from dagster import (
    ConfigurableResource, 
    MaterializeResult, 
    DataVersion, 
    AssetCheckResult,
    InitResourceContext
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
    concurrency_limit: int = 1
    ignore_cron: bool = False
    
    # Private attribute for Dagster logger (not subject to Pydantic immutability)
    _logger: Any = PrivateAttr(default=None)
    
    # Singleton for SQLMesh console (lazy initialized)
    
    def __init__(self, **kwargs):
        # Extract translator before calling super().__init__
        translator = kwargs.pop('translator', None)
        super().__init__(**kwargs)
        
        # Store translator for later use
        if translator:
            self._translator_instance = translator
            
        # Create SQLMesh console at initialization
        self._console = self._get_or_create_console()
        
        # Configure translator in console after creation
        if hasattr(self, '_translator_instance') and self._translator_instance:
            self._console._translator = self._translator_instance
        

    def __del__(self):
        pass  # Simplified cleanup

    @property
    def logger(self):
        """Returns the logger for this resource."""
        return logging.getLogger(__name__)

    @classmethod
    def _get_or_create_console(cls) -> 'SQLMeshEventCaptureConsole':
        """Creates or returns the singleton instance of the SQLMesh event console."""
        # Initialize class variables lazily
        if not hasattr(cls, '_console_instance'):
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
        if not hasattr(self, '_context_cache'):
            # Configure custom console before creating context
            console = self._get_or_create_console()
            console._dagster_logger = self._logger  # Update logger
            
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
        if not hasattr(self, '_translator_cache'):
            # Use translator provided as parameter or create a new one
            self._translator_cache = getattr(self, '_translator_instance', None) or SQLMeshTranslator()
        return self._translator_cache

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # Store Dagster logger in private attribute
        self._logger = context.log
        
        # Configure console with Dagster logger
        if hasattr(self, '_console') and self._console:
            self._console._dagster_logger = self._logger

    def get_models(self):
        """
        Returns all SQLMesh models. Cached for performance.
        """
        if not hasattr(self, '_models_cache'):
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
        """
        Serializes audit arguments to JSON-compatible format.
        """
        if not audit_args:
            return {}
        
        serialized = {}
        for key, value in audit_args.items():
            try:
                # Try to convert to string if it's a complex object
                if hasattr(value, '__str__'):
                    serialized[key] = str(value)
                elif hasattr(value, '__dict__'):
                    # For objects with __dict__, extract main attributes
                    serialized[key] = {k: str(v) for k, v in value.__dict__.items() if not k.startswith('_')}
                else:
                    # Fallback: direct conversion
                    serialized[key] = str(value)
            except Exception:
                # In case of error, use simple representation
                serialized[key] = f"<non-serializable: {type(value).__name__}>"
        
        return serialized

    def materialize_assets(self, models, context=None):
        """
        Materializes specified SQLMesh assets with robust error handling.
        """
        model_names = [model.name for model in models]
        # Ensure our console is active for SQLMesh
        set_console(self._console)
        self._console.clear_events()
        
        try:
            plan = self.context.plan(
                select_models=model_names,
                auto_apply=False, # never apply the plan, we will juste need it for metadata collection
                no_prompts=True
            )
            self.context.run(
                ignore_cron=self.ignore_cron,
                select_models=model_names,
                execution_time=datetime.datetime.now(),
            )
            return plan

        except CircuitBreakerError:
            self._logger.error("Run interrupted: environment changed during execution.")
            raise
        except (PlanError, ConflictingPlanError, NoChangesPlanError, UncategorizedPlanError) as e:
            self._logger.error(f"Planning error: {e}")
            raise
        except (AuditError, NodeAuditsErrors) as e:
            self._logger.error(f"Audit error: {e}")
            raise
        except (PythonModelEvalError, SignalEvalError) as e:
            self._logger.error(f"Model or signal execution error: {e}")
            raise
        except SQLMeshError as e:
            self._logger.error(f"SQLMesh error: {e}")
            raise
        except Exception as e:
            self._logger.error(f"Unexpected error: {e}")
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

        # Create MaterializeResult with plan info
        for asset_key in ordered_asset_keys:
            snapshot = assetkey_to_snapshot.get(asset_key)
            if snapshot:
                snapshot_version = getattr(snapshot, "version", None)
                model_partitions = get_model_partitions_from_plan(plan, self.translator, asset_key, snapshot)
                # Prepare base metadata
                metadata = {
                    "dagster-sqlmesh/snapshot_version": snapshot_version,
                    "dagster-sqlmesh/snapshot_timestamp": convert_unix_timestamp_to_readable(getattr(snapshot, "created_ts", None)) if snapshot else None,
                    "dagster-sqlmesh/model_name": asset_key.path[-1] if asset_key.path else None,
                }
                
                # Add partition metadata if model is partitioned
                if model_partitions and model_partitions.get("is_partitioned", False):
                    metadata["dagster-sqlmesh/partitions"] = format_partition_metadata(model_partitions)
                
                yield MaterializeResult(
                    asset_key=asset_key,
                    metadata=metadata,
                    data_version=DataVersion(str(snapshot_version)) if snapshot_version else None
                )
        
        # Emit AssetCheckResult after all MaterializeResult
        audit_results = self._console.get_audit_results()
        for audit_result in audit_results:
            audit_details = audit_result['audit_details']
            asset_key = audit_result['asset_key']
            
            # Determine if audit passed (for now assume True, we'll refine later)
            passed = True  # TODO: determine real status based on events
            
            # Serialize audit arguments to JSON-compatible format
            serialized_args = self._serialize_audit_args(audit_details['arguments'])
            
            yield AssetCheckResult(
                passed=passed,
                asset_key=asset_key,
                check_name=audit_details['name'],
                metadata={
                    "sqlmesh_model_name": audit_result['model_name'],  # ← SQLMesh model name
                    "audit_query": audit_details['sql'],
                    "audit_blocking": audit_details['blocking'],
                    "audit_dialect": getattr(audit_details, 'dialect', 'unknown'),
                    "audit_args": serialized_args
                }
            )
        
        # Clean console events after emitting all AssetCheckResult
        self._console.clear_events()