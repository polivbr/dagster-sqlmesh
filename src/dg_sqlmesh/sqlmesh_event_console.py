import inspect
import logging
import textwrap
import typing as t
import uuid
from dataclasses import dataclass, field

from sqlmesh.core.console import Console
from sqlmesh.core.plan import EvaluatablePlan
from sqlmesh.core.snapshot import Snapshot
from .sqlmesh_asset_utils import safe_extract_audit_query

logger = logging.getLogger(__name__)

# =============================================================================
# SQLMESH EVENTS (based on dagster-sqlmesh)
# =============================================================================

@dataclass(kw_only=True)
class BaseConsoleEvent:
    unknown_args: dict[str, t.Any] = field(default_factory=dict)

@dataclass(kw_only=True)
class StartPlanEvaluation(BaseConsoleEvent):
    plan: EvaluatablePlan

@dataclass(kw_only=True)
class StopPlanEvaluation(BaseConsoleEvent):
    pass

@dataclass(kw_only=True)
class StartSnapshotEvaluationProgress(BaseConsoleEvent):
    snapshot: Snapshot

@dataclass(kw_only=True)
class UpdateSnapshotEvaluationProgress(BaseConsoleEvent):
    snapshot: Snapshot
    batch_idx: int
    duration_ms: int | None
    num_audits_passed: int | None = None
    num_audits_failed: int | None = None

@dataclass(kw_only=True)
class StopEvaluationProgress(BaseConsoleEvent):
    success: bool = True

@dataclass(kw_only=True)
class LogStatusUpdate(BaseConsoleEvent):
    message: str

@dataclass(kw_only=True)
class LogError(BaseConsoleEvent):
    message: str

@dataclass(kw_only=True)
class LogWarning(BaseConsoleEvent):
    short_message: str
    long_message: str | None = None

@dataclass(kw_only=True)
class LogSuccess(BaseConsoleEvent):
    message: str

@dataclass(kw_only=True)
class LogFailedModels(BaseConsoleEvent):
    errors: list[t.Any]  # NodeExecutionFailedError

@dataclass(kw_only=True)
class LogSkippedModels(BaseConsoleEvent):
    snapshot_names: set[str]

@dataclass(kw_only=True)
class ConsoleException(BaseConsoleEvent):
    exception: Exception

# Union of all possible events
ConsoleEvent = (
    StartPlanEvaluation
    | StopPlanEvaluation
    | StartSnapshotEvaluationProgress
    | UpdateSnapshotEvaluationProgress
    | StopEvaluationProgress
    | LogStatusUpdate
    | LogError
    | LogWarning
    | LogSuccess
    | LogFailedModels
    | LogSkippedModels
    | ConsoleException
)

ConsoleEventHandler = t.Callable[[ConsoleEvent], None]

# =============================================================================
# EVENT CONSOLE (based on dagster-sqlmesh)
# =============================================================================

def get_console_event_by_name(event_name: str) -> type[ConsoleEvent] | None:
    """Get the console event class by name."""
    known_events_classes = t.get_args(ConsoleEvent)
    console_event_map: dict[str, type[ConsoleEvent]] = {
        event.__name__: event for event in known_events_classes
    }
    return console_event_map.get(event_name)

class IntrospectingConsole(Console):
    """A console that dynamically implements methods based on SQLMesh events"""

    events: t.ClassVar[list[type[ConsoleEvent]]] = [
        StartPlanEvaluation,
        StopPlanEvaluation,
        StartSnapshotEvaluationProgress,
        UpdateSnapshotEvaluationProgress,
        StopEvaluationProgress,
        LogStatusUpdate,
        LogError,
        LogWarning,
        LogSuccess,
        LogFailedModels,
        LogSkippedModels,
        ConsoleException,
    ]

    def __init_subclass__(cls):
        super().__init_subclass__()

        known_events_classes = cls.events
        known_events: list[str] = []
        for known_event in known_events_classes:
            assert inspect.isclass(known_event), "event must be a class"
            known_events.append(known_event.__name__)

        # Dynamically create methods for each event
        for method_name in Console.__abstractmethods__:
            if hasattr(cls, method_name):
                if not getattr(getattr(cls, method_name), '__isabstractmethod__', False):
                    logger.debug(f"Skipping {method_name} as it is abstract")
                    continue
            
            logger.debug(f"Checking {method_name}")

            # Convert snake_case to camelCase
            camel_case_method_name = "".join(
                word.capitalize()
                for _, word in enumerate(method_name.split("_"))
            )

            if camel_case_method_name in known_events:
                logger.debug(f"Creating {method_name} for {camel_case_method_name}")
                signature = inspect.signature(getattr(Console, method_name))
                handler = cls.create_event_handler(method_name, camel_case_method_name, signature)
                setattr(cls, method_name, handler)
            else:
                logger.debug(f"Creating {method_name} for unknown event")
                signature = inspect.signature(getattr(Console, method_name))
                handler = cls.create_unknown_event_handler(method_name, signature)
                setattr(cls, method_name, handler)

    @classmethod
    def create_event_handler(cls, method_name: str, event_name: str, signature: inspect.Signature):
        func_signature, call_params = cls.create_signatures_and_params(signature)

        event_handler_str = textwrap.dedent(f"""
        def {method_name}({", ".join(func_signature)}):
            self.publish_known_event('{event_name}', {", ".join(call_params)})
        """)
        exec(event_handler_str)
        return t.cast(t.Callable[[t.Any], t.Any], locals()[method_name])

    @classmethod
    def create_signatures_and_params(cls, signature: inspect.Signature):
        func_signature: list[str] = []
        call_params: list[str] = []
        
        # Separate parameters with and without default values
        required_params = []
        optional_params = []
        
        for param_name, param in signature.parameters.items():
            if param_name == "self":
                func_signature.append("self")
                continue

            param_type_name = param.annotation
            if not isinstance(param_type_name, str):
                param_type_name = param_type_name.__name__
            
            if param.default is inspect._empty:
                # Required parameter (no default value)
                required_params.append((param_name, f"{param_name}: '{param_type_name}'"))
            else:
                # Optional parameter (with default value)
                default_value = param.default
                if isinstance(param.default, str):
                    default_value = f"'{param.default}'"
                optional_params.append((param_name, f"{param_name}: '{param_type_name}' = {default_value}"))
            
            call_params.append(f"{param_name}={param_name}")
        
        # Add required parameters first, then optional ones
        for _, sig in required_params:
            func_signature.append(sig)
        for _, sig in optional_params:
            func_signature.append(sig)
            
        return (func_signature, call_params)

    @classmethod
    def create_unknown_event_handler(cls, method_name: str, signature: inspect.Signature):
        func_signature, call_params = cls.create_signatures_and_params(signature)

        event_handler_str = textwrap.dedent(f"""
        def {method_name}({", ".join(func_signature)}):
            self.publish_unknown_event('{method_name}', {", ".join(call_params)})
        """)
        exec(event_handler_str)
        return t.cast(t.Callable[[t.Any], t.Any], locals()[method_name])

    def __init__(self, log_override: logging.Logger | None = None, **kwargs) -> None:
        # Ignore unsupported kwargs (verbosity, ignore_warnings, etc.)
        self._handlers: dict[str, ConsoleEventHandler] = {}
        self.logger = log_override or logger
        self.id = str(uuid.uuid4())
        self.logger.debug(f"SQLMeshEventConsole[{self.id}]: created")

    def publish_known_event(self, event_name: str, **kwargs: t.Any) -> None:
        console_event = get_console_event_by_name(event_name)
        assert console_event is not None, f"Event {event_name} not found"
        
        expected_kwargs_fields = console_event.__dataclass_fields__
        expected_kwargs: dict[str, t.Any] = {}
        unknown_args: dict[str, t.Any] = {}
        for key, value in kwargs.items():
            if key not in expected_kwargs_fields:
                unknown_args[key] = value
            else:
                expected_kwargs[key] = value
        
        event = console_event(**expected_kwargs, unknown_args=unknown_args)
        self.publish(event)

    def publish(self, event: ConsoleEvent) -> None:
        self.logger.debug(
            f"SQLMeshEventConsole[{self.id}]: sending event {event.__class__.__name__} to {len(self._handlers)} handlers"
        )
        for handler in self._handlers.values():
            handler(event)

    def publish_unknown_event(self, event_name: str, **kwargs: t.Any) -> None:
        self.logger.debug(
            f"SQLMeshEventConsole[{self.id}]: sending unknown '{event_name}' event to {len(self._handlers)} handlers"
        )
        self.logger.debug(f"SQLMeshEventConsole[{self.id}]: unknown event {event_name} {kwargs}")

    def add_handler(self, handler: ConsoleEventHandler) -> str:
        handler_id = str(uuid.uuid4())
        self.logger.debug(f"SQLMeshEventConsole[{self.id}]: Adding handler {handler_id}")
        self._handlers[handler_id] = handler
        return handler_id

    def remove_handler(self, handler_id: str) -> None:
        del self._handlers[handler_id]

# =============================================================================
# CUSTOM CONSOLE FOR CAPTURING AUDITS
# =============================================================================

class SQLMeshEventCaptureConsole(IntrospectingConsole):
    """
    Custom SQLMesh console that captures ALL events:
    - Plan (creation, application)
    - Apply (evaluation, promotion)
    - Audits (results, errors)
    - Debug (logs, errors, success)
    """

    def __init__(self, translator=None, **kwargs):
        super().__init__(**kwargs)
        self._translator = translator  # ← Add translator
        self.audit_results: list[dict[str, t.Any]] = []
        self.audit_stats: dict[str, dict[str, int]] = {}
        self.plan_events: list[dict[str, t.Any]] = []
        self.evaluation_events: list[dict[str, t.Any]] = []
        self.log_events: list[dict[str, t.Any]] = []
        
        # Contextual logger that can be changed dynamically
        # Get log_override from kwargs or use default logger
        self._context_logger = kwargs.get('log_override') or logging.getLogger(__name__)
        # Ensure logger is at INFO level
        self._context_logger.setLevel(logging.INFO)
        
        # Console initialized and ready
        
        # Add our custom handler
        self.add_handler(self._event_handler)
    
    @property
    def context_logger(self):
        """Returns the current contextual logger"""
        return self._context_logger
    
    @context_logger.setter
    def context_logger(self, logger):
        """Allows changing the contextual logger dynamically"""
        self._context_logger = logger

    def _event_handler(self, event: ConsoleEvent) -> None:
        """Main handler that captures ALL SQLMesh events"""
        
        # Debug: display all received events
        self.context_logger.debug(f"🔍 EVENT RECEIVED: {event.__class__.__name__}")
        
        # Capture plan events
        if isinstance(event, StartPlanEvaluation):
            self._handle_start_plan_evaluation(event)
        elif isinstance(event, StopPlanEvaluation):
            self._handle_stop_plan_evaluation(event)
        
        # Capture evaluation events (where audits can trigger)
        elif isinstance(event, StartSnapshotEvaluationProgress):
            self._handle_start_snapshot_evaluation(event)
        elif isinstance(event, UpdateSnapshotEvaluationProgress):
            self._handle_update_snapshot_evaluation(event)
        elif isinstance(event, StopEvaluationProgress):
            self._handle_stop_evaluation(event)
        
        # Capture error logs (for failed audits)
        elif isinstance(event, LogError):
            self._handle_log_error(event)
        elif isinstance(event, LogFailedModels):
            self._handle_log_failed_models(event)
        
        # Capture success logs
        elif isinstance(event, LogSuccess):
            self._handle_log_success(event)
        
        # Capture status logs
        elif isinstance(event, LogStatusUpdate):
            self._handle_log_status_update(event)
        


    def _handle_log_status_update(self, event: LogStatusUpdate) -> None:
        """Captures status logs"""
        # Use Dagster logger if available
        if hasattr(self, '_dagster_logger') and self._dagster_logger:
            self._dagster_logger.info(f"ℹ️ SQLMesh: {event.message}")
        

    def _handle_start_plan_evaluation(self, event: StartPlanEvaluation) -> None:
        """Captures plan start"""
        plan_info = {
            'event_type': 'start_plan_evaluation',
            'plan_id': getattr(event.plan, 'plan_id', 'N/A'),
            'timestamp': t.cast(float, t.Any),
        }
        self.plan_events.append(plan_info)

    def _handle_stop_plan_evaluation(self, event: StopPlanEvaluation) -> None:
        """Captures plan end"""
        plan_info = {
            'event_type': 'stop_plan_evaluation',
            'timestamp': t.cast(float, t.Any),
        }
        self.plan_events.append(plan_info)

    def _handle_start_snapshot_evaluation(self, event: StartSnapshotEvaluationProgress) -> None:
        """Captures snapshot evaluation start (where audits can trigger)"""
        eval_info = {
            'event_type': 'start_snapshot_evaluation',
            'snapshot_name': event.snapshot.name,
            'snapshot_id': str(event.snapshot.snapshot_id),
            'timestamp': t.cast(float, t.Any),
        }
        self.evaluation_events.append(eval_info)

    def _handle_update_snapshot_evaluation(self, event: UpdateSnapshotEvaluationProgress) -> None:
        """Captures updates during evaluation (this is where audits trigger!)"""
        self.context_logger.debug(f"✅ _handle_update_snapshot_evaluation called")
        eval_info = {
            'event_type': 'update_snapshot_evaluation',
            'snapshot_name': event.snapshot.name,
            'batch_idx': event.batch_idx,
            'duration_ms': event.duration_ms,
            'num_audits_passed': event.num_audits_passed,
            'num_audits_failed': event.num_audits_failed,
        }
        self.evaluation_events.append(eval_info)
        
        # Capture audit results via parameters
        if event.num_audits_passed is not None or event.num_audits_failed is not None:
            if hasattr(self, '_dagster_logger') and self._dagster_logger:
                self._dagster_logger.info(f"✅ AUDITS RESULTS: {event.num_audits_passed} passed, {event.num_audits_failed} failed")
            
            # If we have audits in this snapshot, we can capture them here
            if hasattr(event.snapshot, 'model') and hasattr(event.snapshot.model, 'audits_with_args') and event.snapshot.model.audits_with_args:
                audit_results = []
                for audit_obj, audit_args in event.snapshot.model.audits_with_args:
                    try:
                        # Use existing translator to get asset_key
                        asset_key = self._translator.get_asset_key(event.snapshot.model) if self._translator else None
                        
                        audit_result = {
                            'model_name': event.snapshot.model.name,
                            'asset_key': asset_key,
                            'audit_details': self._extract_audit_details(audit_obj, audit_args, event.snapshot.model),
                            'batch_idx': event.batch_idx,
                        }
                        audit_results.append(audit_result)
                    except Exception as e:
                        self._dagster_logger.warning(f"⚠️ Error capturing audit: {e}")
                        continue
                
                self.audit_results.extend(audit_results)

    def _extract_audit_details(self, audit_obj, audit_args, model):
        """Extracts all useful information from an audit"""
        
        # Use utility function
        sql_query = safe_extract_audit_query(
            model=model,
            audit_obj=audit_obj,
            audit_args=audit_args,
            logger=self._dagster_logger if hasattr(self, '_dagster_logger') else None
        )
        
        return {
            'name': getattr(audit_obj, 'name', 'unknown'),
            'sql': sql_query,
            'blocking': getattr(audit_obj, 'blocking', False),
            'skip': getattr(audit_obj, 'skip', False),
            'arguments': audit_args
        }

    def _handle_stop_evaluation(self, event: StopEvaluationProgress) -> None:
        """Captures evaluation end"""
        eval_info = {
            'event_type': 'stop_evaluation',
            'success': event.success,
            'timestamp': t.cast(float, t.Any),
        }
        self.evaluation_events.append(eval_info)

    def _handle_log_error(self, event: LogError) -> None:
        """Captures errors (including failed audits)"""
        error_info = {
            'event_type': 'log_error',
            'message': event.message,
            'timestamp': t.cast(float, t.Any),
        }
        self.log_events.append(error_info)

    def _handle_log_failed_models(self, event: LogFailedModels) -> None:
        """Captures failed models"""
        for error in event.errors:
            error_info = {
                'event_type': 'log_failed_model',
                'error': str(error),
                'timestamp': t.cast(float, t.Any),
            }
            self.log_events.append(error_info)

    def _handle_log_success(self, event: LogSuccess) -> None:
        """Captures successes"""
        success_info = {
            'event_type': 'log_success',
            'message': event.message,
            'timestamp': t.cast(float, t.Any),
        }
        self.log_events.append(success_info)

    def get_audit_results(self) -> list[dict[str, t.Any]]:
        """Returns all captured audit results"""
        return self.audit_results

    def get_evaluation_events(self) -> list[dict[str, t.Any]]:
        """Returns all evaluation events"""
        return self.evaluation_events

    def get_plan_events(self) -> list[dict[str, t.Any]]:
        """Returns all plan events"""
        return self.plan_events

    def get_all_events(self) -> dict[str, list[dict[str, t.Any]]]:
        """Returns ALL captured events organized by category"""
        return {
            'audit_results': self.audit_results,
            'evaluation_events': self.evaluation_events,
            'plan_events': self.plan_events,
            'log_events': self.log_events,
        }

    def clear_events(self) -> None:
        """Clears all captured events"""
        self.audit_results.clear()
        self.audit_stats.clear()
        self.plan_events.clear()
        self.evaluation_events.clear()
        self.log_events.clear() 

 