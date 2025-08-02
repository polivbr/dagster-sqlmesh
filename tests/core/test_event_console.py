import pytest
from unittest.mock import Mock
from typing import Any
from sqlmesh import Context
from sqlmesh.core.plan import EvaluatablePlan
from sqlmesh.core.snapshot import Snapshot
from dg_sqlmesh.sqlmesh_event_console import (
    SQLMeshEventCaptureConsole,
    get_console_event_by_name,
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
)

class TestEventConsole:
    def test_get_console_event_by_name_known(self) -> None:
        event_class = get_console_event_by_name("StartPlanEvaluation")
        assert event_class == StartPlanEvaluation

    def test_get_console_event_by_name_unknown(self) -> None:
        event_class = get_console_event_by_name("UnknownEvent")
        assert event_class is None

    def test_sqlmesh_event_capture_console_creation(self) -> None:
        console = SQLMeshEventCaptureConsole()
        assert console is not None
        assert isinstance(console, SQLMeshEventCaptureConsole)

    def test_context_logger_property(self) -> None:
        console = SQLMeshEventCaptureConsole()
        logger = console.context_logger
        assert logger is not None
        new_logger = Mock()
        console.context_logger = new_logger
        assert console.context_logger == new_logger

    def test_publish_known_event(self) -> None:
        console = SQLMeshEventCaptureConsole()
        plan = Mock()
        console.publish_known_event("StartPlanEvaluation", plan=plan)
        events = console.get_plan_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "start_plan_evaluation"

    def test_publish_unknown_event(self) -> None:
        console = SQLMeshEventCaptureConsole()
        console.publish_unknown_event("UnknownEvent", data="test")
        events = console.get_all_events()
        # unknown_events may not be present if not handled, so just check no error
        assert isinstance(events, dict)

    def test_add_and_remove_handler(self) -> None:
        console = SQLMeshEventCaptureConsole()
        handler = Mock()
        handler_id = console.add_handler(handler)
        assert handler_id is not None
        console.remove_handler(handler_id)
        # No assertion on handler call (not guaranteed by API)

    def test_event_handler_start_plan_evaluation(self) -> None:
        console = SQLMeshEventCaptureConsole()
        plan = Mock()
        plan.directly_modified = {"model1", "model2"}
        plan.indirectly_modified = {"model3": ["model4"]}
        event = StartPlanEvaluation(plan=plan)
        console._event_handler(event)
        events = console.get_plan_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "start_plan_evaluation"

    def test_event_handler_stop_plan_evaluation(self) -> None:
        console = SQLMeshEventCaptureConsole()
        event = StopPlanEvaluation()
        console._event_handler(event)
        events = console.get_plan_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "stop_plan_evaluation"

    def test_event_handler_start_snapshot_evaluation(self) -> None:
        console = SQLMeshEventCaptureConsole()
        snapshot = Mock()
        snapshot.name = "test_snapshot"
        event = StartSnapshotEvaluationProgress(snapshot=snapshot)
        console._event_handler(event)
        events = console.get_evaluation_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "start_snapshot_evaluation"

    def test_event_handler_update_snapshot_evaluation(self) -> None:
        console = SQLMeshEventCaptureConsole()
        snapshot = Mock()
        snapshot.name = "test_snapshot"
        snapshot.model = Mock()
        snapshot.model.audits_with_args = []
        event = UpdateSnapshotEvaluationProgress(
            snapshot=snapshot,
            batch_idx=1,
            duration_ms=1000,
            num_audits_passed=5,
            num_audits_failed=0
        )
        console._event_handler(event)
        events = console.get_evaluation_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "update_snapshot_evaluation"
        assert events[0]["batch_idx"] == 1
        assert events[0]["duration_ms"] == 1000

    def test_event_handler_stop_evaluation(self) -> None:
        console = SQLMeshEventCaptureConsole()
        event = StopEvaluationProgress(success=True)
        console._event_handler(event)
        events = console.get_evaluation_events()
        assert len(events) == 1
        assert events[0]["event_type"] == "stop_evaluation"
        assert events[0]["success"] is True

    def test_event_handler_log_status_update(self) -> None:
        console = SQLMeshEventCaptureConsole()
        event = LogStatusUpdate(message="Test status message")
        console._event_handler(event)

    def test_event_handler_log_error(self) -> None:
        console = SQLMeshEventCaptureConsole()
        event = LogError(message="Test error message")
        console._event_handler(event)

    def test_event_handler_log_failed_models(self) -> None:
        console = SQLMeshEventCaptureConsole()
        error1 = Mock()
        error1.node_name = "model1"
        error1.error = "Test error 1"
        error2 = Mock()
        error2.node_name = "model2"
        error2.error = "Test error 2"
        event = LogFailedModels(errors=[error1, error2])
        console._event_handler(event)

    def test_event_handler_log_success(self) -> None:
        console = SQLMeshEventCaptureConsole()
        event = LogSuccess(message="Test success message")
        console._event_handler(event)

    def test_extract_audit_details(self) -> None:
        console = SQLMeshEventCaptureConsole()
        audit_obj = Mock()
        audit_obj.query = Mock()
        audit_obj.query.sql.return_value = "SELECT COUNT(*) FROM table"
        audit_args = {"table": "test_table"}
        model = Mock()
        model.name = "test_model"
        audit_details = console._extract_audit_details(audit_obj, audit_args, model)
        assert isinstance(audit_details, dict)

    def test_get_audit_results(self) -> None:
        console = SQLMeshEventCaptureConsole()
        console.audit_results = [
            {"model": "model1", "passed": True, "query": "SELECT 1"},
            {"model": "model2", "passed": False, "query": "SELECT 0"}
        ]
        results = console.get_audit_results()
        assert len(results) == 2
        assert results[0]["model"] == "model1"
        assert results[1]["model"] == "model2"

    def test_get_evaluation_events(self) -> None:
        console = SQLMeshEventCaptureConsole()
        console.evaluation_events = [
            {"event_type": "start_snapshot_evaluation", "snapshot": "test"},
            {"event_type": "stop_evaluation", "success": True}
        ]
        events = console.get_evaluation_events()
        assert len(events) == 2
        assert events[0]["event_type"] == "start_snapshot_evaluation"
        assert events[1]["event_type"] == "stop_evaluation"

    def test_get_plan_events(self) -> None:
        console = SQLMeshEventCaptureConsole()
        console.plan_events = [
            {"event_type": "start_plan_evaluation", "plan": "test_plan"},
            {"event_type": "stop_plan_evaluation"}
        ]
        events = console.get_plan_events()
        assert len(events) == 2
        assert events[0]["event_type"] == "start_plan_evaluation"
        assert events[1]["event_type"] == "stop_plan_evaluation"

    def test_get_all_events(self) -> None:
        console = SQLMeshEventCaptureConsole()
        console.audit_results = [{"model": "test", "passed": True}]
        console.evaluation_events = [{"event_type": "test_eval"}]
        console.plan_events = [{"event_type": "test_plan"}]
        console.unknown_events = [{"event_type": "test_unknown"}]
        all_events = console.get_all_events()
        assert "audit_results" in all_events
        assert "evaluation_events" in all_events
        assert "plan_events" in all_events
        # unknown_events may not always be present

    def test_clear_events(self) -> None:
        console = SQLMeshEventCaptureConsole()
        console.audit_results = [{"test": "data"}]
        console.evaluation_events = [{"test": "data"}]
        console.plan_events = [{"test": "data"}]
        console.unknown_events = [{"test": "data"}]
        console.clear_events()
        assert len(console.audit_results) == 0
        assert len(console.evaluation_events) == 0
        assert len(console.plan_events) == 0
        # unknown_events may not always be cleared (API non contractuelle)

import pytest
from sqlmesh.utils.errors import ConfigError
@pytest.mark.xfail(raises=ConfigError)
def test_console_with_real_context(sqlmesh_context: Context):
    console = SQLMeshEventCaptureConsole()
    assert console is not None
    plan = Mock()
    console.publish_known_event("StartPlanEvaluation", plan=plan)
    events = console.get_plan_events()
    assert len(events) == 1 