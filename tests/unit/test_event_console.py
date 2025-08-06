import pytest
from unittest.mock import Mock, MagicMock
from sqlmesh.utils.errors import NodeAuditsErrors
from dg_sqlmesh.sqlmesh_event_console import SQLMeshEventCaptureConsole, LogFailedModels
from dg_sqlmesh.sqlmesh_event_console import LogStatusUpdate
from dg_sqlmesh.sqlmesh_asset_check_utils import extract_successful_audit_results
from dg_sqlmesh.sqlmesh_event_console import UpdateSnapshotEvaluationProgress
from sqlmesh.core.snapshot import Snapshot
from dagster import AssetKey


class TestSQLMeshEventConsole:
    """Test the SQLMesh event console functionality."""

    def test_handle_log_failed_models_basic(self) -> None:
        """Test basic failed models handling - console just captures raw events."""
        console = SQLMeshEventCaptureConsole()
        
        # Create a mock error
        mock_error = Mock()
        mock_error.node = ["test_model"]
        mock_error.__cause__ = None
        
        event = LogFailedModels(errors=[mock_error])
        
        # Call the method
        console._handle_log_failed_models(event)
        
        # Check that an event was logged in the failed_models_events list
        assert len(console.failed_models_events) == 1
        event_info = console.failed_models_events[0]
        
        assert event_info['event_type'] == 'log_failed_models'
        assert 'errors' in event_info
        assert len(event_info['errors']) == 1
        assert event_info['errors'][0] == mock_error

    def test_handle_log_failed_models_with_audit_errors(self) -> None:
        """Test failed models handling with audit errors - console just captures raw events."""
        console = SQLMeshEventCaptureConsole()
        
        # Create mock audit error
        mock_audit_error = Mock()
        mock_audit_error.audit_name = "test_audit"
        mock_audit_error.audit_args = {"arg1": "value1"}
        mock_audit_error.blocking = True
        mock_audit_error.skip = False
        mock_audit_error.__str__ = Mock(return_value="Audit failed")
        
        # Create NodeAuditsErrors
        mock_audits_errors = Mock(spec=NodeAuditsErrors)
        mock_audits_errors.errors = [mock_audit_error]
        
        # Create main error
        mock_error = Mock()
        mock_error.node = ["test_model"]
        mock_error.__cause__ = mock_audits_errors
        
        event = LogFailedModels(errors=[mock_error])
        
        # Call the method
        console._handle_log_failed_models(event)
        
        # Check that an event was logged
        assert len(console.failed_models_events) == 1
        event_info = console.failed_models_events[0]
        
        assert event_info['event_type'] == 'log_failed_models'
        assert 'errors' in event_info
        assert len(event_info['errors']) == 1
        assert event_info['errors'][0] == mock_error

    def test_get_failed_models_events(self) -> None:
        """Test that get_failed_models_events returns the captured events."""
        console = SQLMeshEventCaptureConsole()
        
        # Add some test events
        test_event = {
            'event_type': 'log_failed_models',
            'errors': [Mock()],
            'timestamp': 1234567890.0
        }
        console.failed_models_events.append(test_event)
        
        # Get events
        events = console.get_failed_models_events()
        
        assert len(events) == 1
        assert events[0] == test_event

    def test_clear_events_includes_failed_models(self) -> None:
        """Test that clear_events clears failed_models_events."""
        console = SQLMeshEventCaptureConsole()
        
        # Add some test events
        console.failed_models_events.append({'test': 'event'})
        console.log_events.append({'test': 'log'})
        
        # Clear events
        console.clear_events()
        
        # Check that all event lists are cleared
        assert len(console.failed_models_events) == 0
        assert len(console.log_events) == 0
        assert len(console.audit_results) == 0
        assert len(console.plan_events) == 0
        assert len(console.evaluation_events) == 0 

    def test_handle_log_status_update(self) -> None:
        """Test that LogStatusUpdate events are properly handled."""
        console = SQLMeshEventCaptureConsole()
        
        # Create a LogStatusUpdate event
        event = LogStatusUpdate(message="Test status message")
        
        # Call the method directly
        console._handle_log_status_update(event)
        
        # Check that the event was logged
        assert len(console.log_events) == 1
        event_info = console.log_events[0]
        
        assert event_info['event_type'] == 'log_status_update'
        assert event_info['message'] == "Test status message"

    def test_log_status_update_via_event_handler(self) -> None:
        """Test that LogStatusUpdate events are handled via the main event handler."""
        console = SQLMeshEventCaptureConsole()
        
        # Create a LogStatusUpdate event
        event = LogStatusUpdate(message="Test status via handler")
        
        # Call the main event handler
        console._event_handler(event)
        
        # Check that the event was logged
        assert len(console.log_events) == 1
        event_info = console.log_events[0]
        
        assert event_info['event_type'] == 'log_status_update'
        assert event_info['message'] == "Test status via handler"

    def test_log_status_update_via_publish(self) -> None:
        """Test that LogStatusUpdate events are handled via the publish method."""
        console = SQLMeshEventCaptureConsole()
        
        # Create a LogStatusUpdate event
        event = LogStatusUpdate(message="Test status via publish")
        
        # Call the publish method
        console.publish(event)
        
        # Check that the event was logged
        assert len(console.log_events) == 1
        event_info = console.log_events[0]
        
        assert event_info['event_type'] == 'log_status_update'
        assert event_info['message'] == "Test status via publish" 

    def test_log_status_update_via_publish_known_event(self) -> None:
        """Test that LogStatusUpdate events are handled via publish_known_event (like SQLMesh does)."""
        console = SQLMeshEventCaptureConsole()
        
        # Call publish_known_event like SQLMesh would do
        console.publish_known_event("LogStatusUpdate", message="Test status via publish_known_event")
        
        # Check that the event was logged
        assert len(console.log_events) == 1
        event_info = console.log_events[0]
        
        assert event_info['event_type'] == 'log_status_update'
        assert event_info['message'] == "Test status via publish_known_event" 

    def test_extract_successful_audit_results(self):
        """Test the extract_successful_audit_results utility function"""
        
        # Create mock translator
        mock_translator = Mock()
        mock_translator.get_asset_key.return_value = AssetKey(["test_db", "test_schema", "test_model"])
        
        # Create mock logger
        mock_logger = Mock()
        
        # Test 1: Successful audits (should return results)
        mock_snapshot = Mock()
        mock_snapshot.name = "test_model"
        mock_snapshot.model = Mock()
        mock_snapshot.model.name = "test_model"
        mock_snapshot.model.audits_with_args = [
            (Mock(name="audit1"), {"arg1": "value1"}),
            (Mock(name="audit2"), {"arg2": "value2"})
        ]
        
        event = UpdateSnapshotEvaluationProgress(
            snapshot=mock_snapshot,
            batch_idx=0,
            duration_ms=100,
            num_audits_passed=2,
            num_audits_failed=0
        )
        
        results = extract_successful_audit_results(event, mock_translator, mock_logger)
        
        assert len(results) == 2
        assert results[0]['model_name'] == "test_model"
        assert results[0]['asset_key'] == AssetKey(["test_db", "test_schema", "test_model"])
        assert results[0]['batch_idx'] == 0
        assert 'audit_details' in results[0]
        
        # Test 2: Failed audits (should return empty list)
        event_failed = UpdateSnapshotEvaluationProgress(
            snapshot=mock_snapshot,
            batch_idx=0,
            duration_ms=100,
            num_audits_passed=1,
            num_audits_failed=1
        )
        
        results_failed = extract_successful_audit_results(event_failed, mock_translator, mock_logger)
        assert len(results_failed) == 0
        
        # Test 3: No audits (should return empty list)
        event_no_audits = UpdateSnapshotEvaluationProgress(
            snapshot=mock_snapshot,
            batch_idx=0,
            duration_ms=100,
            num_audits_passed=0,
            num_audits_failed=0
        )
        
        results_no_audits = extract_successful_audit_results(event_no_audits, mock_translator, mock_logger)
        assert len(results_no_audits) == 0
        
        # Test 4: No model with audits (should return empty list)
        mock_snapshot_no_audits = Mock()
        mock_snapshot_no_audits.name = "test_model"
        mock_snapshot_no_audits.model = Mock()
        mock_snapshot_no_audits.model.name = "test_model"
        mock_snapshot_no_audits.model.audits_with_args = []
        
        event_no_model_audits = UpdateSnapshotEvaluationProgress(
            snapshot=mock_snapshot_no_audits,
            batch_idx=0,
            duration_ms=100,
            num_audits_passed=2,
            num_audits_failed=0
        )
        
        results_no_model_audits = extract_successful_audit_results(event_no_model_audits, mock_translator, mock_logger)
        assert len(results_no_model_audits) == 0 