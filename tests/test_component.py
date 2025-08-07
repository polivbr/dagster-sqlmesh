"""
Tests for the SQLMesh Dagster component.
"""

import pytest
from pathlib import Path
from dagster import Definitions
from dg_sqlmesh import SQLMeshProjectComponent


class TestSQLMeshComponent:
    """Test the SQLMesh component functionality."""

    def test_component_import(self):
        """Test that the component can be imported."""
        from dg_sqlmesh import SQLMeshProjectComponent, SQLMeshProjectComponentScaffolder
        assert SQLMeshProjectComponent is not None
        assert SQLMeshProjectComponentScaffolder is not None

    def test_component_creation(self):
        """Test creating a component instance."""
        component = SQLMeshProjectComponent(
            project="tests/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            concurrency_limit=1,
            name="test_assets",
            group_name="test_sqlmesh",
            enable_schedule=False,
        )
        
        assert component.project == "tests/sqlmesh_project"
        assert component.gateway == "duckdb"
        assert component.environment == "dev"
        assert component.concurrency_limit == 1
        assert component.name == "test_assets"
        assert component.group_name == "test_sqlmesh"
        assert component.enable_schedule is False

    def test_component_build_defs(self):
        """Test that the component can build definitions."""
        component = SQLMeshProjectComponent(
            project="tests/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            concurrency_limit=1,
            name="test_assets",
            group_name="test_sqlmesh",
            enable_schedule=False,
        )
        
        # Mock the ComponentLoadContext
        class MockContext:
            pass
        
        context = MockContext()
        
        # This should not raise an exception
        defs = component.build_defs(context)
        assert isinstance(defs, Definitions)

    def test_component_with_retry_policy(self):
        """Test component with retry policy configuration."""
        from dagster import RetryPolicy, Backoff
        
        component = SQLMeshProjectComponent(
            project="tests/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            retry_policy=RetryPolicy(
                max_retries=2,
                delay=10.0,
                backoff=Backoff.EXPONENTIAL,
            ),
            enable_schedule=False,
        )
        
        assert component.retry_policy is not None
        assert component.retry_policy.max_retries == 2
        assert component.retry_policy.delay == 10.0

    def test_component_with_op_tags(self):
        """Test component with op tags configuration."""
        component = SQLMeshProjectComponent(
            project="tests/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            op_tags={"team": "data", "env": "test"},
            enable_schedule=False,
        )
        
        assert component.op_tags is not None
        assert component.op_tags["team"] == "data"
        assert component.op_tags["env"] == "test"

    def test_component_translator(self):
        """Test that the component creates a translator."""
        component = SQLMeshProjectComponent(
            project="tests/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )
        
        translator = component.translator
        assert translator is not None
        # Should be a SQLMeshTranslator instance
        from dg_sqlmesh import SQLMeshTranslator
        assert isinstance(translator, SQLMeshTranslator)

    def test_component_sqlmesh_resource(self):
        """Test that the component creates a SQLMesh resource."""
        component = SQLMeshProjectComponent(
            project="tests/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )
        
        resource = component.sqlmesh_resource
        assert resource is not None
        # Should be a SQLMeshResource instance
        from dg_sqlmesh import SQLMeshResource
        assert isinstance(resource, SQLMeshResource)
        assert resource.project_dir == "tests/sqlmesh_project"
        assert resource.gateway == "duckdb"
        assert resource.environment == "dev"


class TestSQLMeshComponentScaffolder:
    """Test the SQLMesh component scaffolder."""

    def test_scaffolder_import(self):
        """Test that the scaffolder can be imported."""
        from dg_sqlmesh import SQLMeshProjectComponentScaffolder
        assert SQLMeshProjectComponentScaffolder is not None

    def test_scaffolder_params(self):
        """Test scaffolder parameters."""
        from dg_sqlmesh import SQLMeshProjectComponentScaffolder
        
        params_class = SQLMeshProjectComponentScaffolder.get_scaffold_params()
        assert params_class is not None
        
        # Test default values
        params = params_class()
        assert params.init is False
        assert params.project_path is None

    def test_scaffolder_with_project_path(self):
        """Test scaffolder with project path."""
        from dg_sqlmesh import SQLMeshProjectComponentScaffolder
        from dagster.components.scaffold.scaffold import ScaffoldRequest
        from pydantic import BaseModel
        
        class MockParams(BaseModel):
            init: bool = False
            project_path: str = "test_project"
        
        request = ScaffoldRequest(
            project_root="/tmp",
            params=MockParams(),
            component_name="test",
        )
        
        scaffolder = SQLMeshProjectComponentScaffolder()
        # This should not raise an exception
        assert scaffolder is not None


if __name__ == "__main__":
    pytest.main([__file__])
