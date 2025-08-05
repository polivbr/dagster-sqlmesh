import pytest
from pathlib import Path
from typing import Any, Dict, List, Optional

from dagster import (
    AssetKey,
    # Removed RetryPolicy
    Definitions,
    materialize,
)
from dg_sqlmesh import (
    sqlmesh_assets_factory,
    sqlmesh_definitions_factory,
    sqlmesh_adaptive_schedule_factory,
    SQLMeshResource,
    SQLMeshTranslator,
)


class TestSQLMeshAssetsFactory:
    """Test SQLMesh assets factory."""
    
    def test_sqlmesh_assets_factory_basic(self, sqlmesh_resource: SQLMeshResource) -> None:
        """Test basic asset factory functionality."""
        assets = sqlmesh_assets_factory(
            sqlmesh_resource=sqlmesh_resource,
            group_name="test_group"
        )
        
        # Check that assets are created
        assert len(assets) > 0
        
        # Check that all assets have the correct group
        for asset in assets:
            assert asset.group_name == "test_group"
    
    def test_sqlmesh_assets_factory_with_op_tags(self, sqlmesh_resource: SQLMeshResource) -> None:
        """Test asset factory with op tags."""
        op_tags = {"team": "data", "env": "test"}
        
        assets = sqlmesh_assets_factory(
            sqlmesh_resource=sqlmesh_resource,
            op_tags=op_tags
        )
        
        # Check that op tags are applied
        for asset in assets:
            assert asset.op.tags == op_tags
    
    def test_sqlmesh_assets_factory_with_owners(self, sqlmesh_resource: SQLMeshResource) -> None:
        """Test asset factory with owners."""
        owners = ["data-team", "analytics"]
        
        assets = sqlmesh_assets_factory(
            sqlmesh_resource=sqlmesh_resource,
            owners=owners
        )
        
        # Check that owners are applied to metadata
        for asset in assets:
            metadata = asset.metadata
            assert "owners" in metadata
            assert metadata["owners"] == owners


class TestSQLMeshDefinitionsFactory:
    """Test SQLMesh definitions factory."""
    
    def test_sqlmesh_definitions_factory_basic(self) -> None:
        """Test basic definitions factory functionality."""
        defs = sqlmesh_definitions_factory(
            project_dir="tests/sqlmesh_project",
            gateway="duckdb",
            name="test_defs"
        )
        
        # Check that definitions are created
        assert defs is not None
        assert len(defs.assets) > 0
        assert "sqlmesh" in defs.resources
    
    def test_sqlmesh_definitions_factory_with_custom_translator(self) -> None:
        """Test definitions factory with custom translator."""
        class CustomTranslator(SQLMeshTranslator):
            def get_asset_key(self, model) -> AssetKey:
                return AssetKey(["custom", model.name])
        
        defs = sqlmesh_definitions_factory(
            project_dir="tests/sqlmesh_project",
            gateway="duckdb",
            translator=CustomTranslator()
        )
        
        # Check that custom translator is used
        assert defs is not None
        assert len(defs.assets) > 0
    
    def test_sqlmesh_definitions_factory_with_op_tags(self) -> None:
        """Test definitions factory with op tags."""
        op_tags = {"team": "data", "env": "test"}
        
        defs = sqlmesh_definitions_factory(
            project_dir="tests/sqlmesh_project",
            gateway="duckdb",
            op_tags=op_tags
        )
        
        # Check that op tags are applied to assets
        for asset in defs.assets:
            assert asset.op.tags == op_tags
    
    def test_sqlmesh_definitions_factory_with_owners(self) -> None:
        """Test definitions factory with owners."""
        owners = ["data-team", "analytics"]
        
        defs = sqlmesh_definitions_factory(
            project_dir="tests/sqlmesh_project",
            gateway="duckdb",
            owners=owners
        )
        
        # Check that owners are applied to assets
        for asset in defs.assets:
            metadata = asset.metadata
            assert "owners" in metadata
            assert metadata["owners"] == owners


class TestSQLMeshAdaptiveScheduleFactory:
    """Test the sqlmesh_adaptive_schedule_factory function."""

    def test_sqlmesh_adaptive_schedule_factory_basic(self, sqlmesh_resource: SQLMeshResource) -> None:
        """Test basic adaptive schedule factory creation."""
        schedule, job, assets = sqlmesh_adaptive_schedule_factory(
            sqlmesh_resource=sqlmesh_resource
        )
        
        assert schedule is not None
        assert job is not None
        assert assets is not None
        assert schedule.name == "sqlmesh_adaptive_schedule"
        assert job.name == "sqlmesh_job"

    def test_sqlmesh_adaptive_schedule_factory_with_custom_name(self, sqlmesh_resource: SQLMeshResource) -> None:
        """Test adaptive schedule factory with custom name."""
        custom_name = "my_custom_schedule"
        schedule, job, assets = sqlmesh_adaptive_schedule_factory(
            sqlmesh_resource=sqlmesh_resource,
            name=custom_name
        )
        
        assert schedule.name == custom_name

    def test_sqlmesh_adaptive_schedule_factory_schedule_properties(self, sqlmesh_resource: SQLMeshResource) -> None:
        """Test adaptive schedule factory schedule properties."""
        schedule, job, assets = sqlmesh_adaptive_schedule_factory(
            sqlmesh_resource=sqlmesh_resource
        )
        
        # Check that schedule has required properties
        assert hasattr(schedule, "cron_schedule")
        assert hasattr(schedule, "description")
        assert "Adaptive schedule based on SQLMesh crons" in schedule.description


class TestFactoryIntegration:
    """Integration tests for factory functions."""

    def test_factory_integration_with_materialization(self, sqlmesh_resource: SQLMeshResource) -> None:
        """Test that factory-created assets can be selected and configured."""
        assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)
        
        # Test that assets can be created and have the expected structure
        assert len(assets.keys) > 0
        
        # Test that we can select a specific asset
        from dagster import AssetKey
        target_asset = AssetKey(["jaffle_test", "sqlmesh_jaffle_platform", "stg_customers"])
        
        # Verify the asset exists in the assets definition
        assert target_asset in assets.keys
        
        # Test that the asset has the correct structure
        assert assets.op.name == "sqlmesh_assets"
        assert "sqlmesh" in assets.required_resource_keys

    def test_definitions_factory_integration(self) -> None:
        """Test complete definitions factory integration."""
        defs = sqlmesh_definitions_factory(
            project_dir="tests/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
        )
        
        # Test that definitions can be loaded
        assert defs is not None
        assert len(defs.assets) > 0
        assert len(defs.jobs) > 0
        assert len(defs.schedules) > 0
        
        # Test that resources are properly configured
        assert "sqlmesh" in defs.resources
        sqlmesh_resource = defs.resources["sqlmesh"]
        assert isinstance(sqlmesh_resource, SQLMeshResource) 