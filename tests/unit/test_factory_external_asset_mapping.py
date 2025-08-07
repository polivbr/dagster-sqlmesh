"""
Tests for the external_asset_mapping parameter in sqlmesh_definitions_factory.
"""

import warnings
import pytest
from pathlib import Path

from dg_sqlmesh import sqlmesh_definitions_factory
from dg_sqlmesh.translator import SQLMeshTranslator
from dg_sqlmesh.components.sqlmesh_project.component import JinjaSQLMeshTranslator


class TestExternalAssetMapping:
    """Test the external_asset_mapping parameter functionality."""

    @pytest.fixture
    def project_path(self):
        """Get the path to the test SQLMesh project."""
        return Path(__file__).parent.parent / "fixtures" / "sqlmesh_project"

    def test_external_asset_mapping_creates_jinja_translator(self, project_path):
        """Test that external_asset_mapping creates a JinjaSQLMeshTranslator."""
        defs = sqlmesh_definitions_factory(
            project_dir=str(project_path),
            gateway="duckdb",
            environment="dev",
            external_asset_mapping="target/main/{node.name}",
            enable_schedule=False,
        )

        # Check that assets were created
        assert len(defs.assets) > 0

        # Check that the SQLMesh resource has the correct translator
        sqlmesh_resource = defs.resources["sqlmesh"]
        assert isinstance(sqlmesh_resource.translator, JinjaSQLMeshTranslator)

    def test_external_asset_mapping_with_custom_template(self, project_path):
        """Test external_asset_mapping with a custom template."""
        defs = sqlmesh_definitions_factory(
            project_dir=str(project_path),
            gateway="duckdb",
            environment="dev",
            external_asset_mapping="sling/{node.database}/{node.schema}/{node.name}",
            enable_schedule=False,
        )

        # Check that assets were created
        assert len(defs.assets) > 0

        # Check that the translator is correctly configured
        sqlmesh_resource = defs.resources["sqlmesh"]
        translator = sqlmesh_resource.translator
        assert isinstance(translator, JinjaSQLMeshTranslator)
        assert translator.external_asset_mapping_template == "sling/{node.database}/{node.schema}/{node.name}"

    def test_custom_translator_takes_priority(self, project_path):
        """Test that custom translator takes priority over external_asset_mapping."""
        custom_translator = SQLMeshTranslator()
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            defs = sqlmesh_definitions_factory(
                project_dir=str(project_path),
                gateway="duckdb",
                environment="dev",
                translator=custom_translator,
                external_asset_mapping="target/main/{node.name}",
                enable_schedule=False,
            )

            # Check that a warning was issued
            conflict_warnings = [warning for warning in w if "CONFLICT DETECTED" in str(warning.message)]
            assert len(conflict_warnings) == 1
            warning_message = str(conflict_warnings[0].message)
            assert "CONFLICT DETECTED" in warning_message
            assert "translator" in warning_message
            assert "external_asset_mapping" in warning_message

            # Check that assets were created
            assert len(defs.assets) > 0

            # Check that the custom translator was used (not JinjaSQLMeshTranslator)
            sqlmesh_resource = defs.resources["sqlmesh"]
            assert isinstance(sqlmesh_resource.translator, SQLMeshTranslator)
            assert sqlmesh_resource.translator is custom_translator

    def test_default_translator_when_neither_provided(self, project_path):
        """Test that default translator is used when neither translator nor external_asset_mapping is provided."""
        defs = sqlmesh_definitions_factory(
            project_dir=str(project_path),
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )

        # Check that assets were created
        assert len(defs.assets) > 0

        # Check that the default translator was used
        sqlmesh_resource = defs.resources["sqlmesh"]
        assert isinstance(sqlmesh_resource.translator, SQLMeshTranslator)

    def test_external_asset_mapping_with_various_templates(self, project_path):
        """Test external_asset_mapping with various template formats."""
        templates = [
            "target/main/{node.name}",
            "{node.database}/{node.schema}/{node.name}",
            "sling/{node.name}",
            "{node.name}",
        ]

        for template in templates:
            defs = sqlmesh_definitions_factory(
                project_dir=str(project_path),
                gateway="duckdb",
                environment="dev",
                external_asset_mapping=template,
                enable_schedule=False,
            )

            # Check that assets were created
            assert len(defs.assets) > 0

            # Check that the translator is correctly configured
            sqlmesh_resource = defs.resources["sqlmesh"]
            translator = sqlmesh_resource.translator
            assert isinstance(translator, JinjaSQLMeshTranslator)
            assert translator.external_asset_mapping_template == template

    def test_external_asset_mapping_translates_external_assets(self, project_path):
        """Test that external_asset_mapping correctly translates external assets."""
        defs = sqlmesh_definitions_factory(
            project_dir=str(project_path),
            gateway="duckdb",
            environment="dev",
            external_asset_mapping="target/main/{node.name}",
            enable_schedule=False,
        )

        # Get the translator
        sqlmesh_resource = defs.resources["sqlmesh"]
        translator = sqlmesh_resource.translator

        # Test translation of an external asset
        external_fqn = '"jaffle_db"."main"."raw_source_customers"'
        asset_key = translator.get_external_asset_key(external_fqn)
        
        # Should translate to target/main/raw_source_customers
        from dagster import AssetKey
        assert asset_key == AssetKey(["target", "main", "raw_source_customers"])
