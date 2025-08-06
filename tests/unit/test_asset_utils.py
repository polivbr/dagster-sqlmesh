import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import Any, Dict, List, Optional

from dagster import AssetSpec, AssetCheckSpec, AssetKey
from sqlmesh.core.model.definition import ExternalModel
from sqlmesh import Context

from dg_sqlmesh.sqlmesh_asset_utils import (
    get_models_to_materialize,
    get_model_partitions_from_plan,
    get_model_from_asset_key,
    get_topologically_sorted_asset_keys,
    get_asset_kinds,
    get_asset_tags,
    get_asset_metadata,
    format_partition_metadata,
    get_column_descriptions_from_model,
    analyze_sqlmesh_crons_using_api,
    get_dagster_schedule_from_interval,
    validate_external_dependencies,
    create_all_asset_specs,
    create_asset_specs,
    get_extra_keys,
    create_asset_checks,
)
from dg_sqlmesh.sqlmesh_asset_check_utils import safe_extract_audit_query


class TestAssetUtils:
    """Test asset utility functions."""

    def test_get_models_to_materialize_all_models(self) -> None:
        """Test getting all models to materialize."""
        # Mock models
        internal_model1 = Mock()
        internal_model2 = Mock()
        external_model = Mock(spec=ExternalModel)
        
        models = [internal_model1, internal_model2, external_model]
        
        # Mock get_models_func
        get_models_func = Mock(return_value=models)
        
        # Mock translator
        translator = Mock()
        
        result = get_models_to_materialize(None, get_models_func, translator)
        
        assert len(result) == 2
        assert internal_model1 in result
        assert internal_model2 in result
        assert external_model not in result

    def test_get_models_to_materialize_with_selection(self) -> None:
        """Test getting models to materialize with asset selection."""
        from dagster import AssetKey
        
        # Mock models
        internal_model1 = Mock()
        internal_model1.catalog = "catalog1"
        internal_model1.schema_name = "schema1"
        internal_model1.view_name = "view1"
        
        internal_model2 = Mock()
        internal_model2.catalog = "catalog2"
        internal_model2.schema_name = "schema2"
        internal_model2.view_name = "view2"
        
        models = [internal_model1, internal_model2]
        
        # Mock get_models_func
        get_models_func = Mock(return_value=models)
        
        # Mock translator
        translator = Mock()
        translator.get_assetkey_to_model.return_value = {
            AssetKey(["catalog1", "schema1", "view1"]): internal_model1,
            AssetKey(["catalog2", "schema2", "view2"]): internal_model2,
        }
        
        # Test with selection
        selected_asset_keys = [AssetKey(["catalog1", "schema1", "view1"])]
        result = get_models_to_materialize(selected_asset_keys, get_models_func, translator)
        
        assert len(result) == 1
        assert internal_model1 in result
        assert internal_model2 not in result

    def test_get_model_partitions_from_plan_with_partitions(self) -> None:
        """Test getting model partitions from plan with partitioned model."""
        # Mock plan and snapshot
        plan = Mock()
        snapshot = Mock()
        
        # Mock model with partitions
        model = Mock()
        partition1 = Mock()
        partition1.name = "date"
        partition2 = Mock()
        partition2.name = "region"
        model.partitioned_by = [partition1, partition2]
        model.grain = ["day", "region"]
        
        snapshot.model = model
        snapshot.intervals = [{"start": "2023-01-01", "end": "2023-01-02"}]
        
        translator = Mock()
        asset_key = Mock()
        
        result = get_model_partitions_from_plan(plan, translator, asset_key, snapshot)
        
        assert result["partitioned_by"] == ["date", "region"]
        assert result["partition_columns"] == ["date", "region"]
        assert result["grain"] == ["day", "region"]
        assert result["is_partitioned"] is True
        assert len(result["intervals"]) == 1

    def test_get_model_partitions_from_plan_no_partitions(self) -> None:
        """Test getting model partitions from plan with non-partitioned model."""
        # Mock plan and snapshot
        plan = Mock()
        snapshot = Mock()
        
        # Mock model without partitions
        model = Mock()
        model.partitioned_by = []
        model.grain = []
        
        snapshot.model = model
        snapshot.intervals = []
        
        translator = Mock()
        asset_key = Mock()
        
        result = get_model_partitions_from_plan(plan, translator, asset_key, snapshot)
        
        assert result["partitioned_by"] == []
        assert result["partition_columns"] == []
        assert result["grain"] == []
        assert result["is_partitioned"] is False
        assert len(result["intervals"]) == 0

    def test_get_model_from_asset_key(self) -> None:
        """Test getting model from asset key."""
        from dagster import AssetKey
        
        # Mock context and translator
        context = Mock()
        translator = Mock()
        
        # Mock models
        model1 = Mock()
        model2 = Mock()
        models = [model1, model2]
        
        context.models.values.return_value = models
        translator.get_assetkey_to_model.return_value = {
            AssetKey(["catalog1", "schema1", "view1"]): model1,
            AssetKey(["catalog2", "schema2", "view2"]): model2,
        }
        
        asset_key = AssetKey(["catalog1", "schema1", "view1"])
        result = get_model_from_asset_key(context, translator, asset_key)
        
        assert result == model1

    def test_get_topologically_sorted_asset_keys(self) -> None:
        """Test getting topologically sorted asset keys."""
        from dagster import AssetKey
        
        # Mock context and translator
        context = Mock()
        translator = Mock()
        
        # Mock models
        model1 = Mock()
        model1.fqn = "catalog1.schema1.view1"
        model1.catalog = "catalog1"
        model1.schema_name = "schema1"
        model1.view_name = "view1"
        
        model2 = Mock()
        model2.fqn = "catalog2.schema2.view2"
        model2.catalog = "catalog2"
        model2.schema_name = "schema2"
        model2.view_name = "view2"
        
        models = [model1, model2]
        
        context.models.values.return_value = models
        context.dag.sorted = ["catalog1.schema1.view1", "catalog2.schema2.view2"]
        
        translator.get_assetkey_to_model.return_value = {
            AssetKey(["catalog1", "schema1", "view1"]): model1,
            AssetKey(["catalog2", "schema2", "view2"]): model2,
        }
        translator.get_asset_key.side_effect = lambda m: AssetKey([m.catalog, m.schema_name, m.view_name])
        
        selected_asset_keys = [
            AssetKey(["catalog1", "schema1", "view1"]),
            AssetKey(["catalog2", "schema2", "view2"]),
        ]
        
        result = get_topologically_sorted_asset_keys(context, translator, selected_asset_keys)
        
        assert len(result) == 2
        assert result[0] == AssetKey(["catalog1", "schema1", "view1"])
        assert result[1] == AssetKey(["catalog2", "schema2", "view2"])

    # Tests for has_breaking_changes removed - function no longer exists

    def test_get_asset_kinds(self) -> None:
        """Test getting asset kinds."""
        # Mock SQLMesh resource
        sqlmesh_resource = Mock()
        
        # Mock models with different kinds
        model1 = Mock()
        model1.kind = "incremental"
        
        model2 = Mock()
        model2.kind = "full"
        
        model3 = Mock()
        model3.kind = "view"
        
        sqlmesh_resource.get_models.return_value = [model1, model2, model3]
        sqlmesh_resource.translator = Mock()
        sqlmesh_resource.translator._get_context_dialect.return_value = "duckdb"
        
        result = get_asset_kinds(sqlmesh_resource)
        
        # The function returns {"sqlmesh", dialect}, not model kinds
        assert "sqlmesh" in result
        assert "duckdb" in result

    def test_get_asset_tags(self) -> None:
        """Test getting asset tags."""
        # Mock translator and context
        translator = Mock()
        context = Mock()
        
        # Mock model with tags
        model = Mock()
        model.tags = {"team:data", "environment:prod"}
        
        translator.get_tags.return_value = {
            "team:data": "true",
            "environment:prod": "true"
        }
        
        result = get_asset_tags(translator, context, model)
        
        assert result == {
            "team:data": "true",
            "environment:prod": "true"
        }

    def test_get_asset_metadata(self) -> None:
        """Test getting asset metadata."""
        # Mock translator and model
        translator = Mock()
        model = Mock()
        
        # Mock metadata serialization
        translator.serialize_metadata.return_value = {
            "dagster-sqlmesh/name": "test_model",
            "dagster-sqlmesh/type": "table"
        }
        translator.get_table_metadata.return_value = {
            "table_name": "test_table",
            "column_schema": Mock()
        }
        
        code_version = "1.0.0"
        extra_keys = ["name", "type"]
        owners = ["data-team"]
        
        result = get_asset_metadata(translator, model, code_version, extra_keys, owners)
        
        assert "dagster-sqlmesh/name" in result
        assert "dagster-sqlmesh/type" in result
        assert result["owners"] == ["data-team"]

    def test_format_partition_metadata_with_partitions(self) -> None:
        """Test formatting partition metadata with partitions."""
        model_partitions = {
            "partitioned_by": ["date", "region"],
            "intervals": [[1640995200000, 1641081600000]],  # Unix timestamps in milliseconds
            "partition_columns": ["date", "region"],
            "grain": ["day", "region"],
            "is_partitioned": True
        }
        
        result = format_partition_metadata(model_partitions)
        
        assert "partition_columns" in result
        assert "partition_intervals" in result
        assert "partition_grain" in result
        assert len(result["partition_columns"]) == 2

    def test_format_partition_metadata_no_partitions(self) -> None:
        """Test formatting partition metadata without partitions."""
        model_partitions = {
            "partitioned_by": [],
            "intervals": [],
            "partition_columns": [],
            "grain": [],
            "is_partitioned": False
        }
        
        result = format_partition_metadata(model_partitions)
        
        assert result == {}

    def test_get_column_descriptions_from_model(self) -> None:
        """Test getting column descriptions from model."""
        # Mock model with column descriptions
        model = Mock()
        model.column_descriptions = {
            "id": "Primary key",
            "name": "Customer name",
            "email": "Customer email"
        }
        
        result = get_column_descriptions_from_model(model)
        
        assert result == {
            "id": "Primary key",
            "name": "Customer name",
            "email": "Customer email"
        }

    def test_get_column_descriptions_from_model_none(self) -> None:
        """Test getting column descriptions from model with None."""
        # Mock model with None column descriptions
        model = Mock()
        model.column_descriptions = None
        
        result = get_column_descriptions_from_model(model)
        
        # The function should return an empty dict when column_descriptions is None
        # But since it's a mock, we need to check that it's falsy
        assert result == {}

    def test_safe_extract_audit_query_success(self) -> None:
        """Test safe audit query extraction (success)."""
        # Mock audit object and args
        audit_obj = Mock()
        audit_obj.query = Mock()
        audit_obj.query.sql.return_value = "SELECT COUNT(*) FROM table"
        audit_args = {}
        model = Mock()
        model.render_audit_query.return_value = Mock()
        model.render_audit_query.return_value.sql.return_value = "SELECT COUNT(*) FROM table"
        
        result = safe_extract_audit_query(model, audit_obj, audit_args, None)
        
        assert result == "SELECT COUNT(*) FROM table"

    def test_safe_extract_audit_query_exception(self) -> None:
        """Test safe audit query extraction (exception)."""
        # Mock audit object that raises exception
        audit_obj = Mock()
        audit_obj.query = Mock()
        audit_obj.query.sql.side_effect = Exception("Test exception")
        audit_args = {}
        model = Mock()
        model.render_audit_query.side_effect = Exception("Test exception")
        logger = Mock()
        
        result = safe_extract_audit_query(model, audit_obj, audit_args, logger)
        
        assert result == "N/A"
        logger.warning.assert_called()

    def test_analyze_sqlmesh_crons_using_api(self) -> None:
        """Test SQLMesh cron analysis using API."""
        # Mock context with models
        context = Mock()
        
        model1 = Mock()
        model1.cron = "0 0 * * *"  # Daily
        model1.interval_unit = Mock()
        model1.interval_unit.seconds = 86400  # 24 hours
        
        model2 = Mock()
        model2.cron = "0 */6 * * *"  # Every 6 hours
        model2.interval_unit = Mock()
        model2.interval_unit.seconds = 21600  # 6 hours
        
        model3 = Mock()
        model3.cron = None  # No cron
        
        context.models = {
            "model1": model1,
            "model2": model2,
            "model3": model3
        }
        
        result = analyze_sqlmesh_crons_using_api(context)
        
        # Should return a single cron expression (the finest granularity)
        assert isinstance(result, str)
        assert "0 */6 * * *" in result  # Should return 6-hour granularity (finest)

    def test_get_dagster_schedule_from_interval_daily(self) -> None:
        """Test getting Dagster schedule from daily interval."""
        # 24 hours = 86400 seconds
        interval_seconds = 86400
        
        result = get_dagster_schedule_from_interval(interval_seconds)
        
        assert result == "0 0 * * *"

    def test_get_dagster_schedule_from_interval_hourly(self) -> None:
        """Test getting Dagster schedule from hourly interval."""
        # 1 hour = 3600 seconds
        interval_seconds = 3600
        
        result = get_dagster_schedule_from_interval(interval_seconds)
        
        assert result == "0 * * * *"

    def test_get_dagster_schedule_from_interval_custom(self) -> None:
        """Test getting Dagster schedule from custom interval."""
        # 2 hours = 7200 seconds
        interval_seconds = 7200
        
        result = get_dagster_schedule_from_interval(interval_seconds)
        
        # Should return cron expression for custom interval
        assert result == "0 */6 * * *"  # 2 hours falls into 6-hour bucket

    def test_validate_external_dependencies(self) -> None:
        """Test external dependencies validation."""
        # Mock SQLMesh resource
        sqlmesh_resource = Mock()
        
        # Mock models with external dependencies
        model1 = Mock()
        model1.depends_on = {"external.model1", "internal.model1"}
        
        model2 = Mock()
        model2.depends_on = {"external.model2"}
        
        models = [model1, model2]
        
        # Mock context.get_model to return None for external models
        context = Mock()
        def get_model_side_effect(dep_str):
            return None if "external" in dep_str else Mock()
        
        context.get_model.side_effect = get_model_side_effect
        sqlmesh_resource.context = context
        sqlmesh_resource.translator = Mock()
        sqlmesh_resource.translator.get_external_dependencies.return_value = ["external.model1", "external.model2"]
        
        result = validate_external_dependencies(sqlmesh_resource, models)
        
        # Should return list of errors (empty if no validation errors)
        assert isinstance(result, list)

    def test_get_extra_keys(self) -> None:
        """Test getting extra keys."""
        result = get_extra_keys()
        
        assert isinstance(result, list)
        assert len(result) > 0
        # Should contain common SQLMesh model attributes
        assert "kind" in result
        assert "cron" in result

    def test_create_asset_checks(self) -> None:
        """Test creating asset checks."""
        # Mock SQLMesh resource
        sqlmesh_resource = Mock()
        model1 = Mock()
        model1.audits_with_args = []
        model2 = Mock()
        model2.audits_with_args = []
        sqlmesh_resource.get_models.return_value = [model1, model2]
        sqlmesh_resource.translator = Mock()
        sqlmesh_resource.translator.get_asset_key.return_value = Mock()
        
        result = create_asset_checks(sqlmesh_resource)
        
        assert isinstance(result, list)
        # Should return list of AssetCheckSpec objects


class TestAssetSpecs:
    """Test asset spec creation functions."""

    def test_create_all_asset_specs(self) -> None:
        """Test creating all asset specs."""
        # Mock models
        model1 = Mock()
        model1.catalog = "catalog1"
        model1.schema_name = "schema1"
        model1.view_name = "view1"
        model1.kind = "incremental"
        model1.tags = {"team:data"}
        
        model2 = Mock()
        model2.catalog = "catalog2"
        model2.schema_name = "schema2"
        model2.view_name = "view2"
        model2.kind = "full"
        model2.tags = {"environment:prod"}
        
        models = [model1, model2]
        
        # Mock SQLMesh resource
        sqlmesh_resource = Mock()
        
        # Mock translator
        translator = Mock()
        translator.get_asset_key.side_effect = lambda m: AssetKey([m.catalog, m.schema_name, m.view_name])
        translator.get_table_metadata.return_value = {
            "table_name": "test_table",
            "column_schema": Mock()
        }
        translator.serialize_metadata.return_value = {}
        translator.get_model_deps_with_external.return_value = []
        translator.get_group_name_with_fallback.return_value = "sqlmesh"
        translator.get_tags.return_value = {"team": "data"}
        sqlmesh_resource.translator = translator
        
        extra_keys = ["name", "kind"]
        kinds = {"incremental", "full"}
        owners = ["data-team"]
        group_name = "sqlmesh"
        
        result = create_all_asset_specs(
            models, sqlmesh_resource, extra_keys, kinds, owners, group_name
        )
        
        assert len(result) == 2
        assert all(isinstance(spec, AssetSpec) for spec in result)

    def test_create_asset_specs(self) -> None:
        """Test creating asset specs from SQLMesh resource."""
        # Mock SQLMesh resource
        sqlmesh_resource = Mock()
        sqlmesh_resource.get_models.return_value = [Mock(), Mock()]
        
        # Mock translator
        translator = Mock()
        translator.get_asset_key.return_value = AssetKey(["catalog", "schema", "view"])
        translator.get_table_metadata.return_value = {
            "table_name": "test_table",
            "column_schema": Mock()
        }
        translator.serialize_metadata.return_value = {}
        translator.get_model_deps_with_external.return_value = []
        translator.get_group_name_with_fallback.return_value = "sqlmesh"
        translator.get_tags.return_value = {"team": "data"}
        sqlmesh_resource.translator = translator
        
        extra_keys = ["name", "kind"]
        kinds = {"incremental", "full"}
        owners = ["data-team"]
        group_name = "sqlmesh"
        
        result = create_asset_specs(
            sqlmesh_resource, extra_keys, kinds, owners, group_name
        )
        
        assert len(result) == 2
        assert all(isinstance(spec, AssetSpec) for spec in result)


class TestAssetUtilsIntegration:
    """Integration tests for asset utilities with real SQLMesh context."""

    def test_get_models_to_materialize_with_real_context(self, sqlmesh_resource) -> None:
        """Test getting models to materialize with real SQLMesh resource."""
        from dg_sqlmesh import SQLMeshTranslator
        
        translator = SQLMeshTranslator()
        
        # Get real models from resource
        models = sqlmesh_resource.get_models()
        assert len(models) > 0
        
        # Test getting models to materialize
        result = get_models_to_materialize(None, lambda: models, translator)
        
        assert len(result) > 0
        assert all(not isinstance(model, ExternalModel) for model in result)

    def test_get_asset_kinds_with_real_context(self, sqlmesh_resource) -> None:
        """Test getting asset kinds with real SQLMesh resource."""
        result = get_asset_kinds(sqlmesh_resource)
        
        assert isinstance(result, set)
        assert len(result) > 0
        # Should contain SQLMesh and dialect
        assert "sqlmesh" in result

    def test_validate_external_dependencies_with_real_context(self, sqlmesh_resource) -> None:
        """Test external dependencies validation with real context."""
        models = sqlmesh_resource.get_models()
        assert len(models) > 0
        
        result = validate_external_dependencies(sqlmesh_resource, models)
        
        # Should return list of errors (empty if no external dependencies)
        assert isinstance(result, list) 