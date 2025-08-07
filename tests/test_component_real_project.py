"""
Test the SQLMesh component with a real SQLMesh project.
"""

import pytest
from pathlib import Path
from dagster import AssetKey, Definitions
from dg_sqlmesh import SQLMeshProjectComponent


def test_component_with_real_project():
    """Test the component with the existing SQLMesh project in tests/sqlmesh_project."""
    
    # Use the existing SQLMesh project
    project_path = Path(__file__).parent / "sqlmesh_project"
    
    # Create component
    component = SQLMeshProjectComponent(
        project=str(project_path),
        gateway="duckdb",
        environment="dev",
        concurrency_limit=1,
        name="test_sqlmesh_assets",
        group_name="test_sqlmesh",
        enable_schedule=False,
    )
    
    # Test basic properties
    assert component.project == str(project_path)
    assert component.gateway == "duckdb"
    assert component.environment == "dev"
    assert component.concurrency_limit == 1
    assert component.name == "test_sqlmesh_assets"
    assert component.group_name == "test_sqlmesh"
    assert component.enable_schedule is False
    
    # Test that translator is created
    assert component.translator is not None
    from dg_sqlmesh import SQLMeshTranslator
    assert isinstance(component.translator, SQLMeshTranslator)
    
    # Test that resource is created
    assert component.sqlmesh_resource is not None
    from dg_sqlmesh import SQLMeshResource
    assert isinstance(component.sqlmesh_resource, SQLMeshResource)
    assert component.sqlmesh_resource.project_dir == str(project_path)


def test_component_with_external_model_key():
    """Test the component with external_model_key configuration."""
    
    project_path = Path(__file__).parent / "sqlmesh_project"
    
    component = SQLMeshProjectComponent(
        project=str(project_path),
        gateway="duckdb",
        environment="dev",
        external_model_key="target/main/{{ node.name }}",
        enable_schedule=False,
    )
    
    # Test that external_model_key is set
    assert component.external_model_key == "target/main/{{ node.name }}"
    
    # Test that the translator is a JinjaSQLMeshTranslator
    from dg_sqlmesh.components.sqlmesh_project.component import JinjaSQLMeshTranslator
    assert isinstance(component.translator, JinjaSQLMeshTranslator)
    
    # Test the translator with a sample external FQN
    translator = component.translator
    asset_key = translator.get_external_asset_key('"jaffle_db"."main"."raw_source_customers"')
    assert asset_key == AssetKey(["target", "main", "raw_source_customers"])


def test_component_build_defs():
    """Test that the component can build definitions."""
    
    project_path = Path(__file__).parent / "sqlmesh_project"
    
    component = SQLMeshProjectComponent(
        project=str(project_path),
        gateway="duckdb",
        environment="dev",
        enable_schedule=False,
    )
    
    # Mock the ComponentLoadContext
    class MockContext:
        pass
    
    context = MockContext()
    
    # This should not raise an exception
    defs = component.build_defs(context)
    assert isinstance(defs, Definitions)
    
    # Check that we have some assets
    assert len(defs.assets) > 0


def test_component_with_retry_policy():
    """Test component with retry policy configuration."""
    
    from dagster import RetryPolicy, Backoff
    
    project_path = Path(__file__).parent / "sqlmesh_project"
    
    component = SQLMeshProjectComponent(
        project=str(project_path),
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


def test_component_with_op_tags():
    """Test component with op tags configuration."""
    
    project_path = Path(__file__).parent / "sqlmesh_project"
    
    component = SQLMeshProjectComponent(
        project=str(project_path),
        gateway="duckdb",
        environment="dev",
        op_tags={"team": "data", "env": "test"},
        enable_schedule=False,
    )
    
    assert component.op_tags is not None
    assert component.op_tags["team"] == "data"
    assert component.op_tags["env"] == "test"


def test_jinja_translator_various_formats():
    """Test the JinjaSQLMeshTranslator with various FQN formats."""
    
    from dg_sqlmesh.components.sqlmesh_project.component import JinjaSQLMeshTranslator
    
    # Test with quoted FQN
    translator = JinjaSQLMeshTranslator("target/main/{{ node.name }}")
    asset_key = translator.get_external_asset_key('"jaffle_db"."main"."raw_source_customers"')
    assert asset_key == AssetKey(["target", "main", "raw_source_customers"])
    
    # Test with unquoted FQN
    asset_key = translator.get_external_asset_key("jaffle_db.main.raw_source_customers")
    assert asset_key == AssetKey(["target", "main", "raw_source_customers"])
    
    # Test with custom template
    translator = JinjaSQLMeshTranslator("sling/{{ node.database }}/{{ node.name }}")
    asset_key = translator.get_external_asset_key('"jaffle_db"."main"."raw_source_customers"')
    assert asset_key == AssetKey(["sling", "jaffle_db", "raw_source_customers"])
    
    # Test with conditional template
    translator = JinjaSQLMeshTranslator(
        "{% if node.database == 'jaffle_db' %}target/main/{{ node.name }}{% else %}{{ node.database }}/{{ node.schema }}/{{ node.name }}{% endif %}"
    )
    asset_key = translator.get_external_asset_key('"jaffle_db"."main"."raw_source_customers"')
    assert asset_key == AssetKey(["target", "main", "raw_source_customers"])
    
    # Test with different database
    asset_key = translator.get_external_asset_key('"other_db"."main"."raw_source_customers"')
    assert asset_key == AssetKey(["other_db", "main", "raw_source_customers"])


def test_jinja_translator_fallback():
    """Test the JinjaSQLMeshTranslator fallback behavior."""
    
    from dg_sqlmesh.components.sqlmesh_project.component import JinjaSQLMeshTranslator
    
    translator = JinjaSQLMeshTranslator("target/main/{{ node.name }}")
    
    # Test with malformed FQN - should fallback to parent implementation
    asset_key = translator.get_external_asset_key("invalid_fqn")
    # Should return some fallback asset key
    assert isinstance(asset_key, AssetKey)
    
    # Test with empty FQN
    asset_key = translator.get_external_asset_key("")
    assert isinstance(asset_key, AssetKey)


if __name__ == "__main__":
    pytest.main([__file__])
