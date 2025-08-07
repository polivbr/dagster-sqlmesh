"""
Integration tests for the SQLMesh Dagster component.
"""

import shutil
import sys
import tempfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import pytest
from dagster import AssetKey, AssetSpec, BackfillPolicy
from dagster._core.definitions.backfill_policy import BackfillPolicyType
from dagster._core.definitions.metadata.source_code import (
    CodeReferencesMetadataValue,
    LocalFileCodeReference,
)
from dagster._core.test_utils import ensure_dagster_tests_import
from dagster._utils.env import environ
from dagster.components.core.component_tree import ComponentTree
from dagster.components.core.load_defs import build_component_defs
from dagster.components.resolved.core_models import AssetAttributesModel, OpSpec
from dagster.components.resolved.errors import ResolutionException
from dagster.components.testing import TestOpCustomization, TestTranslation

from dg_sqlmesh import SQLMeshProjectComponent
from dg_sqlmesh.components.sqlmesh_project.component import get_projects_from_sqlmesh_component

ensure_dagster_tests_import()
from dagster_tests.components_tests.integration_tests.component_loader import (
    load_test_component_defs,
)
from dagster_tests.components_tests.utils import (
    build_component_defs_for_test,
    create_project_from_components,
    load_component_for_test,
)

STUB_LOCATION_PATH = Path(__file__).parent / "code_locations" / "sqlmesh_project_location"
COMPONENT_RELPATH = "defs/sqlmesh_project"

# Expected asset keys from the test SQLMesh project
SQLMESH_PROJECT_KEYS = {
    AssetKey("stg_customers"),
    AssetKey("stg_orders"),
    AssetKey("stg_products"),
    AssetKey("stg_supplies"),
    AssetKey("customers"),
    AssetKey("orders"),
    AssetKey("products"),
    AssetKey("supplies"),
}


@pytest.fixture(scope="module")
def sqlmesh_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as temp_dir:
        shutil.copytree(STUB_LOCATION_PATH, temp_dir, dirs_exist_ok=True)
        # Create a basic SQLMesh project structure for testing
        sqlmesh_path = Path(temp_dir) / "defs/sqlmesh_project/sqlmesh_project"
        
        # Create config.yaml
        config_content = """# SQLMesh configuration
default_target: duckdb
default_sql_dialect: duckdb

model_defaults:
  dialect: duckdb
  materialized: table

targets:
  duckdb:
    type: duckdb
    path: ":memory:"
"""
        (sqlmesh_path / "config.yaml").write_text(config_content)

        # Create models directory
        models_dir = sqlmesh_path / "models"
        models_dir.mkdir(exist_ok=True)
        stg_dir = models_dir / "stg"
        stg_dir.mkdir(exist_ok=True)
        marts_dir = models_dir / "marts"
        marts_dir.mkdir(exist_ok=True)

        # Create sample models
        sample_models = {
            "stg/stg_customers.sql": """MODEL (
  name stg_customers,
  kind FULL,
  grain customer_id,
  tags ["dagster:group_name:staging", "staging"],
  audits(
    number_of_rows(threshold := 1),
    not_null(columns := (customer_id, name))
  )
);

SELECT 
  1 as customer_id,
  'Alice' as name
""",
            "stg/stg_orders.sql": """MODEL (
  name stg_orders,
  kind FULL,
  grain order_id,
  tags ["dagster:group_name:staging", "staging"],
  audits(
    number_of_rows(threshold := 1),
    not_null(columns := (order_id, customer_id))
  )
);

SELECT 
  1 as order_id,
  1 as customer_id
""",
            "marts/customers.sql": """MODEL (
  name customers,
  kind FULL,
  grain customer_id,
  tags ["dagster:group_name:marts", "marts"],
  audits(
    number_of_rows(threshold := 1),
    not_null(columns := (customer_id))
  )
);

SELECT 
  customer_id,
  name
FROM stg_customers
""",
            "marts/orders.sql": """MODEL (
  name orders,
  kind FULL,
  grain order_id,
  tags ["dagster:group_name:marts", "marts"],
  audits(
    number_of_rows(threshold := 1),
    not_null(columns := (order_id))
  )
);

SELECT 
  order_id,
  customer_id
FROM stg_orders
""",
        }

        for model_path, content in sample_models.items():
            full_path = models_dir / model_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(content)

        yield sqlmesh_path


class TestSQLMeshOpCustomization(TestOpCustomization):
    def test_translation(
        self, attributes: Mapping[str, Any], assertion: Callable[[OpSpec], bool], sqlmesh_path
    ) -> None:
        component = load_component_for_test(
            SQLMeshProjectComponent,
            {
                "project": str(sqlmesh_path),
                "op": attributes,
            },
        )
        op = component.op
        assert op
        assert assertion(op)


@pytest.mark.parametrize(
    "backfill_policy", [None, "single_run", "multi_run", "multi_run_with_max_partitions"]
)
def test_python_params(sqlmesh_path: Path, backfill_policy: Optional[str]) -> None:
    backfill_policy_arg = {}
    if backfill_policy == "single_run":
        backfill_policy_arg["backfill_policy"] = {"type": "single_run"}
    elif backfill_policy == "multi_run":
        backfill_policy_arg["backfill_policy"] = {"type": "multi_run"}
    elif backfill_policy == "multi_run_with_max_partitions":
        backfill_policy_arg["backfill_policy"] = {"type": "multi_run", "max_partitions_per_run": 3}

    defs = build_component_defs_for_test(
        SQLMeshProjectComponent,
        {
            "project": str(sqlmesh_path),
            "op": {
                "name": "some_op",
                "tags": {"tag1": "value"},
                **backfill_policy_arg,
            },
        },
    )
    
    # Check that assets are created
    asset_keys = defs.resolve_asset_graph().get_all_asset_keys()
    assert len(asset_keys) > 0
    
    # Check that we have at least one asset
    assets_def = defs.resolve_assets_def("stg_customers")
    assert assets_def.op.name == "some_op"
    assert assets_def.op.tags["tag1"] == "value"

    if backfill_policy is None:
        assert assets_def.backfill_policy is None
    elif backfill_policy == "single_run":
        assert isinstance(assets_def.backfill_policy, BackfillPolicy)
        assert assets_def.backfill_policy.policy_type == BackfillPolicyType.SINGLE_RUN
    elif backfill_policy == "multi_run":
        assert isinstance(assets_def.backfill_policy, BackfillPolicy)
        assert assets_def.backfill_policy.policy_type == BackfillPolicyType.MULTI_RUN
        assert assets_def.backfill_policy.max_partitions_per_run == 1
    elif backfill_policy == "multi_run_with_max_partitions":
        assert isinstance(assets_def.backfill_policy, BackfillPolicy)
        assert assets_def.backfill_policy.policy_type == BackfillPolicyType.MULTI_RUN
        assert assets_def.backfill_policy.max_partitions_per_run == 3


def test_load_from_path(sqlmesh_path: Path) -> None:
    with load_test_component_defs(sqlmesh_path.parent.parent.parent) as defs:
        asset_keys = defs.resolve_asset_graph().get_all_asset_keys()
        assert len(asset_keys) > 0

        for asset_node in defs.resolve_asset_graph().asset_nodes:
            # Check that assets have proper tags
            assert "sqlmesh" in asset_node.tags


def test_project_prepare_cli(sqlmesh_path: Path) -> None:
    src_path = sqlmesh_path.parent.parent.parent
    with create_project_from_components(str(src_path)) as res:
        p, _ = res
        
        projects = get_projects_from_sqlmesh_component(p)
        assert projects
        assert len(projects) > 0


def test_sqlmesh_subclass_additional_scope_fn(sqlmesh_path: Path) -> None:
    @dataclass
    class DebugSQLMeshProjectComponent(SQLMeshProjectComponent):
        @classmethod
        def get_additional_scope(cls) -> Mapping[str, Any]:
            return {
                "get_tags_for_model": lambda model: {
                    "model_id": str(getattr(model, "view_name", "")).replace("_", "-")
                }
            }

    defs = build_component_defs_for_test(
        DebugSQLMeshProjectComponent,
        {
            "project": str(sqlmesh_path),
            "translation": {"tags": "{{ get_tags_for_model(model) }}"},
        },
    )
    assets_def = defs.resolve_assets_def(AssetKey("stg_customers"))
    assert assets_def.get_asset_spec(AssetKey("stg_customers")).tags["model_id"] == "stg-customers"


class TestSQLMeshTranslation(TestTranslation):
    def test_translation(
        self,
        sqlmesh_path: Path,
        attributes: Mapping[str, Any],
        assertion: Callable[[AssetSpec], bool],
        key_modifier: Optional[Callable[[AssetKey], AssetKey]],
    ) -> None:
        defs = build_component_defs_for_test(
            SQLMeshProjectComponent,
            {
                "project": str(sqlmesh_path),
                "translation": attributes,
            },
        )
        key = AssetKey("stg_customers")

        if key_modifier:
            key = key_modifier(key)

        assets_def = defs.resolve_assets_def(key)
        assert assertion(assets_def.get_asset_spec(key))


def test_external_model_key_mapping(sqlmesh_path: Path) -> None:
    """Test external model key mapping with Jinja2 templates."""
    defs = build_component_defs_for_test(
        SQLMeshProjectComponent,
        {
            "project": str(sqlmesh_path),
            "external_model_key": "target/main/{{ node.name }}",
        },
    )
    
    # Check that the component was created with the external_model_key
    component = load_component_for_test(
        SQLMeshProjectComponent,
        {
            "project": str(sqlmesh_path),
            "external_model_key": "target/main/{{ node.name }}",
        },
    )
    
    assert component.external_model_key == "target/main/{{ node.name }}"
    
    # Test that the translator is a JinjaSQLMeshTranslator
    from dg_sqlmesh.components.sqlmesh_project.component import JinjaSQLMeshTranslator
    assert isinstance(component.translator, JinjaSQLMeshTranslator)


def test_spec_is_available_in_scope(sqlmesh_path: Path) -> None:
    defs = build_component_defs_for_test(
        SQLMeshProjectComponent,
        {
            "project": str(sqlmesh_path),
            "translation": {"metadata": {"asset_key": "{{ spec.key.path }}"}},
        },
    )
    assets_def = defs.resolve_assets_def(AssetKey("stg_customers"))
    assert assets_def.get_asset_spec(AssetKey("stg_customers")).metadata["asset_key"] == [
        "stg_customers"
    ]


def map_spec(spec: AssetSpec) -> AssetSpec:
    return spec.replace_attributes(tags={"is_custom_spec": "yes"})


def map_spec_to_attributes(spec: AssetSpec):
    return AssetAttributesModel(tags={"is_custom_spec": "yes"})


def map_spec_to_attributes_dict(spec: AssetSpec) -> dict[str, Any]:
    return {"tags": {"is_custom_spec": "yes"}}


@pytest.mark.parametrize("map_fn", [map_spec, map_spec_to_attributes, map_spec_to_attributes_dict])
def test_udf_map_spec(sqlmesh_path: Path, map_fn: Callable[[AssetSpec], Any]) -> None:
    @dataclass
    class DebugSQLMeshProjectComponent(SQLMeshProjectComponent):
        @classmethod
        def get_additional_scope(cls) -> Mapping[str, Any]:
            return {"map_spec": map_fn}

    defs = build_component_defs_for_test(
        DebugSQLMeshProjectComponent,
        {
            "project": str(sqlmesh_path),
            "translation": "{{ map_spec(spec) }}",
        },
    )
    assets_def = defs.resolve_assets_def(AssetKey("stg_customers"))
    assert assets_def.get_asset_spec(AssetKey("stg_customers")).tags["is_custom_spec"] == "yes"


def test_python_interface(sqlmesh_path: Path):
    context = ComponentTree.for_test().load_context
    assert SQLMeshProjectComponent(
        project=str(sqlmesh_path),
    ).build_defs(context)

    defs = SQLMeshProjectComponent(
        project=str(sqlmesh_path),
        translation=lambda spec, _: spec.replace_attributes(tags={"python": "rules"}),
    ).build_defs(context)
    assets_def = defs.resolve_assets_def(AssetKey("stg_customers"))
    assert assets_def.get_asset_spec(AssetKey("stg_customers")).tags["python"] == "rules"


def test_resolution(sqlmesh_path: Path):
    with environ({"SQLMESH_ENVIRONMENT": "prod"}):
        environment = """environment: "{{ env.SQLMESH_ENVIRONMENT }}" """
        c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
{environment}
        """)
    assert c.environment == "prod"


def test_project_root(sqlmesh_path: Path):
    # match to ensure {{ project_root }} is evaluated
    with pytest.raises(ResolutionException, match="project_dir /sqlmesh does not exist"):
        SQLMeshProjectComponent.resolve_from_yaml("""
project: "{{ project_root }}/sqlmesh"
        """)

    # match to ensure {{ project_root }} is evaluated
    with pytest.raises(ResolutionException, match="project_dir /sqlmesh does not exist"):
        SQLMeshProjectComponent.resolve_from_yaml("""
project:
  project_dir: "{{ project_root }}/sqlmesh"
        """)


def test_external_model_key_resolution(sqlmesh_path: Path):
    """Test that external_model_key is properly resolved."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
external_model_key: "target/main/{{ node.name }}"
        """)
    assert c.external_model_key == "target/main/{{ node.name }}"
    
    # Test with environment variable
    with environ({"EXTERNAL_KEY_TEMPLATE": "sling/{{ node.name }}"}):
        c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
external_model_key: "{{ env.EXTERNAL_KEY_TEMPLATE }}"
        """)
    assert c.external_model_key == "sling/{{ node.name }}"


def test_gateway_configuration(sqlmesh_path: Path):
    """Test gateway configuration."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
gateway: "duckdb"
        """)
    assert c.gateway == "duckdb"
    
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
gateway: "postgres"
        """)
    assert c.gateway == "postgres"


def test_environment_configuration(sqlmesh_path: Path):
    """Test environment configuration."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
environment: "dev"
        """)
    assert c.environment == "dev"
    
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
environment: "prod"
        """)
    assert c.environment == "prod"


def test_concurrency_limit_configuration(sqlmesh_path: Path):
    """Test concurrency limit configuration."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
concurrency_limit: 5
        """)
    assert c.concurrency_limit == 5


def test_name_configuration(sqlmesh_path: Path):
    """Test name configuration."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
name: "custom_sqlmesh_assets"
        """)
    assert c.name == "custom_sqlmesh_assets"


def test_group_name_configuration(sqlmesh_path: Path):
    """Test group name configuration."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
group_name: "custom_sqlmesh_group"
        """)
    assert c.group_name == "custom_sqlmesh_group"


def test_op_tags_configuration(sqlmesh_path: Path):
    """Test op tags configuration."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
op_tags:
  team: "data"
  env: "prod"
        """)
    assert c.op_tags["team"] == "data"
    assert c.op_tags["env"] == "prod"


def test_retry_policy_configuration(sqlmesh_path: Path):
    """Test retry policy configuration."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
retry_policy:
  max_retries: 3
  delay: 10.0
  backoff: "exponential"
        """)
    assert c.retry_policy.max_retries == 3
    assert c.retry_policy.delay == 10.0
    assert c.retry_policy.backoff == "exponential"


def test_schedule_name_configuration(sqlmesh_path: Path):
    """Test schedule name configuration."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
schedule_name: "custom_sqlmesh_schedule"
        """)
    assert c.schedule_name == "custom_sqlmesh_schedule"


def test_enable_schedule_configuration(sqlmesh_path: Path):
    """Test enable schedule configuration."""
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
enable_schedule: true
        """)
    assert c.enable_schedule is True
    
    c = SQLMeshProjectComponent.resolve_from_yaml(f"""
project: {sqlmesh_path!s}
enable_schedule: false
        """)
    assert c.enable_schedule is False
