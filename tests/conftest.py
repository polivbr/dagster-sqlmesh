import os
import subprocess
import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from dg_sqlmesh import SQLMeshResource
from sqlmesh import Context

# Suppress Pydantic deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="pydantic")
warnings.filterwarnings("ignore", message=".*json_encoders.*", category=DeprecationWarning)
warnings.filterwarnings("ignore", message=".*class-based `config`.*", category=DeprecationWarning)

# Register custom marks to avoid warnings
pytest_plugins = []

def pytest_configure(config):
    """Configure pytest to register custom marks and suppress warnings."""
    config.addinivalue_line(
        "markers", "core: mark test as core functionality test"
    )

@pytest.hookimpl(hookwrapper=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> Iterator[None]:
    """Mark tests in the `integration` directories. Mark other tests as `core`."""
    for item in items:
        if "integration" in item.path.parts:
            item.add_marker(pytest.mark.integration)
        else:
            item.add_marker(pytest.mark.core)

    yield


@pytest.fixture(scope="session", autouse=True)
def setup_duckdb_dbfile_path_fixture() -> None:
    """Set environment variables to generate a unique duckdb dbfile path for each pytest-xdist worker."""
    # Use a simple unique identifier for now
    import uuid
    unique_id = str(uuid.uuid4())[:8]
    sqlmesh_duckdb_db_file_name = f"test_{unique_id}_jaffle"
    sqlmesh_duckdb_dbfile_path = f"tests/fixtures/sqlmesh_project/{sqlmesh_duckdb_db_file_name}.db"

    os.environ["SQLMESH_PYTEST_XDIST_DUCKDB_DBFILE_NAME"] = sqlmesh_duckdb_db_file_name
    os.environ["SQLMESH_PYTEST_XDIST_DUCKDB_DBFILE_PATH"] = sqlmesh_duckdb_dbfile_path


@pytest.fixture(scope="session", autouse=True)
def disable_openblas_threading_affinity_fixture() -> None:
    """Disable OpenBLAS and GotoBLAS threading affinity to prevent test failures."""
    os.environ["OPENBLAS_MAIN_FREE"] = "1"
    os.environ["GOTOBLAS_MAIN_FREE"] = "1"


@pytest.fixture(scope="session", autouse=True)
def ensure_sqlmesh_dev_environment(sqlmesh_project_path: Path) -> None:
    """Ensure the 'dev' environment exists and is invalidated for testing."""
    # First, load the test data
    subprocess.run(
        [
            "uv", "run", "--group", "dev",
            "python", "tests/load_jaffle_data.py"
        ],
        check=True,
        capture_output=True,
    )
    
    try:
        # Then, ensure the dev environment exists
            subprocess.run(
                [
                    "uv", "run", "--group", "dev",
                    "sqlmesh", "-p", ".", "plan", "dev", "--no-prompts"
                ],
                check=True,
                capture_output=True,
                input=b"y\n",  # Auto-respond "yes" to prompts
                cwd=str(sqlmesh_project_path),  # Run from the project directory
            )
            
            # Then invalidate the dev environment to force recomputation
            subprocess.run(
                [
                    "uv", "run", "--group", "dev",
                    "sqlmesh", "-p", ".", "invalidate", "dev"
                ],
                check=True,
                capture_output=True,
                cwd=str(sqlmesh_project_path),  # Run from the project directory
            )
    except subprocess.CalledProcessError:
        # If the environment doesn't exist yet, create it
        subprocess.run(
            [
                "uv", "run", "--group", "dev",
                "sqlmesh", "-p", ".", "plan", "dev", "--no-prompts"
            ],
            check=True,
            input=b"y\n",  # Auto-respond "yes" to prompts
            cwd=str(sqlmesh_project_path),  # Run from the project directory
        )


def _create_sqlmesh_context(
    project_dir: Path, load_test_data: bool = True
) -> Context:
    """Create a SQLMesh context for testing."""
    context = Context(str(project_dir))
    
    if load_test_data:
        # Load test data if not already loaded
        try:
            context.load()
        except Exception:
            # If loading fails, try to load test data
            subprocess.run(
                ["uv", "run", "--group", "dev", "python", "tests/load_jaffle_data.py"],
                check=True,
                cwd=project_dir.parent,
            )
            context.load()
    
    return context


@pytest.fixture(name="sqlmesh_project_path", scope="session")
def sqlmesh_project_path_fixture() -> Path:
    """Get the path to the SQLMesh test project."""
    return Path("tests/fixtures/sqlmesh_project")


@pytest.fixture(name="sqlmesh_context", scope="session")
def sqlmesh_context_fixture(sqlmesh_project_path: Path) -> Context:
    """Create a SQLMesh context for testing."""
    return _create_sqlmesh_context(sqlmesh_project_path)


@pytest.fixture(name="sqlmesh_resource", scope="session")
def sqlmesh_resource_fixture(sqlmesh_project_path: Path) -> SQLMeshResource:
    """Create a SQLMeshResource for testing."""
    return SQLMeshResource(project_dir=str(sqlmesh_project_path), gateway="duckdb", environment="dev")


@pytest.fixture(name="sqlmesh_models", scope="session")
def sqlmesh_models_fixture(sqlmesh_context: Context) -> dict[str, Any]:
    """Get SQLMesh models for testing."""
    # Get all models from the context
    models = {}
    for model_name in sqlmesh_context.models:
        model = sqlmesh_context.get_model(model_name)
        models[model_name] = {
            "name": model.name,
            "kind": model.kind,
            "cron": getattr(model, "cron", None),
            "columns": model.columns,
            "dependencies": list(model.dependencies),
        }
    return models


@pytest.fixture(name="sqlmesh_external_models", scope="session")
def sqlmesh_external_models_fixture(sqlmesh_context: Context) -> dict[str, Any]:
    """Get SQLMesh external models for testing."""
    # Get external models from external_models.yaml
    external_models_path = sqlmesh_context.path / "external_models.yaml"
    if external_models_path.exists():
        import yaml
        with open(external_models_path) as f:
            return yaml.safe_load(f)
    return {}


@pytest.fixture(name="sqlmesh_audits", scope="session")
def sqlmesh_audits_fixture(sqlmesh_context: Context) -> dict[str, Any]:
    """Get SQLMesh audits for testing."""
    audits = {}
    audits_path = sqlmesh_context.path / "audits"
    if audits_path.exists():
        for audit_file in audits_path.glob("*.sql"):
            with open(audit_file) as f:
                audits[audit_file.stem] = f.read()
    return audits 