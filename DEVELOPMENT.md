# Development Guide

This guide explains how to develop, test, and publish the `dg-sqlmesh` package.

## Makefile Commands

The project includes a Makefile to automate common tasks.

```bash
# Show help
make help

# Show current version
make check-version

# Package info
make info

# Clean build artifacts
make clean

# Build the package
make build

# Install in development mode
make install-dev

# Dead code detection
make vulture
```

## Test SQLMesh Project (DuckDB)

The repository contains a complete SQLMesh test project in `tests/sqlmesh_project/` to validate the integration.

### Prerequisites

```bash
# Install development dependencies (includes SQLMesh and DuckDB)
uv sync --group dev
```

### Database Setup

```bash
# Load test data into DuckDB
uv run --group dev python tests/load_jaffle_data.py
```

### Validate SQLMesh Plan

```bash
uv run --group dev sqlmesh -p tests/sqlmesh_project plan --no-prompts
```

### Example: Create Definitions for the Test Project

```python
from dg_sqlmesh import sqlmesh_definitions_factory

defs = sqlmesh_definitions_factory(
    project_dir="tests/sqlmesh_project",
    gateway="duckdb",
)
```

## Package Structure

```
dg-sqlmesh/
├── src/dg_sqlmesh/
│   ├── __init__.py                  # Public API entrypoint
│   ├── factory.py                   # Factories (assets/definitions/schedule)
│   ├── resource.py                  # SQLMeshResource
│   ├── translator.py                # SQLMeshTranslator
│   ├── sqlmesh_asset_utils.py       # Asset utilities
│   ├── sqlmesh_asset_check_utils.py # Audit/check utilities
│   ├── sqlmesh_asset_execution_utils.py # Execution helpers
│   └── notifier_service.py          # Notifier singleton + context registration
├── tests/                           # Unit/integration tests
├── examples/                        # Usage examples
├── pyproject.toml                   # Package config
├── Makefile                         # Dev commands
└── README.md                        # Main documentation
```

## Public API

The package exposes the following symbols:

```python
from dg_sqlmesh import (
    sqlmesh_definitions_factory,
    sqlmesh_assets_factory,
    sqlmesh_adaptive_schedule_factory,
    SQLMeshResource,
    SQLMeshTranslator,
)
```

## Typical Development Workflow

1) Local setup

```bash
make install-dev
```

2) Iterate

```bash
make test
make vulture
```

3) Release preparation

```bash
make clean
make build
make validate
```

4) Publish

```bash
make bump-patch  # or bump-minor / bump-major
make publish
```

## Pre-Publish Checklist

- [ ] Tests passing: `make test`
- [ ] Build success: `make build`
- [ ] Dead code verified: `make vulture`
- [ ] Version updated: `make check-version`
- [ ] Documentation up to date
- [ ] PyPI token configured: `export UV_PUBLISH_TOKEN=...`
- [ ] Full validation: `make validate`

## Troubleshooting

### Missing PyPI token

```bash
Error: UV_PUBLISH_TOKEN environment variable not set
```

Solution: `export UV_PUBLISH_TOKEN=your_token`

### Build error

```bash
make clean && make build
```

### Version mismatch

```bash
make check-version
# Ensure pyproject.toml and __init__.py are in sync
```

### Dead code reported by vulture

This is informational and does not fail the build:

```bash
make vulture
```

## Architecture Notes (Integration specifics)

- Individual assets: each SQLMesh model is surfaced as a Dagster asset.
- Shared execution: a single SQLMesh run per Dagster run (shared via SQLMeshResultsResource).
- Notifier-based audits: audit failures captured via `notifier_service` (no legacy console).
- No retries: enforced via Dagster tags (see ADR-0004).


