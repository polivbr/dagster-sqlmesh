# Welcome to dg-sqlmesh Documentation

<div class="grid cards" markdown>

- :fontawesome-solid-rocket: **[Quick Start](getting-started/quick-start.md)**

  Get up and running with dg-sqlmesh in minutes. Learn the basics and create your first SQLMesh-Dagster integration.

- :fontawesome-solid-book: **[User Guide](user-guide/core-concepts.md)**

  Comprehensive guide covering all aspects of SQLMesh integration with Dagster, from basic concepts to advanced features.

- :fontawesome-solid-code: **[API Reference](api/factory-functions.md)**

  Complete API documentation for all functions, classes, and methods in the dg-sqlmesh package.

- :fontawesome-solid-lightbulb: **[Examples](examples/basic-usage.md)**

  Practical examples and code samples showing how to use dg-sqlmesh in real-world scenarios.

</div>

## What is dg-sqlmesh?

**dg-sqlmesh** is a powerful Python package that provides seamless integration between [SQLMesh](https://sqlmesh.com/) and [Dagster](https://dagster.io/), enabling you to orchestrate SQLMesh models as Dagster assets with full support for audits, metadata, and adaptive scheduling.

## 📚 Documentation

**📖 [Full Documentation →](https://fosk06.github.io/dagster-sqlmesh/)**

Our comprehensive documentation includes:

- **Getting Started** - Installation and setup guides
- **User Guide** - Core concepts and architecture
- **Examples** - Practical usage patterns
- **Development** - Contributing guidelines

### 🎯 Key Features

- **Individual Asset Control** : Each SQLMesh model becomes a separate Dagster asset
- **Automatic Materialization** : SQLMesh models are automatically converted to Dagster assets
- **Audit Integration** : SQLMesh audits become Dagster AssetCheckSpec with proper result handling
- **Adaptive Scheduling** : Intelligent schedule creation based on SQLMesh cron analysis
- **External Asset Mapping** : Support for external sources with Jinja2 templating
- **Component System** : Declarative YAML configuration for easy integration

### 🚀 Why Choose dg-sqlmesh?

<div class="grid" markdown>

- **Seamless Integration**

  Bridge the gap between SQLMesh's powerful data modeling and Dagster's orchestration capabilities.

- **Production Ready**

  Built with enterprise-grade features including concurrency control, error handling, and monitoring.

- **Developer Friendly**

  Simple factory functions, comprehensive examples, and clear documentation.

- **Extensible**

  Custom translators, external asset mapping, and flexible configuration options.

</div>

## Quick Installation

```bash
pip install dg-sqlmesh
```

## Basic Usage

```python
from dg_sqlmesh import sqlmesh_definitions_factory

# All-in-one factory with external asset mapping!
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",
    external_asset_mapping="target/main/{node.name}",
    concurrency_limit=1,
    group_name="sqlmesh",
    enable_schedule=True,
)
```

## Current Version

<div class="version-info" markdown>

**Latest Release**: [v1.9.2](https://github.com/fosk06/dagster-sqlmesh/releases/tag/v1.9.2)

**Features**: Complete documentation system, GitHub Pages infrastructure, and improved maintainability

</div>

## Get Started

Ready to begin? Choose your path:

- **[Installation Guide](getting-started/installation.md)** - Set up dg-sqlmesh in your environment
- **[Quick Start Tutorial](getting-started/quick-start.md)** - Build your first integration in minutes
- **[User Guide](user-guide/core-concepts.md)** - Learn about core concepts and architecture

## Community & Support

- **GitHub Repository**: [fosk06/dagster-sqlmesh](https://github.com/fosk06/dagster-sqlmesh)
- **PyPI Package**: [dg-sqlmesh](https://pypi.org/project/dg-sqlmesh/)
- **Issues & Discussions**: [GitHub Issues](https://github.com/fosk06/dagster-sqlmesh/issues)
- **Contributing**: [Development Guide](development/contributing.md)

---

<div class="admonition tip" markdown>

**Pro Tip**: Check out our [examples](examples/basic-usage.md) for ready-to-use code snippets and comprehensive documentation.

</div>
