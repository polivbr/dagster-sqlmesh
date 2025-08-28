# dg-sqlmesh

[![PyPI version](https://badge.fury.io/py/dg-sqlmesh.svg)](https://badge.fury.io/py/dg-sqlmesh)
[![Current Version](https://img.shields.io/badge/version-1.9.1-blue.svg)](https://github.com/fosk06/dagster-sqlmesh/releases)

A Dagster integration for SQLMesh that provides seamless orchestration of SQLMesh models, schedules, and assets within Dagster workflows.

## 🚀 Quick Start

```bash
# Install the package
pip install dg-sqlmesh

# Or install from source
pip install -e .
```

## 📚 Documentation

**📖 [Full Documentation →](https://fosk06.github.io/dagster-sqlmesh/)**

Our comprehensive documentation includes:

- **Getting Started** - Installation and setup guides
- **User Guide** - Core concepts and architecture
- **Examples** - Practical usage patterns
- **Development** - Contributing guidelines

## 🎯 Key Features

- **SQLMesh Integration** - Native support for SQLMesh models and schedules
- **Asset Management** - Automatic asset creation from SQLMesh models
- **Scheduling** - Adaptive scheduling with Dagster's scheduling system
- **Audit Integration** - Built-in audit checks and validation
- **Environment Management** - Multi-environment support

## 🏗️ Architecture

dg-sqlmesh provides a clean abstraction layer between Dagster and SQLMesh:

- **SQLMeshResource** - Manages SQLMesh context and execution
- **SQLMeshTranslator** - Converts SQLMesh concepts to Dagster assets
- **Factory Functions** - Easy setup and configuration

## 🔧 Installation

### From PyPI

```bash
pip install dg-sqlmesh
```

### From Source

```bash
git clone https://github.com/fosk06/dagster-sqlmesh.git
cd dagster-sqlmesh
pip install -e .
```

## 📖 Examples

See our [examples directory](examples/) and [documentation](https://fosk06.github.io/dagster-sqlmesh/) for comprehensive usage examples.

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://fosk06.github.io/dagster-sqlmesh/development/contributing/) for details.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- **Documentation**: [https://fosk06.github.io/dagster-sqlmesh/](https://fosk06.github.io/dagster-sqlmesh/)
- **GitHub**: [https://github.com/fosk06/dagster-sqlmesh](https://github.com/fosk06/dagster-sqlmesh)
- **PyPI**: [https://pypi.org/project/dg-sqlmesh/](https://pypi.org/project/dg-sqlmesh/)
- **Releases**: [https://github.com/fosk06/dagster-sqlmesh/releases](https://github.com/fosk06/dagster-sqlmesh/releases)
