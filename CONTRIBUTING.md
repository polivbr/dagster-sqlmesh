# Contributing to dg-sqlmesh

Thank you for your interest in contributing to dg-sqlmesh! This guide will help you get started.

## 🚀 Quick Start

### Prerequisites

- Python 3.11 or 3.12
- [uv](https://github.com/astral-sh/uv) package manager
- Git

### Development Setup

1. **Fork and clone the repository**

   ```bash
   git clone https://github.com/yourusername/dagster-sqlmesh.git
   cd dagster-sqlmesh
   ```

2. **Install dependencies**

   ```bash
   uv sync --group dev
   ```

3. **Load test data**

   ```bash
   uv run --group dev python tests/load_jaffle_data.py
   ```

4. **Run tests to verify setup**
   ```bash
   make test
   ```

## 🛠️ Development Workflow

### Before Making Changes

1. **Create a feature branch**

   ```bash
   git checkout -b feat/your-feature-name
   ```

2. **Run pre-commit checks**
   ```bash
   make pre-commit
   ```

### During Development

1. **Run tests frequently**

   ```bash
   make test          # Run all tests
   make test-unit     # Run unit tests only
   make coverage      # Run tests with coverage
   ```

2. **Lint your code**

   ```bash
   make lint          # Run ruff linting and formatting
   make vulture       # Check for dead code
   ```

3. **Follow code standards**
   - All code, comments, and documentation must be in **English**
   - Use type hints for all functions
   - Add docstrings for public APIs
   - Follow the existing code patterns

### Testing

We use pytest with comprehensive test coverage:

- **Unit tests**: `tests/unit/` - Test individual components
- **Integration tests**: `tests/integration/` - Test full workflows
- **Test project**: `tests/fixtures/sqlmesh_project/` - SQLMesh test project

#### Running Specific Tests

```bash
# Run specific test file
uv run pytest tests/unit/test_factory.py -v

# Run specific test class
uv run pytest tests/unit/test_factory.py::TestSQLMeshAssetsFactory -v

# Run with markers
uv run pytest tests/ -m unit -v
uv run pytest tests/ -m integration -v
```

#### Adding Tests

- **Unit tests**: Test individual functions/classes in isolation
- **Integration tests**: Test complete workflows
- **Use fixtures**: Leverage existing fixtures in `tests/conftest.py`
- **Mock external services**: Use mocks for external dependencies

### Code Style

We use `ruff` for linting and formatting:

```bash
# Check formatting and linting
make lint

# Auto-fix issues
uv run ruff check --fix src/dg_sqlmesh/
uv run ruff format src/dg_sqlmesh/
```

## 📝 Pull Request Process

### 1. Prepare Your PR

- [ ] All tests pass: `make test`
- [ ] Code is properly formatted: `make lint`
- [ ] No dead code: `make vulture`
- [ ] Documentation updated (if applicable)
- [ ] GitHub Release notes updated (for notable changes)

### 2. Create Pull Request

1. **Push your branch**

   ```bash
   git push origin feat/your-feature-name
   ```

2. **Create PR on GitHub**
   - Use the provided PR template
   - Link related issues
   - Describe your changes clearly
   - Mark as draft if work in progress

### 3. Review Process

- **Automated checks**: CI/CD will run automatically
- **Manual review**: Maintainers will review your code
- **Address feedback**: Make requested changes
- **Final approval**: PR will be merged after approval

## 🏗️ Architecture

### Key Principles

1. **Individual Asset Pattern**: Each SQLMesh model becomes a separate Dagster asset
2. **Shared Execution**: Single SQLMesh execution per Dagster run
3. **Event-Driven Status**: Asset status derived from SQLMesh events
4. **Resource Management**: SQLMeshResultsResource for shared state

### Code Organization

```
src/dg_sqlmesh/
├── __init__.py                    # Public API
├── factory.py                     # Main factory functions
├── resource.py                    # SQLMeshResource implementation
├── translator.py                  # SQLMesh ↔ Dagster mapping
├── sqlmesh_asset_utils.py         # Asset creation utilities
├── sqlmesh_asset_execution_utils.py # Execution utilities
├── sqlmesh_asset_check_utils.py   # Asset check utilities
└── components/                    # Dagster component
```

### Adding New Features

1. **Follow existing patterns**: Look at similar implementations
2. **Add tests**: Both unit and integration tests
3. **Update documentation**: README, docstrings, examples
4. **Consider backward compatibility**: Avoid breaking changes
5. **Add ADR**: For significant architectural decisions

## 🐛 Bug Reports

### Before Reporting

1. **Search existing issues**: Avoid duplicates
2. **Use latest version**: Ensure you're using the latest release
3. **Minimal reproduction**: Create a minimal example

### Reporting Process

1. **Use issue template**: Follow the bug report template
2. **Provide context**: Environment, versions, configuration
3. **Include logs**: Relevant error messages and tracebacks
4. **Minimal example**: Code that reproduces the issue

## ✨ Feature Requests

### Before Requesting

1. **Check roadmap**: Review existing plans in README
2. **Search issues**: See if already requested
3. **Consider alternatives**: Explore existing workarounds

### Request Process

1. **Use template**: Follow the feature request template
2. **Explain problem**: What limitation are you facing?
3. **Propose solution**: How would you like it to work?
4. **Provide examples**: Show usage examples

## 🔒 Security

### Reporting Security Issues

**Do not** report security vulnerabilities through public GitHub issues.

Instead:

1. Email: thomastrividic@gmail.com
2. Include: Detailed description and reproduction steps
3. Response: We'll respond within 48 hours

### Security Best Practices

- Keep dependencies updated
- Use secrets properly (no hardcoded credentials)
- Follow secure coding practices
- Run security scans: `make security`

## 📚 Documentation

### Types of Documentation

1. **Code documentation**: Docstrings and comments
2. **User documentation**: README, examples
3. **Developer documentation**: ADRs, CONTRIBUTING
4. **API documentation**: Auto-generated from docstrings

### Writing Guidelines

- **Clear and concise**: Easy to understand
- **Examples included**: Show how to use features
- **Keep updated**: Maintain accuracy with code changes
- **English only**: All documentation in English

## 🏷️ Release Process

### For Maintainers

1. **Version bump**:

   ```bash
   make bump-patch  # or bump-minor, bump-major
   ```

2. **Create release**:

   ```bash
   ./scripts/release.sh patch  # Interactive release
   ```

3. **Automated process**:
   - CI/CD tests the release
   - Publishes to PyPI
   - Creates GitHub release

### Version Schema

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

## 🎯 Getting Help

### Community Support

- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: Questions and community support
- **Documentation**: README and examples

### Maintainer Contact

- **GitHub**: @fosk06
- **Email**: thomastrividic@gmail.com

## 📜 License

By contributing to dg-sqlmesh, you agree that your contributions will be licensed under the Apache-2.0 License.

---

Thank you for contributing to dg-sqlmesh! 🙏
