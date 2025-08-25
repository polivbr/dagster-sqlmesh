# Contributing to dg-sqlmesh

We love your input! We want to make contributing to `dg-sqlmesh` as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## Development Process

We use GitHub to host code, to track issues and feature requests, as well as accept pull requests.

### Pull Requests

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

## Development Setup

### Prerequisites

- Python 3.11+
- uv package manager
- SQLMesh 0.209.0+
- Dagster 1.11.4+

### Local Development

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/dagster-sqlmesh.git
cd dagster-sqlmesh

# Install dependencies
uv sync

# Run tests
make test

# Run linting
make lint

# Format code
make format
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run unit tests only
uv run pytest tests/unit/

# Run integration tests only
uv run pytest tests/integration/

# Run with coverage
uv run pytest --cov=src/dg_sqlmesh --cov-report=html
```

## Code Style

We use several tools to maintain code quality:

- **Ruff**: For linting and formatting
- **Vulture**: For detecting dead code
- **pytest**: For testing

Run the full quality check:

```bash
make validate  # Runs: clean, build, test, ruff, vulture
```

### Code Standards

- Follow PEP 8 style guidelines
- Use type hints where applicable
- Write docstrings for public functions and classes
- Keep functions focused and small
- Use meaningful variable and function names

### Commit Message Format

We use conventional commits:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests or correcting existing tests
- `chore`: Changes to the build process or auxiliary tools

Examples:
```
feat(resource): add support for custom SQLMesh environments
fix(factory): resolve downstream blocking issue with stale notifier state
docs(readme): update installation instructions for Python 3.12
```

## Architecture Decision Records (ADRs)

For significant architectural changes, please:

1. Review existing ADRs in `/adr/`
2. Create a new ADR following the template
3. Get feedback from maintainers before implementation

## Testing Guidelines

### Test Structure

- **Unit tests**: Test individual functions and classes in isolation
- **Integration tests**: Test SQLMesh-Dagster integration end-to-end
- **Fixtures**: Shared test data in `tests/fixtures/`

### Test Naming

```python
def test_function_name_expected_behavior():
    """Test that function_name does expected_behavior when given specific input."""
```

### Test Coverage

Aim for:
- 90%+ coverage for new code
- All public APIs must be tested
- Critical paths must have integration tests

## Release Process

We follow semantic versioning (SemVer):

- **Patch** (1.8.0 → 1.8.1): Bug fixes, documentation updates
- **Minor** (1.8.0 → 1.9.0): New features, backwards compatible
- **Major** (1.8.0 → 2.0.0): Breaking changes

### Release Commands

```bash
# Patch release
make release-patch

# Minor release  
make release-minor

# Major release
make release-major
```

## Issue Reporting

### Bug Reports

When filing a bug report, please include:

- **Environment**: Python version, Dagster version, SQLMesh version
- **Steps to reproduce**: Minimal example that reproduces the issue
- **Expected behavior**: What you expected to happen
- **Actual behavior**: What actually happened
- **Error messages**: Full stack traces if applicable

### Feature Requests

For feature requests, please include:

- **Use case**: Why is this feature needed?
- **Proposed solution**: How should it work?
- **Alternatives**: What alternatives have you considered?
- **Breaking changes**: Would this introduce breaking changes?

## Documentation

- Update README.md for user-facing changes
- Update ADRs for architectural decisions
- Add docstrings for new public APIs
- Update examples in `/examples/` if relevant

## Community

- Be respectful and inclusive
- Help others learn and contribute
- Follow our [Code of Conduct](CODE_OF_CONDUCT.md)
- Ask questions in GitHub Discussions or Issues

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.

## Questions?

Don't hesitate to ask! Create an issue or start a discussion. We're here to help.

---

Thank you for contributing to dg-sqlmesh! 🚀
