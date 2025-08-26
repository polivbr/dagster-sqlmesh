st# CI/CD Infrastructure

This directory contains the CI/CD infrastructure for the dg-sqlmesh project using GitHub Actions.

## 🚀 Workflows

### 1. **Test & Quality** (`test.yml`)

**Triggers**: Push to `main`, Pull Requests

**Jobs**:

- **Test Matrix**: Python 3.11, 3.12 on Ubuntu
- **Linting**: ruff check + format validation
- **Dead Code**: vulture analysis
- **Tests**: pytest with coverage reporting
- **Build**: Package build validation
- **Security**: Basic security audit

**Features**:

- ✅ Parallel execution across Python versions
- ✅ Coverage reporting to Codecov
- ✅ Artifact upload for build validation
- ✅ Version consistency checks

### 2. **Release** (`release.yml`)

**Triggers**:

- Tags matching `v*.*.*` (e.g., `v1.8.2`)
- Manual workflow dispatch

**Jobs**:

- **Validation**: Version consistency checks
- **Testing**: Full test suite before release
- **Build**: Clean package build
- **PyPI**: Automatic publication to PyPI
- **GitHub Release**: Changelog generation and release creation

**Features**:

- ✅ Automated version validation
- ✅ Pre-release detection (alpha/beta/rc)
- ✅ Changelog generation from commits
- ✅ PyPI trusted publishing
- ✅ GitHub release with artifacts

### 3. **Security** (`security.yml`)

**Triggers**:

- Weekly schedule (Mondays 9 AM UTC)
- Dependency file changes
- Manual dispatch

**Jobs**:

- **Dependency Scan**: pip-audit + safety checks
- **CodeQL**: Static analysis for security vulnerabilities
- **License Check**: License compliance verification
- **Secrets**: TruffleHog secret detection

## 🔧 Configuration Files

### **Dependabot** (`dependabot.yml`)

- Weekly dependency updates
- Separate tracking for Python deps and GitHub Actions
- Auto-labeling and reviewer assignment

### **Issue Templates**

- `bug_report.yml`: Structured bug reporting
- `feature_request.yml`: Feature request template

### **Pull Request Template**

- Comprehensive checklist for contributors
- Testing and documentation requirements
- Breaking change assessment

## 🎯 Release Process

### Automated Release (Recommended)

1. **Prepare release**:

   ```bash
   # Bump version and create tag
   make bump-patch  # or bump-minor, bump-major
   git add pyproject.toml src/dg_sqlmesh/__init__.py
   git commit -m "chore: bump version to 1.8.2"
   git tag v1.8.2
   git push origin main --tags
   ```

2. **GitHub Actions handles**:
   - ✅ Version validation
   - ✅ Test execution
   - ✅ Package building
   - ✅ PyPI publication
   - ✅ GitHub release creation

### Manual Release

Use the workflow dispatch in GitHub:

1. Go to Actions → Release
2. Click "Run workflow"
3. Enter version (e.g., `1.8.2`)

## 🔐 Secrets Required

| Secret           | Purpose          | Setup                                  |
| ---------------- | ---------------- | -------------------------------------- |
| `PYPI_API_TOKEN` | PyPI publication | https://pypi.org/manage/account/token/ |

## 📊 Monitoring

### **Status Badges**

Add to README.md:

```markdown
[![Tests](https://github.com/fosk06/dagster-sqlmesh/actions/workflows/test.yml/badge.svg)](https://github.com/fosk06/dagster-sqlmesh/actions/workflows/test.yml)
[![Release](https://github.com/fosk06/dagster-sqlmesh/actions/workflows/release.yml/badge.svg)](https://github.com/fosk06/dagster-sqlmesh/actions/workflows/release.yml)
[![Security](https://github.com/fosk06/dagster-sqlmesh/actions/workflows/security.yml/badge.svg)](https://github.com/fosk06/dagster-sqlmesh/actions/workflows/security.yml)
[![codecov](https://codecov.io/gh/fosk06/dagster-sqlmesh/branch/main/graph/badge.svg)](https://codecov.io/gh/fosk06/dagster-sqlmesh)
```

### **Coverage**

Coverage reports are uploaded to Codecov automatically. Set up at:
https://codecov.io/gh/fosk06/dagster-sqlmesh

## 🛠️ Development

### **Local Testing**

Before pushing, run locally:

```bash
# Full validation
make validate

# Individual steps
make lint        # ruff linting
make test        # pytest with coverage
make vulture     # dead code detection
make build       # package build
```

### **Pre-commit Setup** (Optional)

```bash
# Install pre-commit
pip install pre-commit

# Setup hooks
pre-commit install

# Manual run
pre-commit run --all-files
```

## 🔍 Troubleshooting

### **Common Issues**

1. **Version Mismatch**:

   - Ensure `pyproject.toml` and `src/dg_sqlmesh/__init__.py` have same version
   - Use `make check-version` to verify

2. **Test Failures**:

   - Run `make test` locally first
   - Check test data loading with `tests/load_jaffle_data.py`

3. **Build Issues**:

   - Clean build artifacts: `make clean`
   - Verify dependencies: `uv sync --group dev`

4. **PyPI Publication**:
   - Verify `PYPI_API_TOKEN` secret is set
   - Check token permissions on PyPI

### **Debug Workflows**

Enable debug logging:

```yaml
env:
  ACTIONS_STEP_DEBUG: true
  ACTIONS_RUNNER_DEBUG: true
```

## 📈 Metrics

### **GitHub Actions Usage**

- **Ubuntu runners**: ~2000 minutes/month free
- **Current usage**: ~5-10 minutes per PR, ~15 minutes per release
- **Estimated capacity**: ~200-400 PRs/month within free tier

### **Optimization**

- ✅ Matrix strategy for parallel execution
- ✅ Ubuntu-only (10x cheaper than macOS)
- ✅ Cached dependencies with uv
- ✅ Conditional job execution
- ✅ Artifact cleanup (30-90 day retention)

---

_This CI/CD infrastructure follows GitHub Actions best practices and provides comprehensive testing, security, and release automation for the dg-sqlmesh project._
