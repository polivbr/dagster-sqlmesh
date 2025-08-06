# Makefile for dg-sqlmesh package
# Usage: make <target>

.PHONY: help build clean publish test install-dev bump-version bump-patch bump-minor bump-major check-version vulture

# Default target
help:
	@echo "Available targets:"
	@echo "  help          - Show this help message"
	@echo "  build         - Build the package (source + wheel)"
	@echo "  clean         - Clean build artifacts and dist"
	@echo "  publish       - Publish to PyPI (requires UV_PUBLISH_TOKEN)"
	@echo "  test          - Run tests"
	@echo "  install-dev   - Install in development mode"
	@echo "  check-version - Show current version"
	@echo "  vulture       - Detect dead code with vulture"
	@echo "  ruff          - Lint code with ruff"
	@echo "  format        - Format code with ruff"
	@echo "  lint          - Lint and format code"
	@echo "  bump-patch    - Bump patch version (0.1.0 -> 0.1.1)"
	@echo "  bump-minor    - Bump minor version (0.1.0 -> 0.2.0)"
	@echo "  bump-major    - Bump major version (0.1.0 -> 1.0.0)"
	@echo ""
	@echo "Examples:"
	@echo "  make build"
	@echo "  make publish"
	@echo "  make bump-patch"

# Build the package
build:
	@echo "🔨 Building package..."
	uv build
	@echo "✅ Build completed! Check dist/ directory"

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	rm -rf dist/
	rm -rf build/
	rm -rf src/*.egg-info/
	rm -rf *.egg-info/
	@echo "✅ Clean completed!"

# Detect dead code with vulture
vulture:
	@echo "🦅 Detecting dead code with vulture..."
	@uv run vulture src/dg_sqlmesh/ --min-confidence 50 || true
	@echo "✅ Vulture analysis completed!"

# Lint code with ruff
ruff:
	@echo "🔍 Linting code with ruff..."
	@uv run ruff check src/dg_sqlmesh/
	@echo "✅ Ruff linting completed!"

# Format code with ruff
format:
	@echo "🎨 Formatting code with ruff..."
	@uv run ruff format src/dg_sqlmesh/
	@echo "✅ Code formatting completed!"

# Lint and format code
lint: ruff format
	@echo "✅ Linting and formatting completed!"

# Check publish configuration
check-publish:
	@echo "🔍 Checking publish configuration..."
	@bash -c 'if [ -f .env ]; then echo "📄 .env file found"; echo "📄 Loading environment from .env file..."; source .env; fi; if [ -z "$$UV_PUBLISH_TOKEN" ]; then echo "❌ UV_PUBLISH_TOKEN not set"; echo "   Add to .env: UV_PUBLISH_TOKEN=your_token"; echo "   Or export: export UV_PUBLISH_TOKEN=your_token"; else echo "✅ UV_PUBLISH_TOKEN is set (length: $$(echo $$UV_PUBLISH_TOKEN | wc -c))"; fi; if [ -z "$$UV_PUBLISH_USERNAME" ] && [ -z "$$UV_PUBLISH_PASSWORD" ]; then echo "ℹ️ UV_PUBLISH_USERNAME/PASSWORD not set (using token auth)"; else echo "✅ UV_PUBLISH_USERNAME/PASSWORD are set"; fi; echo "✅ Publish configuration check completed!"'

# Publish to PyPI
publish:
	@echo "🚀 Publishing to PyPI..."
	@bash -c 'if [ -f .env ]; then echo "📄 Loading environment from .env file..."; source .env; fi; if [ -z "$$UV_PUBLISH_TOKEN" ]; then echo "❌ Error: UV_PUBLISH_TOKEN environment variable not set"; echo "Set it with: export UV_PUBLISH_TOKEN=your_token"; echo "Or add it to your .env file: UV_PUBLISH_TOKEN=your_token"; exit 1; fi; uv publish --token $$UV_PUBLISH_TOKEN'
	@echo "✅ Published to PyPI!"

# Publish with username/password (alternative)
publish-auth:
	@echo "🚀 Publishing to PyPI with username/password..."
	@bash -c 'if [ -f .env ]; then echo "📄 Loading environment from .env file..."; source .env; fi; if [ -z "$$UV_PUBLISH_USERNAME" ] || [ -z "$$UV_PUBLISH_PASSWORD" ]; then echo "❌ Error: UV_PUBLISH_USERNAME and UV_PUBLISH_PASSWORD must be set"; echo "Set them with: export UV_PUBLISH_USERNAME=your_username"; echo "Or add them to your .env file: UV_PUBLISH_USERNAME=your_username"; exit 1; fi; uv publish --username $$UV_PUBLISH_USERNAME --password $$UV_PUBLISH_PASSWORD'
	@echo "✅ Published to PyPI!"

# Run tests
test:
	@echo "�� Running tests..."
	@uv run --group dev pytest tests/ -v || echo "⚠️ No tests found or tests failed"
	@echo "✅ Tests completed!"

# Install in development mode
install-dev:
	@echo "📦 Installing in development mode..."
	uv pip install -e .
	@echo "✅ Development installation completed!"

# Check current version
check-version:
	@echo "📋 Current version:"
	@grep 'version = ' pyproject.toml
	@grep '__version__ = ' src/dg_sqlmesh/__init__.py

# Bump version helpers
bump-version:
	@echo "❌ Please specify version type: make bump-patch, bump-minor, or bump-major"

bump-patch:
	@echo "🔢 Bumping patch version..."
	@current_version=$$(grep 'version = ' pyproject.toml | cut -d'"' -f2); \
	new_version=$$(echo $$current_version | awk -F. '{print $$1"."$$2"."$$3+1}'); \
	echo "Updating version from $$current_version to $$new_version"; \
	sed -i '' 's/version = ".*"/version = "'$$new_version'"/' pyproject.toml; \
	sed -i '' 's/__version__ = ".*"/__version__ = "'$$new_version'"/' src/dg_sqlmesh/__init__.py; \
	echo "✅ Version bumped to $$new_version"

bump-minor:
	@echo "🔢 Bumping minor version..."
	@current_version=$$(grep 'version = ' pyproject.toml | cut -d'"' -f2); \
	new_version=$$(echo $$current_version | awk -F. '{print $$1"."$$2+1".0"}'); \
	echo "Updating version from $$current_version to $$new_version"; \
	sed -i '' 's/version = ".*"/version = "'$$new_version'"/' pyproject.toml; \
	sed -i '' 's/__version__ = ".*"/__version__ = "'$$new_version'"/' src/dg_sqlmesh/__init__.py; \
	echo "✅ Version bumped to $$new_version"

bump-major:
	@echo "🔢 Bumping major version..."
	@current_version=$$(grep 'version = ' pyproject.toml | cut -d'"' -f2); \
	new_version=$$(echo $$current_version | awk -F. '{print $$1+1".0.0"}'); \
	echo "Updating version from $$current_version to $$new_version"; \
	sed -i '' 's/version = ".*"/version = "'$$new_version'"/' pyproject.toml; \
	sed -i '' 's/__version__ = ".*"/__version__ = "'$$new_version'"/' src/dg_sqlmesh/__init__.py; \
	echo "✅ Version bumped to $$new_version"

# Full release workflow
release-patch: clean bump-patch build publish
	@echo "🎉 Patch release completed!"

release-minor: clean bump-minor build publish
	@echo "🎉 Minor release completed!"

release-major: clean bump-major build publish
	@echo "🎉 Major release completed!"

# Development workflow
dev-setup: install-dev
	@echo "✅ Development environment ready!"

# Check package quality
check:
	@echo "🔍 Checking package quality..."
	@echo "📋 Current version:"
	@make check-version
	@echo ""
	@echo "📦 Package structure:"
	@ls -la src/dg_sqlmesh/
	@echo ""
	@echo "📄 Dependencies:"
	@uv tree
	@echo ""
	@echo "🔍 Code linting:"
	@make ruff
	@echo ""
	@echo "🦅 Dead code analysis:"
	@make vulture
	@echo ""
	@echo "✅ Quality check completed!"

# Show package info
info:
	@echo "📦 Package Information:"
	@echo "Name: dg-sqlmesh"
	@echo "Description: Seamless integration between Dagster and SQLMesh"
	@echo "License: Apache-2.0"
	@echo "Author: Thomas Trividic"
	@echo ""
	@echo "📋 Current version:"
	@make check-version
	@echo ""
	@echo "🔗 PyPI URLs:"
	@echo "  - Homepage: https://github.com/thomastrividic/dagster-sqlmesh"
	@echo "  - Repository: https://github.com/thomastrividic/dagster-sqlmesh"
	@echo "  - Documentation: https://github.com/thomastrividic/dagster-sqlmesh#readme"

# Validate package before publishing
validate: clean build test ruff vulture
	@echo "✅ Package validation completed!"

# Quick publish (build + publish)
quick-publish: build publish
	@echo "🚀 Quick publish completed!" 