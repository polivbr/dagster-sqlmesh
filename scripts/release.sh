#!/bin/bash

# Release script for dg-sqlmesh
# Usage: ./scripts/release.sh [patch|minor|major]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default to patch if no argument provided
BUMP_TYPE=${1:-patch}

# Validate bump type
if [[ ! "$BUMP_TYPE" =~ ^(patch|minor|major)$ ]]; then
    echo -e "${RED}❌ Invalid bump type: $BUMP_TYPE${NC}"
    echo -e "${YELLOW}Usage: $0 [patch|minor|major]${NC}"
    exit 1
fi

echo -e "${BLUE}🚀 Starting release process for dg-sqlmesh${NC}"
echo -e "${BLUE}📈 Bump type: $BUMP_TYPE${NC}"

# Check if we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo -e "${RED}❌ Not on main branch (current: $CURRENT_BRANCH)${NC}"
    echo -e "${YELLOW}Please switch to main branch: git checkout main${NC}"
    exit 1
fi

# Check if working directory is clean
if [ -n "$(git status --porcelain)" ]; then
    echo -e "${RED}❌ Working directory is not clean${NC}"
    echo -e "${YELLOW}Please commit or stash your changes${NC}"
    git status --short
    exit 1
fi

# Pull latest changes
echo -e "${BLUE}📥 Pulling latest changes...${NC}"
git pull origin main

# Get current version
CURRENT_VERSION=$(grep 'version = ' pyproject.toml | cut -d'"' -f2)
echo -e "${BLUE}📋 Current version: $CURRENT_VERSION${NC}"

# Run validation
echo -e "${BLUE}🔍 Running validation...${NC}"
make validate

echo -e "${GREEN}✅ Validation passed!${NC}"

# Bump version
echo -e "${BLUE}🔢 Bumping $BUMP_TYPE version...${NC}"
make bump-$BUMP_TYPE

# Get new version
NEW_VERSION=$(grep 'version = ' pyproject.toml | cut -d'"' -f2)
echo -e "${GREEN}✅ Version bumped from $CURRENT_VERSION to $NEW_VERSION${NC}"

# Create commit
echo -e "${BLUE}📝 Creating release commit...${NC}"
git add pyproject.toml src/dg_sqlmesh/__init__.py
git commit -m "chore: bump version to $NEW_VERSION"

# Create tag
echo -e "${BLUE}🏷️ Creating tag v$NEW_VERSION...${NC}"
git tag "v$NEW_VERSION"

# Show summary
echo -e "${GREEN}✅ Release preparation completed!${NC}"
echo
echo -e "${YELLOW}📋 Summary:${NC}"
echo -e "  • Version: $CURRENT_VERSION → $NEW_VERSION"
echo -e "  • Commit: chore: bump version to $NEW_VERSION"
echo -e "  • Tag: v$NEW_VERSION"
echo
echo -e "${YELLOW}🚀 Next steps:${NC}"
echo -e "  1. Review the changes: ${BLUE}git show${NC}"
echo -e "  2. Push to trigger release: ${BLUE}git push origin main --tags${NC}"
echo -e "  3. Monitor GitHub Actions: ${BLUE}https://github.com/fosk06/dagster-sqlmesh/actions${NC}"
echo
echo -e "${YELLOW}📦 After release:${NC}"
echo -e "  • PyPI: https://pypi.org/project/dg-sqlmesh/$NEW_VERSION/"
echo -e "  • GitHub: https://github.com/fosk06/dagster-sqlmesh/releases/tag/v$NEW_VERSION"

# Ask for confirmation to push
echo
read -p "$(echo -e ${YELLOW}❓ Push to GitHub now? [y/N]: ${NC})" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${BLUE}🚀 Pushing to GitHub...${NC}"
    git push origin main --tags
    echo -e "${GREEN}✅ Release triggered! Check GitHub Actions for progress.${NC}"
    echo -e "${BLUE}🔗 https://github.com/fosk06/dagster-sqlmesh/actions${NC}"
else
    echo -e "${YELLOW}⏸️ Push skipped. Run this when ready:${NC}"
    echo -e "${BLUE}git push origin main --tags${NC}"
fi
