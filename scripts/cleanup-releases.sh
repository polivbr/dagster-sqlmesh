#!/bin/bash

# Script de nettoyage des releases problématiques
# Usage: ./scripts/cleanup-releases.sh

set -e

echo "🧹 Starting release cleanup..."

# Vérifier la version actuelle
echo "📋 Current project version:"
make check-version

# Lister tous les tags
echo ""
echo "🏷️  All tags:"
git tag -l | sort -V

# Demander confirmation pour supprimer les tags problématiques
echo ""
echo "⚠️  The following tags may cause conflicts:"
echo "   - v1.9.1 (already on PyPI)"
echo "   - v1.9.2 (already on PyPI)"

read -p "Do you want to remove these tags? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️  Removing problematic tags..."
    
    # Supprimer les tags locaux
    git tag -d v1.9.1 2>/dev/null || echo "Tag v1.9.1 not found locally"
    git tag -d v1.9.2 2>/dev/null || echo "Tag v1.9.2 not found locally"
    
    # Supprimer les tags distants
    git push origin :refs/tags/v1.9.1 2>/dev/null || echo "Remote tag v1.9.1 not found"
    git push origin :refs/tags/v1.9.2 2>/dev/null || echo "Remote tag v1.9.2 not found"
    
    echo "✅ Problematic tags removed"
else
    echo "❌ Cleanup cancelled"
    exit 0
fi

# Vérifier la version pour la prochaine release
echo ""
echo "🔍 Checking version for next release..."

CURRENT_VERSION=$(grep 'version = ' pyproject.toml | cut -d'"' -f2)
echo "Current version in pyproject.toml: $CURRENT_VERSION"

# Suggérer la prochaine version
if [[ $CURRENT_VERSION =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    MAJOR=${BASH_REMATCH[1]}
    MINOR=${BASH_REMATCH[2]}
    PATCH=${BASH_REMATCH[3]}
    
    NEXT_PATCH="$MAJOR.$MINOR.$((PATCH + 1))"
    NEXT_MINOR="$MAJOR.$((MINOR + 1)).0"
    NEXT_MAJOR="$((MAJOR + 1)).0.0"
    
    echo ""
    echo "📈 Suggested next versions:"
    echo "   Patch:  $NEXT_PATCH (recommended for fixes)"
    echo "   Minor:  $NEXT_MINOR (for new features)"
    echo "   Major:  $NEXT_MAJOR (for breaking changes)"
    
    echo ""
    echo "🚀 To create a new release:"
    echo "   1. Update version: make bump-patch (or bump-minor/major)"
    echo "   2. Commit changes: git add . && git commit -m 'chore: bump version to $NEXT_PATCH'"
    echo "   3. Create tag: git tag -a v$NEXT_PATCH -m 'Release v$NEXT_PATCH: [description]'"
    echo "   4. Push tag: git push origin v$NEXT_PATCH"
    echo "   5. CI/CD will automatically: validate → GitHub release → PyPI publish"
fi

echo ""
echo "✅ Cleanup completed successfully!"
