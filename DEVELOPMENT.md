# Development Guide

Ce guide explique comment développer et publier le package `dg-sqlmesh`.

## 🛠️ Makefile Commands

Le projet inclut un Makefile complet pour automatiser les tâches de développement et de publication.

### 📋 Commandes de base

```bash
# Afficher l'aide
make help

# Vérifier la version actuelle
make check-version

# Afficher les informations du package
make info

# Nettoyer les artefacts de build
make clean

# Builder le package
make build

# Installer en mode développement
make install-dev

# Détecter le code mort avec vulture
make vulture
```

### 🧪 Configuration du projet SQLMesh de test

Le projet inclut un projet SQLMesh de test complet dans `tests/sqlmesh_project/` pour tester l'intégration.

#### Prérequis

```bash
# Installer les dépendances de développement (inclut SQLMesh et DuckDB)
uv sync --group dev
```

#### Configuration de la base de données

Le projet utilise DuckDB avec une base persistée pour les tests :

```bash
# Charger les données de test dans DuckDB
uv run --group dev python tests/load_jaffle_data.py
```

**Données chargées :**

- `raw_source_customers` : 2,583 lignes
- `raw_source_products` : 10 lignes
- `raw_source_orders` : 657,460 lignes
- `raw_source_items` : 975,185 lignes
- `raw_source_stores` : 6 lignes
- `raw_source_supplies` : 65 lignes
- `raw_source_tweets` : 3 lignes

#### Test du projet SQLMesh

```bash
# Vérifier que le plan SQLMesh fonctionne
uv run --group dev sqlmesh -p tests/sqlmesh_project plan --no-prompts

# Appliquer le plan (optionnel)
uv run --group dev sqlmesh -p tests/sqlmesh_project apply --no-prompts
```

#### Structure du projet de test

```
tests/
├── sqlmesh_project/
│   ├── config.yaml              # Configuration DuckDB
│   ├── external_models.yaml     # Modèles externes
│   ├── models/                  # Modèles SQLMesh
│   │   ├── stg/                # Modèles staging
│   │   └── marts/              # Modèles marts
│   └── jaffle_test.db          # Base DuckDB (ignorée par Git)
├── jaffle-data/                 # Données source CSV
│   ├── raw_source_customers.csv
│   ├── raw_source_products.csv
│   ├── raw_source_orders.csv
│   ├── raw_source_items.csv
│   ├── raw_source_stores.csv
│   ├── raw_source_supplies.csv
│   └── raw_source_tweets.csv
└── load_jaffle_data.py         # Script de chargement
```

#### Test de l'intégration avec notre package

```python
# Exemple d'utilisation avec le projet de test
from dg_sqlmesh import sqlmesh_definitions_factory

# Créer les definitions avec le projet de test
defs = sqlmesh_definitions_factory(
    project_dir="tests/sqlmesh_project",
    gateway="duckdb",
    ignore_cron=True  # Pour les tests
)

# Utiliser avec Dagster
from dagster import materialize
result = materialize(defs)
```

#### Dépannage

**Erreur de table manquante :**

```bash
# Recharger les données
uv run --group dev python tests/load_jaffle_data.py
```

**Erreur de configuration :**

```bash
# Vérifier la configuration
uv run --group dev sqlmesh -p tests/sqlmesh_project plan --no-prompts
```

**Base de données corrompue :**

```bash
# Supprimer et recréer
rm tests/sqlmesh_project/jaffle_test.db
uv run --group dev python tests/load_jaffle_data.py
```

### 🚀 Publication sur PyPI

#### Préparation

1. **Obtenir un token PyPI** :

   - Allez sur https://pypi.org/account/tokens/
   - Créez un token API avec les permissions d'upload

2. **Configurer l'environnement** (choisir une méthode) :

   **Méthode 1 : Fichier .env (recommandé)**

   ```bash
   # Créer un fichier .env à la racine du projet
   echo "UV_PUBLISH_TOKEN=your_pypi_token_here" > .env
   ```

   **Méthode 2 : Variable d'environnement**

   ```bash
   export UV_PUBLISH_TOKEN=your_pypi_token_here
   ```

   **Note** : Le fichier `.env` est automatiquement chargé par le Makefile et est ignoré par Git pour la sécurité.

#### Publication

```bash
# Publication simple (charge automatiquement .env si présent)
make publish

# Publication avec validation complète
make validate
make publish

# Publication rapide (build + publish)
make quick-publish
```

#### Alternative avec username/password

```bash
# Dans le fichier .env
UV_PUBLISH_USERNAME=your_username
UV_PUBLISH_PASSWORD=your_password

# Ou en variables d'environnement
export UV_PUBLISH_USERNAME=your_username
export UV_PUBLISH_PASSWORD=your_password

# Puis publier
make publish-auth
```

### 🔢 Gestion des versions

Le Makefile inclut des commandes pour automatiser le bump de version :

```bash
# Bump patch version (0.1.0 -> 0.1.1)
make bump-patch

# Bump minor version (0.1.0 -> 0.2.0)
make bump-minor

# Bump major version (0.1.0 -> 1.0.0)
make bump-major
```

### 🎉 Workflows de release complets

```bash
# Release patch (clean + bump + build + publish)
make release-patch

# Release minor
make release-minor

# Release major
make release-major
```

### 🧪 Tests et validation

```bash
# Lancer les tests
make test

# Validation complète (clean + build + test + vulture)
make validate

# Vérifier la qualité du package
make check

# Détecter le code mort
make vulture
```

## 📦 Structure du package

```
dg-sqlmesh/
├── src/dg_sqlmesh/
│   ├── __init__.py              # Point d'entrée avec API publique
│   ├── factory.py               # Factories principales (renommé de decorators.py)
│   ├── resource.py              # SQLMeshResource
│   ├── translator.py            # SQLMeshTranslator
│   ├── sqlmesh_asset_utils.py  # Utilitaires internes
│   ├── sqlmesh_asset_check_utils.py # Utilitaires pour checks
│   └── sqlmesh_event_console.py # Console personnalisée
├── tests/                       # Tests unitaires
├── examples/                    # Exemples d'utilisation
├── docs/                        # Documentation
├── pyproject.toml              # Configuration du package
├── Makefile                    # Commandes de développement
└── README.md                   # Documentation principale
```

## 🔧 API publique

Le package expose les éléments suivants :

```python
from dg_sqlmesh import (
    # Point d'entrée principal
    sqlmesh_definitions_factory,      # Factory tout-en-un
    sqlmesh_assets_factory,           # Factory pour assets
    sqlmesh_adaptive_schedule_factory, # Factory pour schedule

    # Composants principaux
    SQLMeshResource,                  # Resource Dagster
    SQLMeshTranslator,                # Translator extensible
)
```

## 🚀 Workflow de développement typique

1. **Développement** :

   ```bash
   make install-dev
   # ... développement ...
   make test
   make vulture  # Vérifier le code mort
   ```

2. **Préparation release** :

   ```bash
   make clean
   make build
   make validate  # Inclut vulture
   ```

3. **Publication** :

   ```bash
   make bump-patch  # ou bump-minor/major
   make publish
   ```

## 📋 Checklist avant publication

- [ ] Tests passent : `make test`
- [ ] Build réussi : `make build`
- [ ] Code mort vérifié : `make vulture`
- [ ] Version mise à jour : `make check-version`
- [ ] Documentation à jour
- [ ] Token PyPI configuré : `export UV_PUBLISH_TOKEN=...`
- [ ] Validation complète : `make validate`

## 🔍 Dépannage

### Erreur de token PyPI

```bash
❌ Error: UV_PUBLISH_TOKEN environment variable not set
```

**Solution** : `export UV_PUBLISH_TOKEN=your_token`

### Erreur de build

```bash
make clean
make build
```

### Erreur de version

```bash
make check-version
# Vérifier que pyproject.toml et __init__.py sont synchronisés
```

### Code mort détecté par vulture

Vulture peut détecter du code mort pendant le développement. C'est normal et n'empêche pas la publication :

```bash
make vulture
# Analyse le code mort avec un seuil de confiance de 50%
# Ne fait pas échouer le build
```

## 📚 Ressources

- [Documentation PyPI](https://packaging.python.org/)
- [Documentation uv](https://docs.astral.sh/uv/)
- [Documentation Dagster](https://docs.dagster.io/)
- [Documentation SQLMesh](https://sqlmesh.com/)
