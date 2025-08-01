"""
Dagster SQLMesh Integration

A Python package that provides seamless integration between Dagster and SQLMesh
for modern data engineering workflows.
"""

__version__ = "0.1.1"
__author__ = "Thomas Trividic"

# Import main components for easy access
from .factory import sqlmesh_definitions_factory, sqlmesh_assets_factory, sqlmesh_adaptive_schedule_factory
from .resource import SQLMeshResource
from .translator import SQLMeshTranslator

__all__ = [
    "__version__",
    "__author__",
    "sqlmesh_definitions_factory",
    "sqlmesh_assets_factory", 
    "sqlmesh_adaptive_schedule_factory",
    "SQLMeshResource",
    "SQLMeshTranslator",
]
