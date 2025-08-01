# Utility functions for SQLMesh AssetCheckSpec creation

from dagster import AssetCheckSpec, AssetKey
from typing import List
from sqlmesh.core.model.definition import ExternalModel


def create_asset_checks_from_model(model, asset_key: AssetKey) -> List[AssetCheckSpec]:
    """
    Creates AssetCheckSpec for audits of a SQLMesh model.
    
    Args:
        model: SQLMesh model
        asset_key: Dagster AssetKey associated with the model
    
    Returns:
        List of AssetCheckSpec for model audits
    """
    asset_checks = []
    
    # Get model audits
    audits_with_args = model.audits_with_args if hasattr(model, 'audits_with_args') else []
    
    for audit_obj, audit_args in audits_with_args:
        asset_checks.append(
            AssetCheckSpec(
                name=audit_obj.name,
                asset=asset_key,  # ← It's "asset" not "asset_key" !
                description=f"Triggered by sqlmesh audit {audit_obj.name} on model {model.name}",
                blocking=False,  # ← sqlmesh can block materialization if audit fails, but we don't want to block dagster
                metadata={
                    "audit_query": str(audit_obj.query.sql()),
                    "audit_blocking": audit_obj.blocking,  # ← Keep original info in metadata
                    "audit_dialect": audit_obj.dialect,
                    "audit_args": audit_args
                }
            )
        )
    
    return asset_checks


def create_all_asset_checks(models, translator) -> List[AssetCheckSpec]:
    """
    Creates all AssetCheckSpec for all SQLMesh models.
    
    Args:
        models: List of SQLMesh models
        translator: SQLMeshTranslator to map models to AssetKey
    
    Returns:
        List of all AssetCheckSpec
    """
    all_checks = []
    
    for model in models:
        # Ignore external models
        if isinstance(model, ExternalModel):
            continue
            
        asset_key = translator.get_asset_key(model)
        model_checks = create_asset_checks_from_model(model, asset_key)
        all_checks.extend(model_checks)
    
    return all_checks 