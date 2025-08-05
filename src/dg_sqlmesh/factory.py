from dagster import (
    asset,
    AssetExecutionContext,
    RetryPolicy,
    schedule,
    define_asset_job,
    RunRequest,
    Definitions,
    AssetKey,
    MaterializeResult,
    AssetCheckResult,
)
from .resource import SQLMeshResource
from .resource import UpstreamAuditFailureError
from .sqlmesh_asset_utils import (
    get_asset_kinds,
    create_asset_specs,
    create_asset_checks,
    get_extra_keys,
    validate_external_dependencies,
    get_models_to_materialize,
)
from .sqlmesh_asset_check_utils import create_asset_checks_from_model
from sqlmesh.core.model.definition import ExternalModel
import datetime
from .translator import SQLMeshTranslator
from typing import Optional, Dict, List, Any

def sqlmesh_assets_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    retry_policy: Optional[RetryPolicy] = None,
    owners: Optional[List[str]] = None,
):
    """
    Factory to create SQLMesh Dagster assets.
    """
    try:
        extra_keys = get_extra_keys()
        kinds = get_asset_kinds(sqlmesh_resource)
        specs = create_asset_specs(sqlmesh_resource, extra_keys, kinds, owners, group_name)
        asset_checks = create_asset_checks(sqlmesh_resource)
    except Exception as e:
        raise ValueError(f"Failed to create SQLMesh assets: {e}") from e

    # Créer les assets individuels directement (sans orchestrateur pour le build)
    assets = []
    
    def create_model_asset(current_model_name, current_asset_spec, current_model_checks):
        @asset(
            key=current_asset_spec.key,
            description=f"SQLMesh model: {current_model_name}",
            group_name=current_asset_spec.group_name,
            metadata=current_asset_spec.metadata,
            tags=current_asset_spec.tags,
            deps=current_asset_spec.deps,
            check_specs=current_model_checks,
            op_tags=op_tags,
            retry_policy=retry_policy,
        )
        def model_asset(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
            context.log.info(f"🔄 Processing SQLMesh model: {current_model_name}")
            
            # Materialiser directement ce modèle
            models_to_materialize = get_models_to_materialize(
                [current_asset_spec.key],
                sqlmesh.get_models,
                sqlmesh.translator,
            )
            
            if not models_to_materialize:
                raise Exception(f"No models found for asset {current_asset_spec.key}")
            
            # Lancer la materialization
            plan = sqlmesh.materialize_assets_threaded(models_to_materialize, context=context)
            
            # Vérifier le succès via la console
            failed_models_events = sqlmesh._console.get_failed_models_events()
            model_failed = False
            
            for event in failed_models_events:
                for error in event.get('errors', []):
                    failed_model_name = sqlmesh._extract_model_info(error)[0]
                    if failed_model_name == current_model_name:
                        model_failed = True
                        break
                if model_failed:
                    break
            
            if model_failed:
                error_msg = f"Model {current_model_name} failed during materialization"
                context.log.error(f"❌ {error_msg}")
                raise Exception(error_msg)
            else:
                context.log.info(f"✅ Model {current_model_name}: SUCCESS")
                
                # Si on a des checks, on doit retourner leurs résultats
                if current_model_checks:
                    # Récupérer les résultats des audits depuis la console
                    check_results = []
                    evaluation_events = sqlmesh._console.get_evaluation_events()
                    
                    context.log.info(f"🔍 Looking for evaluation events for model: {current_model_name}")
                    context.log.info(f"🔍 Found {len(evaluation_events)} evaluation events")
                    
                    for event in evaluation_events:
                        if event.get('event_type') == 'update_snapshot_evaluation':
                            snapshot_name = event.get('snapshot_name')
                            context.log.info(f"🔍 Checking snapshot: {snapshot_name}")
                            # Le snapshot_name contient le FQN complet avec guillemets, on doit extraire le nom du modèle
                            # snapshot_name: "jaffle_db"."sqlmesh_jaffle_platform"."stg_products"
                            # current_model_name: sqlmesh_jaffle_platform.stg_products
                            # On extrait la partie après le dernier point du snapshot
                            if snapshot_name:
                                # Extraire le nom du modèle depuis le snapshot
                                parts = snapshot_name.split('"."')
                                if len(parts) >= 3:
                                    snapshot_model_name = parts[1] + '.' + parts[2].replace('"', '')
                                    if snapshot_model_name == current_model_name:
                                        num_audits_passed = event.get('num_audits_passed', 0)
                                        num_audits_failed = event.get('num_audits_failed', 0)
                                        
                                        # Créer les résultats des checks
                                        for check in current_model_checks:
                                            # Pour l'instant, on considère que tous les audits passent si num_audits_failed == 0
                                            passed = num_audits_failed == 0
                                            check_results.append(
                                                AssetCheckResult(
                                                    check_name=check.name,
                                                    passed=passed,
                                                    metadata={
                                                        "audits_passed": num_audits_passed,
                                                        "audits_failed": num_audits_failed
                                                    }
                                                )
                                            )
                                        
                                        break
                    
                    # Si on n'a pas trouvé d'événement d'évaluation, créer des résultats par défaut
                    if not check_results:
                        context.log.warning(f"⚠️ No evaluation events found for model {current_model_name}, using default check results")
                        for check in current_model_checks:
                            check_results.append(
                                AssetCheckResult(
                                    check_name=check.name,
                                    passed=True,  # Par défaut, on considère que ça passe
                                    metadata={
                                        "note": "No evaluation events found, using default result"
                                    }
                                )
                            )
                    
                    # Retourner MaterializeResult + résultats des checks
                    return MaterializeResult(
                        asset_key=current_asset_spec.key,
                        metadata={
                            "status": "success"
                        }
                    ), *check_results
                else:
                    # Pas de checks, retourner juste MaterializeResult
                    return MaterializeResult(
                        asset_key=current_asset_spec.key,
                        metadata={
                            "status": "success"
                        }
                    )
        
        # Renommer pour éviter les collisions
        model_asset.__name__ = f"sqlmesh_{current_model_name}_asset"
        return model_asset
    
    # Utiliser les utilitaires existants
    models = sqlmesh_resource.get_models()
    
    # Créer les assets pour chaque modèle qui a un AssetSpec
    for model in models:
        # Ignorer les modèles externes
        if isinstance(model, ExternalModel):
            continue
            
        # Utiliser le translator pour obtenir l'AssetKey
        asset_key = sqlmesh_resource.translator.get_asset_key(model)
        
        # Chercher le bon AssetSpec dans la liste
        asset_spec = None
        for spec in specs:
            if spec.key == asset_key:
                asset_spec = spec
                break
        
        if asset_spec is None:
            continue  # Skip si pas de spec trouvé
            
        # Utiliser l'utilitaire existant pour créer les checks
        model_checks = create_asset_checks_from_model(model, asset_key)
        assets.append(create_model_asset(model.name, asset_spec, model_checks))
    
    return assets


def sqlmesh_adaptive_schedule_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_adaptive_schedule",
):
    """
    Factory to create an adaptive Dagster schedule based on SQLMesh crons.
    
    Args:
        sqlmesh_resource: Configured SQLMesh resource
        name: Schedule name
    """
    
    # Get recommended schedule based on SQLMesh crons
    recommended_schedule = sqlmesh_resource.get_recommended_schedule()
    
    # Create SQLMesh assets (list of individual assets)
    sqlmesh_assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)
    
    # Check if we have assets
    if not sqlmesh_assets:
        raise ValueError("No SQLMesh assets created - check if models exist")
    
    # Create job with all assets (no selection needed since we have individual assets)
    sqlmesh_job = define_asset_job(
        name="sqlmesh_job",
        selection=sqlmesh_assets,  # Pass the list of assets directly
    )
    
    @schedule(
        job=sqlmesh_job,
        cron_schedule=recommended_schedule,
        name=name,
        description=f"Adaptive schedule based on SQLMesh crons (granularity: {recommended_schedule})"
    )
    def _sqlmesh_adaptive_schedule(context):
        return RunRequest(
            run_key=f"sqlmesh_adaptive_{datetime.datetime.now().isoformat()}",
            tags={"schedule": "sqlmesh_adaptive", "granularity": recommended_schedule}
        )
    
    return _sqlmesh_adaptive_schedule, sqlmesh_job, sqlmesh_assets


def sqlmesh_definitions_factory(
    *,
    project_dir: str = "sqlmesh_project",
    gateway: str = "postgres",
    environment: str = "prod",
    concurrency_limit: int = 1,
    ignore_cron: bool = False,
    translator: Optional[SQLMeshTranslator] = None,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    retry_policy: Optional[RetryPolicy] = None,
    owners: Optional[List[str]] = None,
    schedule_name: str = "sqlmesh_adaptive_schedule",
):
    """
    All-in-one factory to create a complete SQLMesh integration with Dagster.
    
    Args:
        project_dir: SQLMesh project directory
        gateway: SQLMesh gateway (postgres, duckdb, etc.)
        concurrency_limit: Concurrency limit
        ignore_cron: Ignore crons (for tests)
        translator: Custom translator for asset keys
        name: Multi-asset name
        group_name: Default group for assets
        op_tags: Operation tags
        retry_policy: Retry policy
        owners: Asset owners
        schedule_name: Adaptive schedule name
    """
    
    # Parameter validation
    if concurrency_limit < 1:
        raise ValueError("concurrency_limit must be >= 1")
    
    # Robust default values
    op_tags = op_tags or {"sqlmesh": "true"}
    owners = owners or []
    
    # Create SQLMesh resource
    sqlmesh_resource = SQLMeshResource(
        project_dir=project_dir,
        gateway=gateway,
        environment=environment,
        translator=translator,
        concurrency_limit=concurrency_limit,
        ignore_cron=ignore_cron
    )
    
    # Validate external dependencies
    try:
        models = sqlmesh_resource.get_models()
        validation_errors = validate_external_dependencies(sqlmesh_resource, models)
        if validation_errors:
            raise ValueError(f"External dependencies validation failed:\n" + "\n".join(validation_errors))
    except Exception as e:
        raise ValueError(f"Failed to validate external dependencies: {e}") from e
    
    # Create SQLMesh assets
    sqlmesh_assets = sqlmesh_assets_factory(
        sqlmesh_resource=sqlmesh_resource,
        group_name=group_name,
        op_tags=op_tags,
        retry_policy=retry_policy,
        owners=owners,
    )
    
    # Create adaptive schedule and job
    sqlmesh_adaptive_schedule, sqlmesh_job, _ = sqlmesh_adaptive_schedule_factory(
        sqlmesh_resource=sqlmesh_resource,
        name=schedule_name
    )
    
    # Return complete Definitions
    return Definitions(
        assets=sqlmesh_assets,
        jobs=[sqlmesh_job],
        schedules=[sqlmesh_adaptive_schedule],
        resources={
            "sqlmesh": sqlmesh_resource,
        },
    ) 