from dagster import (
    asset,
    AssetExecutionContext,
    schedule,
    define_asset_job,
    RunRequest,
    Definitions,
    MaterializeResult,
    AssetCheckResult,
    ConfigurableResource,
)
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import (
    get_asset_kinds,
    create_asset_specs,
    get_extra_keys,
    validate_external_dependencies,
    get_models_to_materialize,
)
from .sqlmesh_asset_check_utils import create_asset_checks_from_model
from sqlmesh.core.model.definition import ExternalModel
import datetime
from .translator import SQLMeshTranslator
from typing import Optional, Dict, List, Any

class SQLMeshResultsResource(ConfigurableResource):
    """Resource pour partager les résultats SQLMesh entre les assets d'un même run."""
    
    def __init__(self):
        super().__init__()
        self._results = {}
    
    def store_results(self, run_id: str, results: Dict[str, Any]) -> None:
        """Stocke les résultats SQLMesh pour un run donné."""
        self._results[run_id] = results
    
    def get_results(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Récupère les résultats SQLMesh pour un run donné."""
        return self._results.get(run_id)
    
    def has_results(self, run_id: str) -> bool:
        """Vérifie si des résultats existent pour un run donné."""
        return run_id in self._results

def sqlmesh_assets_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    owners: Optional[List[str]] = None,
):
    """
    Factory to create SQLMesh Dagster assets.
    """
    try:
        extra_keys = get_extra_keys()
        kinds = get_asset_kinds(sqlmesh_resource)
        specs = create_asset_specs(sqlmesh_resource, extra_keys, kinds, owners, group_name)
    except Exception as e:
        raise ValueError(f"Failed to create SQLMesh assets: {e}") from e

    # Créer les assets individuels avec exécution SQLMesh partagée
    assets = []
    
    def create_model_asset(current_model_name, current_asset_spec, current_model_checks):
        @asset(
            key=current_asset_spec.key,
            description=f"SQLMesh model: {current_model_name}",
            group_name=current_asset_spec.group_name,
            metadata=current_asset_spec.metadata,
            deps=current_asset_spec.deps,
            check_specs=current_model_checks,
            op_tags=op_tags,
            # Force no retries to prevent infinite loops with SQLMesh audit failures
            tags={
                **(current_asset_spec.tags or {}),
                "dagster/max_retries": "0",
                "dagster/retry_on_asset_or_op_failure": "false"
            },
        )
        def model_asset(context: AssetExecutionContext, sqlmesh: SQLMeshResource, sqlmesh_results: SQLMeshResultsResource):
            context.log.info(f"🔄 Processing SQLMesh model: {current_model_name}")
            context.log.info(f"🔍 DEBUG: Run ID: {context.run_id}")
            context.log.info(f"🔍 DEBUG: Asset Key: {current_asset_spec.key}")
            context.log.info(f"🔍 DEBUG: Selected assets: {context.selected_asset_keys}")
            
            # Vérifier si on a déjà exécuté SQLMesh dans ce run
            run_id = context.run_id
            
            # Récupérer ou créer les résultats SQLMesh partagés
            if not sqlmesh_results.has_results(run_id):
                context.log.info(f"🚀 First asset in run, launching SQLMesh execution for all selected assets")
                context.log.info(f"🔍 DEBUG: No existing results for run {run_id}")
                
                # Obtenir tous les assets sélectionnés dans ce run
                selected_asset_keys = context.selected_asset_keys
                context.log.info(f"🔍 Selected assets in this run: {selected_asset_keys}")
                
                # Lancer une seule exécution SQLMesh pour tous les assets sélectionnés
                models_to_materialize = get_models_to_materialize(
                    selected_asset_keys,
                    sqlmesh.get_models,
                    sqlmesh.translator,
                )
                
                if not models_to_materialize:
                    raise Exception(f"No models found for selected assets: {selected_asset_keys}")
                
                context.log.info(f"🔍 Materializing {len(models_to_materialize)} models: {[m.name for m in models_to_materialize]}")
                
                # Exécution SQLMesh unique
                context.log.info(f"🔍 DEBUG: Starting SQLMesh materialization...")
                plan = sqlmesh.materialize_assets_threaded(models_to_materialize, context=context)
                context.log.info(f"🔍 DEBUG: SQLMesh materialization completed")
                
                # Capturer tous les résultats
                context.log.info(f"🔍 DEBUG: Processing failed models events...")
                failed_check_results = sqlmesh._process_failed_models_events()
                context.log.info(f"🔍 DEBUG: Failed check results count: {len(failed_check_results)}")
                
                context.log.info(f"🔍 DEBUG: Processing skipped models events...")
                skipped_models_events = sqlmesh._console.get_skipped_models_events()
                context.log.info(f"🔍 DEBUG: Skipped models events count: {len(skipped_models_events)}")
                
                context.log.info(f"🔍 DEBUG: Processing evaluation events...")
                evaluation_events = sqlmesh._console.get_evaluation_events()
                context.log.info(f"🔍 DEBUG: Evaluation events count: {len(evaluation_events)}")
                
                # Stocker les résultats dans le resource partagé
                results = {
                    "failed_check_results": failed_check_results,
                    "skipped_models_events": skipped_models_events,
                    "evaluation_events": evaluation_events,
                    "plan": plan
                }
                
                sqlmesh_results.store_results(run_id, results)
                context.log.info(f"💾 Stored SQLMesh results for run {run_id}")
                
            else:
                context.log.info(f"📋 Using existing SQLMesh results from run {run_id}")
                context.log.info(f"🔍 DEBUG: Found existing results for run {run_id}")
            
            # Récupérer les résultats pour ce run
            results = sqlmesh_results.get_results(run_id)
            failed_check_results = results["failed_check_results"]
            skipped_models_events = results["skipped_models_events"]
            evaluation_events = results["evaluation_events"]
            
            context.log.info(f"🔍 DEBUG: Processing results for model {current_model_name}")
            context.log.info(f"🔍 DEBUG: Failed check results: {len(failed_check_results)}")
            context.log.info(f"🔍 DEBUG: Skipped models events: {len(skipped_models_events)}")
            context.log.info(f"🔍 DEBUG: Evaluation events: {len(evaluation_events)}")
            
            # Vérifier le statut de notre modèle spécifique
            model_was_skipped = False
            model_has_audit_failures = False
            
            # Vérifier les skips à cause d'échecs upstream
            context.log.info(f"🔍 DEBUG: Checking for skipped models...")
            for event in skipped_models_events:
                skipped_snapshots = event.get('snapshot_names', set())
                context.log.info(f"🔍 Skipped snapshots: {skipped_snapshots}")
                
                for snapshot_name in skipped_snapshots:
                    if snapshot_name:
                        parts = snapshot_name.split('"."')
                        if len(parts) >= 3:
                            skipped_model_name = parts[1] + '.' + parts[2].replace('"', '')
                            context.log.info(f"🔍 DEBUG: Checking skipped model: {skipped_model_name} vs {current_model_name}")
                            if skipped_model_name == current_model_name:
                                model_was_skipped = True
                                context.log.error(f"❌ Model {current_model_name} was skipped due to upstream failures")
                                break
                if model_was_skipped:
                    break
            
            # Vérifier les échecs d'audit (modèle exécuté mais audit failed)
            context.log.info(f"🔍 DEBUG: Checking for audit failures...")
            for check_result in failed_check_results:
                context.log.info(f"🔍 DEBUG: Checking failed check: {check_result.asset_key} vs {current_asset_spec.key}")
                if check_result.asset_key == current_asset_spec.key:
                    model_has_audit_failures = True
                    context.log.error(f"❌ Model {current_model_name} has audit failures: {check_result.metadata.get('audit_message', 'Unknown error')}")
                    break
            
            context.log.info(f"🔍 DEBUG: Model {current_model_name} - was_skipped: {model_was_skipped}, has_audit_failures: {model_has_audit_failures}")
            
            # Décider de l'action à prendre
            if model_was_skipped:
                # Modèle skip → Lever une exception (pas de materialization)
                error_msg = f"Model {current_model_name} was skipped due to upstream failures"
                context.log.error(f"❌ {error_msg}")
                context.log.info(f"🔍 DEBUG: Raising exception for skipped model")
                raise Exception(error_msg)
            elif model_has_audit_failures:
                # Modèle exécuté mais audit failed → Materializer + AssetCheckResult(failed=True)
                context.log.info(f"⚠️ Model {current_model_name}: MATERIALIZATION SUCCESS but AUDIT FAILED")
                context.log.info(f"🔍 DEBUG: Returning MaterializeResult with failed checks")
                
                # Si on a des checks, on doit retourner leurs résultats
                if current_model_checks:
                    check_results = []
                    
                    # Créer des AssetCheckResult failed pour tous les checks
                    for check in current_model_checks:
                        # Trouver le message d'erreur spécifique pour ce check
                        audit_message = "Model materialization succeeded but audits failed"
                        for check_result in failed_check_results:
                            if check_result.asset_key == current_asset_spec.key:
                                audit_message = check_result.metadata.get('audit_message', audit_message)
                                break
                        
                        check_result = AssetCheckResult(
                            check_name=check.name,
                            passed=False,
                            metadata={
                                "audit_message": audit_message,
                                "audits_passed": 0,
                                "audits_failed": len(current_model_checks),
                                "sqlmesh_audit_name": check.name,  # Nom de l'audit SQLMesh
                                "sqlmesh_model": current_model_name,  # Nom du modèle SQLMesh
                                "error_details": f"SQLMesh audit '{check.name}' failed: {audit_message}"
                            }
                        )
                        check_results.append(check_result)
                        context.log.info(f"🔍 DEBUG: Created failed check result for: {check.name} with message: {audit_message}")
                    
                    context.log.info(f"🔍 DEBUG: Returning {len(check_results)} failed check results")
                    return MaterializeResult(
                        asset_key=current_asset_spec.key,
                        metadata={
                            "status": "materialization_success_audit_failed"
                        },
                        check_results=check_results  # ← CORRECT !
                    )
                else:
                    context.log.warning(f"⚠️ No checks defined for model {current_model_name}, returning only MaterializeResult")
                    return MaterializeResult(
                        asset_key=current_asset_spec.key,
                        metadata={
                            "status": "materialization_success_audit_failed"
                        }
                    )
            else:
                # Modèle exécuté et audit passed → Materializer + AssetCheckResult(passed=True)
                context.log.info(f"✅ Model {current_model_name}: SUCCESS")
                context.log.info(f"🔍 DEBUG: Returning MaterializeResult with passed checks")
                
                # Si on a des checks, on doit retourner leurs résultats
                if current_model_checks:
                    check_results = []
                    
                    context.log.info(f"🔍 Looking for evaluation events for model: {current_model_name}")
                    context.log.info(f"🔍 Found {len(evaluation_events)} evaluation events")
                    
                    for event in evaluation_events:
                        if event.get('event_type') == 'update_snapshot_evaluation':
                            snapshot_name = event.get('snapshot_name')
                            context.log.info(f"🔍 Checking snapshot: {snapshot_name}")
                            
                            if snapshot_name:
                                parts = snapshot_name.split('"."')
                                if len(parts) >= 3:
                                    snapshot_model_name = parts[1] + '.' + parts[2].replace('"', '')
                                    if snapshot_model_name == current_model_name:
                                        num_audits_passed = event.get('num_audits_passed', 0)
                                        num_audits_failed = event.get('num_audits_failed', 0)
                                        
                                        for check in current_model_checks:
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
                    
                    if not check_results:
                        context.log.warning(f"⚠️ No evaluation events found for model {current_model_name}, using default check results")
                        for check in current_model_checks:
                            check_results.append(
                                AssetCheckResult(
                                    check_name=check.name,
                                    passed=True,
                                    metadata={
                                        "note": "No evaluation events found, using default result"
                                    }
                                )
                            )
                    
                    context.log.info(f"🔍 DEBUG: Returning {len(check_results)} check results")
                    return MaterializeResult(
                        asset_key=current_asset_spec.key,
                        metadata={
                            "status": "success"
                        },
                        check_results=check_results
                    )
                else:
                    context.log.info(f"🔍 DEBUG: No checks defined, returning simple MaterializeResult")
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
    # Force run_retries=false to prevent infinite loops with SQLMesh audit failures
    sqlmesh_job = define_asset_job(
        name="sqlmesh_job",
        selection=sqlmesh_assets,  # Pass the list of assets directly
        tags={
            "dagster/max_retries": "0",
            "dagster/retry_on_asset_or_op_failure": "false"
        }
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
    translator: Optional[SQLMeshTranslator] = None,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    owners: Optional[List[str]] = None,
    schedule_name: str = "sqlmesh_adaptive_schedule",
    enable_schedule: bool = False,  # ← NOUVEAU : Désactiver le schedule par défaut
):
    """
    All-in-one factory to create a complete SQLMesh integration with Dagster.
    
    Args:
        project_dir: SQLMesh project directory
        gateway: SQLMesh gateway (postgres, duckdb, etc.)
        concurrency_limit: Concurrency limit
        translator: Custom translator for asset keys
        name: Multi-asset name
        group_name: Default group for assets
        op_tags: Operation tags
        owners: Asset owners
        schedule_name: Adaptive schedule name
        enable_schedule: Whether to enable the adaptive schedule (default: False)
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
    )
    
    # Create SQLMesh results resource for sharing between assets
    sqlmesh_results_resource = SQLMeshResultsResource()
    
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
        owners=owners,
    )
    
    # Create adaptive schedule and job (only if enabled)
    schedules = []
    jobs = []
    
    if enable_schedule:
        sqlmesh_adaptive_schedule, sqlmesh_job, _ = sqlmesh_adaptive_schedule_factory(
            sqlmesh_resource=sqlmesh_resource,
            name=schedule_name
        )
        schedules.append(sqlmesh_adaptive_schedule)
        jobs.append(sqlmesh_job)
    
    # Return complete Definitions
    return Definitions(
        assets=sqlmesh_assets,
        jobs=jobs,
        schedules=schedules,
        resources={
            "sqlmesh": sqlmesh_resource,
            "sqlmesh_results": sqlmesh_results_resource,
        },
    ) 