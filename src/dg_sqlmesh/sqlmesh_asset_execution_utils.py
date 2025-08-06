"""
Utilitaires pour l'exécution des assets SQLMesh.
Contient les fonctions extraites de la fonction model_asset pour améliorer la lisibilité et la testabilité.
"""

from dagster import AssetExecutionContext, MaterializeResult, AssetCheckResult, AssetKey
from typing import Dict, List, Any, Tuple
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import get_models_to_materialize


def execute_sqlmesh_materialization(
    context: AssetExecutionContext,
    sqlmesh: SQLMeshResource,
    sqlmesh_results: Any,
    run_id: str,
    selected_asset_keys: List[AssetKey],
) -> Dict[str, Any]:
    """
    Exécute la matérialisation SQLMesh pour tous les assets sélectionnés.

    Args:
        context: Contexte Dagster
        sqlmesh: Resource SQLMesh
        sqlmesh_results: Resource pour partager les résultats
        run_id: ID du run Dagster
        selected_asset_keys: Assets sélectionnés

    Returns:
        Résultats de l'exécution SQLMesh
    """
    context.log.info(
        "🚀 First asset in run, launching SQLMesh execution for all selected assets"
    )
    context.log.debug(f"🔍 No existing results for run {run_id}")

    context.log.info(f"🔍 Selected assets in this run: {selected_asset_keys}")

    # Lancer une seule exécution SQLMesh pour tous les assets sélectionnés
    models_to_materialize = get_models_to_materialize(
        selected_asset_keys,
        sqlmesh.get_models,
        sqlmesh.translator,
    )

    if not models_to_materialize:
        raise Exception(f"No models found for selected assets: {selected_asset_keys}")

    context.log.info(
        f"🔍 Materializing {len(models_to_materialize)} models: {[m.name for m in models_to_materialize]}"
    )

    # Exécution SQLMesh unique
    context.log.debug("🔍 Starting SQLMesh materialization...")
    plan = sqlmesh.materialize_assets_threaded(models_to_materialize, context=context)
    context.log.debug("🔍 SQLMesh materialization completed")

    # Capturer tous les résultats
    context.log.debug("🔍 Processing failed models events...")
    failed_check_results = sqlmesh._process_failed_models_events()
    context.log.debug(f"🔍 Failed check results count: {len(failed_check_results)}")

    context.log.debug("🔍 Processing skipped models events...")
    skipped_models_events = sqlmesh._console.get_skipped_models_events()
    context.log.debug(f"🔍 Skipped models events count: {len(skipped_models_events)}")

    context.log.debug("🔍 Processing evaluation events...")
    evaluation_events = sqlmesh._console.get_evaluation_events()
    context.log.debug(f"🔍 Evaluation events count: {len(evaluation_events)}")

    # Stocker les résultats dans le resource partagé
    results = {
        "failed_check_results": failed_check_results,
        "skipped_models_events": skipped_models_events,
        "evaluation_events": evaluation_events,
        "plan": plan,
    }

    sqlmesh_results.store_results(run_id, results)
    context.log.info(f"💾 Stored SQLMesh results for run {run_id}")

    return results


def process_sqlmesh_results(
    context: AssetExecutionContext, sqlmesh_results: Any, run_id: str
) -> Tuple[List[AssetCheckResult], List[Dict], List[Dict]]:
    """
    Récupère et traite les résultats SQLMesh partagés.

    Args:
        context: Contexte Dagster
        sqlmesh_results: Resource pour partager les résultats
        run_id: ID du run Dagster

    Returns:
        Tuple de (failed_check_results, skipped_models_events, evaluation_events)
    """
    context.log.info(f"📋 Using existing SQLMesh results from run {run_id}")
    context.log.debug(f"🔍 Found existing results for run {run_id}")

    # Récupérer les résultats pour ce run
    results = sqlmesh_results.get_results(run_id)
    failed_check_results = results["failed_check_results"]
    skipped_models_events = results["skipped_models_events"]
    evaluation_events = results["evaluation_events"]

    context.log.debug("🔍 Processing results for model")
    context.log.debug(f"🔍 Failed check results: {len(failed_check_results)}")
    context.log.debug(f"🔍 Skipped models events: {len(skipped_models_events)}")
    context.log.debug(f"🔍 Evaluation events: {len(evaluation_events)}")

    return failed_check_results, skipped_models_events, evaluation_events


def check_model_status(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    failed_check_results: List[AssetCheckResult],
    skipped_models_events: List[Dict],
) -> Tuple[bool, bool]:
    """
    Vérifie le statut d'un modèle spécifique.

    Args:
        context: Contexte Dagster
        current_model_name: Nom du modèle actuel
        current_asset_spec: Spécification de l'asset
        failed_check_results: Résultats d'audit échoués
        skipped_models_events: Événements de modèles ignorés

    Returns:
        Tuple de (model_was_skipped, model_has_audit_failures)
    """
    model_was_skipped = False
    model_has_audit_failures = False

    # Vérifier les skips à cause d'échecs upstream
    context.log.debug("🔍 Checking for skipped models...")
    for event in skipped_models_events:
        skipped_snapshots = event.get("snapshot_names", set())
        context.log.debug(f"🔍 Skipped snapshots: {skipped_snapshots}")

        for snapshot_name in skipped_snapshots:
            if snapshot_name:
                parts = snapshot_name.split('"."')
                if len(parts) >= 3:
                    skipped_model_name = parts[1] + "." + parts[2].replace('"', "")
                    context.log.debug(
                        f"🔍 Checking skipped model: {skipped_model_name} vs {current_model_name}"
                    )
                    if skipped_model_name == current_model_name:
                        model_was_skipped = True
                        context.log.error(
                            f"❌ Model {current_model_name} was skipped due to upstream failures"
                        )
                        break
        if model_was_skipped:
            break

    # Vérifier les échecs d'audit (modèle exécuté mais audit failed)
    context.log.debug("🔍 Checking for audit failures...")
    for check_result in failed_check_results:
        context.log.debug(
            f"🔍 Checking failed check: {check_result.asset_key} vs {current_asset_spec.key}"
        )
        if check_result.asset_key == current_asset_spec.key:
            model_has_audit_failures = True
            context.log.error(
                f"❌ Model {current_model_name} has audit failures: {check_result.metadata.get('audit_message', 'Unknown error')}"
            )
            break

    context.log.debug(
        f"🔍 Model {current_model_name} - was_skipped: {model_was_skipped}, has_audit_failures: {model_has_audit_failures}"
    )

    return model_was_skipped, model_has_audit_failures


def handle_audit_failures(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    current_model_checks: List[Any],
    failed_check_results: List[AssetCheckResult],
) -> MaterializeResult:
    """
    Gère les cas où le modèle s'est exécuté mais les audits ont échoué.

    Args:
        context: Contexte Dagster
        current_model_name: Nom du modèle
        current_asset_spec: Spécification de l'asset
        current_model_checks: Checks du modèle
        failed_check_results: Résultats d'audit échoués

    Returns:
        MaterializeResult avec les checks échoués
    """
    context.log.info(
        f"⚠️ Model {current_model_name}: MATERIALIZATION SUCCESS but AUDIT FAILED"
    )
    context.log.debug("🔍 Returning MaterializeResult with failed checks")

    # Si on a des checks, on doit retourner leurs résultats
    if current_model_checks:
        check_results = []

        # Créer des AssetCheckResult failed pour tous les checks
        for check in current_model_checks:
            # Trouver le message d'erreur spécifique pour ce check
            audit_message = "Model materialization succeeded but audits failed"
            for check_result in failed_check_results:
                if check_result.asset_key == current_asset_spec.key:
                    audit_message = check_result.metadata.get(
                        "audit_message", audit_message
                    )
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
                    "error_details": f"SQLMesh audit '{check.name}' failed: {audit_message}",
                },
            )
            check_results.append(check_result)
            context.log.debug(
                f"🔍 Created failed check result for: {check.name} with message: {audit_message}"
            )

        context.log.debug(f"🔍 Returning {len(check_results)} failed check results")
        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "materialization_success_audit_failed"},
            check_results=check_results,
        )
    else:
        context.log.warning(
            f"⚠️ No checks defined for model {current_model_name}, returning only MaterializeResult"
        )
        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "materialization_success_audit_failed"},
        )


def handle_successful_execution(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    current_model_checks: List[Any],
    evaluation_events: List[Dict],
) -> MaterializeResult:
    """
    Gère les cas où le modèle s'est exécuté avec succès.

    Args:
        context: Contexte Dagster
        current_model_name: Nom du modèle
        current_asset_spec: Spécification de l'asset
        current_model_checks: Checks du modèle
        evaluation_events: Événements d'évaluation

    Returns:
        MaterializeResult avec les checks réussis
    """
    context.log.info(f"✅ Model {current_model_name}: SUCCESS")
    context.log.debug("🔍 Returning MaterializeResult with passed checks")

    # Si on a des checks, on doit retourner leurs résultats
    if current_model_checks:
        check_results = []

        context.log.info(
            f"🔍 Looking for evaluation events for model: {current_model_name}"
        )
        context.log.info(f"🔍 Found {len(evaluation_events)} evaluation events")

        for event in evaluation_events:
            if event.get("event_type") == "update_snapshot_evaluation":
                snapshot_name = event.get("snapshot_name")
                context.log.info(f"🔍 Checking snapshot: {snapshot_name}")

                if snapshot_name:
                    parts = snapshot_name.split('"."')
                    if len(parts) >= 3:
                        snapshot_model_name = parts[1] + "." + parts[2].replace('"', "")
                        if snapshot_model_name == current_model_name:
                            num_audits_passed = event.get("num_audits_passed", 0)
                            num_audits_failed = event.get("num_audits_failed", 0)

                            for check in current_model_checks:
                                passed = num_audits_failed == 0
                                check_results.append(
                                    AssetCheckResult(
                                        check_name=check.name,
                                        passed=passed,
                                        metadata={
                                            "audits_passed": num_audits_passed,
                                            "audits_failed": num_audits_failed,
                                        },
                                    )
                                )
                            break

        if not check_results:
            context.log.warning(
                f"⚠️ No evaluation events found for model {current_model_name}, using default check results"
            )
            for check in current_model_checks:
                check_results.append(
                    AssetCheckResult(
                        check_name=check.name,
                        passed=True,
                        metadata={
                            "note": "No evaluation events found, using default result"
                        },
                    )
                )

        context.log.debug(f"🔍 Returning {len(check_results)} check results")
        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "success"},
            check_results=check_results,
        )
    else:
        context.log.debug("🔍 No checks defined, returning simple MaterializeResult")
        return MaterializeResult(
            asset_key=current_asset_spec.key, metadata={"status": "success"}
        )


def create_materialize_result(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    current_model_checks: List[Any],
    model_was_skipped: bool,
    model_has_audit_failures: bool,
    failed_check_results: List[AssetCheckResult],
    evaluation_events: List[Dict],
) -> MaterializeResult:
    """
    Crée le MaterializeResult approprié selon le statut du modèle.

    Args:
        context: Contexte Dagster
        current_model_name: Nom du modèle
        current_asset_spec: Spécification de l'asset
        current_model_checks: Checks du modèle
        model_was_skipped: Si le modèle a été ignoré
        model_has_audit_failures: Si le modèle a des échecs d'audit
        failed_check_results: Résultats d'audit échoués
        evaluation_events: Événements d'évaluation

    Returns:
        MaterializeResult approprié
    """
    if model_was_skipped:
        # Modèle skip → Lever une exception (pas de materialization)
        error_msg = f"Model {current_model_name} was skipped due to upstream failures"
        context.log.error(f"❌ {error_msg}")
        context.log.debug("🔍 Raising exception for skipped model")
        raise Exception(error_msg)
    elif model_has_audit_failures:
        return handle_audit_failures(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            failed_check_results,
        )
    else:
        return handle_successful_execution(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            evaluation_events,
        )
