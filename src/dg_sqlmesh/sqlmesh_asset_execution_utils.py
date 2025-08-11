"""
Utilitaires pour l'exécution des assets SQLMesh.
Contient les fonctions extraites de la fonction model_asset pour améliorer la lisibilité et la testabilité.
"""

import json
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    AssetCheckResult,
    AssetKey,
    AssetCheckSeverity,
)
from typing import Dict, List, Any, Tuple
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import get_models_to_materialize
from .sqlmesh_asset_check_utils import build_audit_check_metadata


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
    # Console removed → no legacy failed models events
    context.log.debug("🔍 Processing failed models events... (skipped, console disabled)")
    failed_check_results: List[AssetCheckResult] = []
    context.log.debug("🔍 Failed check results count: 0")

    context.log.debug("🔍 Processing skipped models events... (skipped, console disabled)")
    skipped_models_events: List[Dict] = []
    context.log.debug(f"🔍 Skipped models events count: {len(skipped_models_events)}")

    # No evaluation events (console disabled)
    evaluation_events: List[Dict] = []
    context.log.debug(f"🔍 Evaluation events count: {len(evaluation_events)}")

    # No non-blocking warnings (console disabled)
    non_blocking_audit_warnings: List[Dict] = []

    # Stocker les résultats dans le resource partagé
    # Capturer les échecs d'audits depuis le notifier (robuste)
    try:
        notifier = sqlmesh._get_or_create_notifier()
        notifier_audit_failures = notifier.get_audit_failures()
    except Exception:
        notifier_audit_failures = []
    # Log a compact summary to help debugging (avoid dumping SQL)
    try:
        summary = [
            {
                "model": f.get("model"),
                "audit": f.get("audit"),
                "blocking": f.get("blocking"),
                "count": f.get("count"),
            }
            for f in notifier_audit_failures
        ]
        context.log.info(f"🔎 Notifier audit failures summary: {summary}")
    except Exception:
        pass

    # Construire les AssetKey bloquants et les assets downstream affectés
    blocking_failed_asset_keys = []
    try:
        for fail in notifier_audit_failures:
            if fail.get("blocking") and fail.get("model"):
                model = sqlmesh.context.get_model(fail.get("model"))
                if model:
                    blocking_failed_asset_keys.append(sqlmesh.translator.get_asset_key(model))
    except Exception:
        pass

    try:
        affected_downstream_asset_keys = sqlmesh._get_affected_downstream_assets(
            blocking_failed_asset_keys
        )
    except Exception:
        affected_downstream_asset_keys = set()
    # Ensure we don't include the failing assets themselves in the downstream set
    try:
        affected_downstream_asset_keys = set(affected_downstream_asset_keys) - set(
            blocking_failed_asset_keys
        )
    except Exception:
        affected_downstream_asset_keys = set(affected_downstream_asset_keys)
    try:
        context.log.info(
            f"🔎 Blocking failed assets: {blocking_failed_asset_keys} | Downstream affected: {list(affected_downstream_asset_keys)}"
        )
    except Exception:
        pass

    results = {
        "failed_check_results": failed_check_results,
        "skipped_models_events": skipped_models_events,
        "non_blocking_audit_warnings": non_blocking_audit_warnings,
        "notifier_audit_failures": notifier_audit_failures,
        "affected_downstream_asset_keys": list(affected_downstream_asset_keys),
        "plan": plan,
    }

    sqlmesh_results.store_results(run_id, results)
    context.log.info(f"💾 Stored SQLMesh results for run {run_id}")

    return results


def process_sqlmesh_results(
    context: AssetExecutionContext, sqlmesh_results: Any, run_id: str
) -> Tuple[List[AssetCheckResult], List[Dict], List[Dict], List[Dict], List[AssetKey]]:
    """
    Récupère et traite les résultats SQLMesh partagés.

    Args:
        context: Contexte Dagster
        sqlmesh_results: Resource pour partager les résultats
        run_id: ID du run Dagster

    Returns:
        Tuple de (
            failed_check_results,
            skipped_models_events,
            non_blocking_audit_warnings,
            notifier_audit_failures,
            affected_downstream_asset_keys,
        )
    """
    context.log.info(f"📋 Using existing SQLMesh results from run {run_id}")
    context.log.debug(f"🔍 Found existing results for run {run_id}")

    # Récupérer les résultats pour ce run
    results = sqlmesh_results.get_results(run_id)
    failed_check_results = results["failed_check_results"]
    skipped_models_events = results["skipped_models_events"]
    non_blocking_audit_warnings = results.get("non_blocking_audit_warnings", [])
    notifier_audit_failures = results.get("notifier_audit_failures", [])
    affected_downstream_asset_keys = results.get("affected_downstream_asset_keys", [])

    context.log.debug("🔍 Processing results for model")
    context.log.debug(f"🔍 Failed check results: {len(failed_check_results)}")
    context.log.debug(f"🔍 Skipped models events: {len(skipped_models_events)}")
    context.log.debug(
        f"🔍 Non-blocking audit warnings: {len(non_blocking_audit_warnings)}"
    )
    context.log.debug(
        f"🔍 Notifier audit failures: {len(notifier_audit_failures)} | affected downstream: {len(affected_downstream_asset_keys)}"
    )

    return (
        failed_check_results,
        skipped_models_events,
        non_blocking_audit_warnings,
        notifier_audit_failures,
        affected_downstream_asset_keys,
    )


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
    non_blocking_audit_warnings: List[Dict],
    notifier_audit_failures: List[Dict],
) -> MaterializeResult:
    """
    Gère les cas où le modèle s'est exécuté avec succès.

    Args:
        context: Contexte Dagster
        current_model_name: Nom du modèle
        current_asset_spec: Spécification de l'asset
        current_model_checks: Checks du modèle
    Returns:
        MaterializeResult avec les checks réussis
    """
    context.log.info(f"✅ Model {current_model_name}: SUCCESS")
    context.log.debug("🔍 Returning MaterializeResult with passed checks")

    # Si on a des checks, on doit retourner leurs résultats
    if current_model_checks:
        check_results = []

        # Notifier-only: build from notifier and (legacy) console warnings

        if not check_results:
            context.log.warning(
                f"⚠️ No evaluation events found for model {current_model_name}, building results from notifier/console"
            )

            # Build failing set from notifier non-blocking and console warnings
            nb_audits_for_model = {
                w.get("audit_name")
                for w in non_blocking_audit_warnings
                if w.get("model_name") == current_model_name
            }
            for fail in notifier_audit_failures:
                if not fail.get("blocking") and fail.get("model") == current_model_name:
                    nb_audits_for_model.add(fail.get("audit"))

            # Build audit details lookup from SQLMesh model for PASS metadata
            audit_details_by_name: Dict[str, Dict] = {}
            try:
                sqlmesh_model = context.resources.sqlmesh.context.get_model(current_model_name)  # type: ignore[attr-defined]
                if sqlmesh_model and hasattr(sqlmesh_model, "audits_with_args"):
                    for audit_obj, audit_args in sqlmesh_model.audits_with_args:
                        try:
                            from .sqlmesh_asset_check_utils import extract_audit_details

                            details = extract_audit_details(
                                audit_obj, audit_args, sqlmesh_model, logger=getattr(context, "log", None)
                            )
                            audit_details_by_name[details["name"]] = details
                        except Exception:
                            continue
            except Exception:
                pass

            # Emit WARN failed for non-blocking failures, PASS for others
            for check in current_model_checks:
                if check.name in nb_audits_for_model:
                    # fetch details for richer metadata
                    fail = next(
                        (f for f in notifier_audit_failures if f.get("model") == current_model_name and f.get("audit") == check.name),
                        {},
                    )
                    check_results.append(
                        AssetCheckResult(
                            check_name=check.name,
                            passed=False,
                            severity=AssetCheckSeverity.WARN,
                            metadata={
                                "audit_blocking": False,
                                "sqlmesh_model_name": current_model_name,
                                "audit_query": fail.get("sql", "N/A"),
                                "audit_args": json.dumps(fail.get("args", {}), default=str),
                                "audit_count": fail.get("count", 0),
                            },
                        )
                    )
                else:
                    # Build PASS metadata via centralized builder
                    pass_meta = build_audit_check_metadata(
                        context=context.resources.sqlmesh.context if hasattr(context.resources, "sqlmesh") else None,  # type: ignore[attr-defined]
                        model_or_name=current_model_name,
                        audit_name=check.name,
                        logger=getattr(context, "log", None),
                    )
                    pass_meta.update({
                        "audits_passed": 1,
                        "audits_failed": 0,
                    })
                    check_results.append(
                        AssetCheckResult(
                            check_name=check.name,
                            passed=True,
                            metadata=pass_meta,
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
    non_blocking_audit_warnings: List[Dict],
    notifier_audit_failures: List[Dict],
    affected_downstream_asset_keys: List[AssetKey],
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
    Returns:
        MaterializeResult approprié
    """
    if model_was_skipped:
        # Modèle skip → Lever une exception (pas de materialization)
        error_msg = f"Model {current_model_name} was skipped due to upstream failures"
        context.log.error(f"❌ {error_msg}")
        context.log.debug("🔍 Raising exception for skipped model")
        raise Exception(error_msg)
    elif model_has_audit_failures or any(
        f.get("blocking") and f.get("model") == current_model_name
        for f in notifier_audit_failures
    ):
        context.log.info(
            f"🔶 Creating failed MaterializeResult for {current_model_name} due to blocking audit failure"
        )

        # Build precise check results: only the failing audits should fail
        failed_for_model = [
            f for f in notifier_audit_failures if f.get("model") == current_model_name
        ]
        blocking_names = {f.get("audit") for f in failed_for_model if f.get("blocking")}
        non_blocking_names = {f.get("audit") for f in failed_for_model if not f.get("blocking")}

        # Merge legacy console non-blocking warnings
        for w in non_blocking_audit_warnings:
            if w.get("model_name") == current_model_name:
                non_blocking_names.add(w.get("audit_name"))

        check_results: List[AssetCheckResult] = []
        for check in current_model_checks:
            if check.name in blocking_names:
                fail = next(
                    (f for f in failed_for_model if f.get("audit") == check.name),
                    {},
                )
                check_results.append(
                    AssetCheckResult(
                        check_name=check.name,
                        passed=False,
                        severity=AssetCheckSeverity.ERROR,
                        metadata={
                            "audit_message": "Model materialization succeeded but audits failed",
                            "sqlmesh_audit_name": check.name,
                            "sqlmesh_model": current_model_name,
                            "error_details": f"SQLMesh audit '{check.name}' failed",
                            "audit_blocking": True,
                            "audit_query": fail.get("sql", "N/A"),
                            "audit_args": json.dumps(fail.get("args", {}), default=str),
                        },
                    )
                )
            elif check.name in non_blocking_names:
                check_results.append(
                    AssetCheckResult(
                        check_name=check.name,
                        passed=False,
                        severity=AssetCheckSeverity.WARN,
                        metadata={
                            "audit_message": "Non-blocking audit failed",
                        },
                    )
                )
            else:
                check_results.append(
                    AssetCheckResult(
                        check_name=check.name,
                        passed=True,
                        metadata={
                            "audits_passed": 1,
                            "audits_failed": 0,
                        },
                    )
                )

        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "materialization_success_audit_failed"},
            check_results=check_results,
        )
    else:
        # If current asset is unaffected but is in affected downstream set, raise to block
        if current_asset_spec.key in set(affected_downstream_asset_keys):
            # bloquer en suivant le pattern upstream
            context.log.info(
                f"⛔ Blocking downstream materialization for {current_model_name} due to upstream failures"
            )
            raise Exception(
                f"Asset {current_asset_spec.key} skipped due to upstream audit failures"
            )

        return handle_successful_execution(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            non_blocking_audit_warnings,
            notifier_audit_failures,
        )
