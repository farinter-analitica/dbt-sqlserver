from typing import Sequence, Set, Dict, List, Optional, Tuple
import dagster as dg
from dagster_shared_gf.resources.correo_e import EmailSenderResource
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.shared_variables import env_str, default_timezone_teg, tags_repo
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


running_default_schedule_status: dg.DefaultSensorStatus = get_for_current_env(
    {
        "local": dg.DefaultSensorStatus.STOPPED,
        "dev": dg.DefaultSensorStatus.RUNNING,
        "prd": dg.DefaultSensorStatus.RUNNING,
    }
)
stopped_default_schedule_status: dg.DefaultSensorStatus = get_for_current_env(
    {
        "local": dg.DefaultSensorStatus.STOPPED,
        "dev": dg.DefaultSensorStatus.STOPPED,
        "prd": dg.DefaultSensorStatus.STOPPED,
    }
)
only_prd_default_schedule_status: dg.DefaultSensorStatus = get_for_current_env(
    {
        "local": dg.DefaultSensorStatus.STOPPED,
        "dev": dg.DefaultSensorStatus.STOPPED,
        "prd": dg.DefaultSensorStatus.RUNNING,
    }
)
only_dev_default_schedule_status: dg.DefaultSensorStatus = get_for_current_env(
    {
        "local": dg.DefaultSensorStatus.STOPPED,
        "dev": dg.DefaultSensorStatus.RUNNING,
        "prd": dg.DefaultSensorStatus.STOPPED,
    }
)

DEFAULT_EMAILS = (
    ["brian.padilla@farinter.com", "david.saravia@farinter.com"]
    if env_str in ["prd", "dev"]
    else ["brian.padilla@farinter.com"]
)
DEFAULT_RUN_FAILURE_EMAILS = [
    "brian.padilla@farinter.com",
    "edwin.martinez@farinter.com",
    "david.saravia@farinter.com",
]  # lista para fallos de run sin asset

# Etiqueta para filtrar activos que no deben notificar en caso de fallo
FILTER_OUT_TAG = tags_repo.IgnorarNotificacionFallo


# ----------------- Helpers reutilizables -----------------


def _format_run_failure_email_simple(
    run_id: str,
    job_name: Optional[str],
    run_start_iso: Optional[str],
    run_end_iso: Optional[str],
    initiator: Optional[str] = None,
    error: Optional[str] = None,
) -> tuple[str, str]:
    """Formato simple para fallos de run sin asset identificado."""
    subject = f"[analiticastetl][Run Failure][{env_str}] {job_name or 'N/A'} - {initiator or 'N/A'} - Run {run_id}"
    body_parts = [
        f"Run id: {run_id}",
        f"Job: {job_name or 'N/A'}",
        f"Hora inicio (UTC): {run_start_iso or 'N/A'}",
        f"Hora fin (UTC): {run_end_iso or 'N/A'}",
        "No se identificó un asset específico asociado al fallo (RUN_FAILURE).",
    ]
    body = "\n".join(body_parts)
    if initiator:
        body = f"Iniciador: {initiator}\n\n" + body
    if error:
        body = f"Error: {error}\n\n" + body
    return subject, body


def _get_run_initiator(run: dg.DagsterRun) -> Optional[str]:
    """Intenta determinar quién/cuál inició el run usando etiquetas estándar de Dagster.

    Retorna una cadena corta como 'sensor:NAME', 'schedule:NAME', 'user:EMAIL',
    'tick:ID', 'external_source:VALUE' o None cuando es desconocido.
    """
    try:
        from dagster._core.storage.tags import (
            SCHEDULE_NAME_TAG,
            SENSOR_NAME_TAG,
            TICK_ID_TAG,
            EXTERNAL_JOB_SOURCE_TAG_KEY,
            AUTOMATION_CONDITION_TAG,
            USER_TAG,
        )

        tags = run.tags or {}
        # sensor tiene precedencia sobre schedule
        if tags.get(SENSOR_NAME_TAG):
            return f"sensor:{tags.get(SENSOR_NAME_TAG)}"
        if tags.get(SCHEDULE_NAME_TAG):
            return f"schedule:{tags.get(SCHEDULE_NAME_TAG)}"
        if tags.get(TICK_ID_TAG):
            return f"tick:{tags.get(TICK_ID_TAG)}"
        if tags.get(EXTERNAL_JOB_SOURCE_TAG_KEY):
            return f"external_source:{tags.get(EXTERNAL_JOB_SOURCE_TAG_KEY)}"
        # automation condition es etiqueta booleana; si está presente, marcar como automation
        if tags.get(AUTOMATION_CONDITION_TAG) == "true":
            return "automation"
        # user tags: tanto legacy 'user' como 'dagster/user'
        if tags.get(USER_TAG):
            return f"user:{tags.get(USER_TAG)}"
        if tags.get("dagster/user"):
            return f"user:{tags.get('dagster/user')}"
    except Exception:
        pass
    return None


def get_asset_owners(
    asset_key: dg.AssetKey, context: dg.RunFailureSensorContext
) -> Sequence[str]:
    """Obtener dueños de un activo desde la definición del repositorio (si existe)."""
    repo_ref = context.repository_def
    if not repo_ref:
        return []
    asset_metadata = repo_ref.assets_defs_by_key.get(asset_key)
    if not asset_metadata:
        return []
    owners = asset_metadata.owners_by_key.get(asset_key)
    return owners if owners else []


def _extract_failed_asset_keys(
    event: dg.DagsterEvent,
    repo_ref: dg.RepositoryDefinition,
    job_def: Optional[dg.JobDefinition],
    run_asset_selection: Optional[Set[dg.AssetKey]],
) -> Set[dg.AssetKey]:
    """Extraer claves de activos fallidos desde el evento, filtrando por selección del run."""
    result: Set[dg.AssetKey] = set()

    # Para eventos que tienen asset_key directamente
    if event.event_type in (
        dg.DagsterEventType.ASSET_FAILED_TO_MATERIALIZE,
        dg.DagsterEventType.ASSET_MATERIALIZATION,
        dg.DagsterEventType.ASSET_OBSERVATION,
        dg.DagsterEventType.ASSET_MATERIALIZATION_PLANNED,
    ):
        ak = event.asset_key
        if ak and isinstance(ak, dg.AssetKey):
            if not run_asset_selection or ak in run_asset_selection:
                result.add(ak)

    # Para STEP_FAILURE, usar node_handle para obtener activos
    elif event.event_type == dg.DagsterEventType.STEP_FAILURE:
        if event.node_handle and job_def:
            keys = job_def.asset_layer.get_selected_entity_keys_for_node(
                event.node_handle
            )
            for k in keys or []:
                if isinstance(k, dg.AssetKey):
                    if not run_asset_selection or k in run_asset_selection:
                        result.add(k)
                elif isinstance(k, dg.AssetCheckKey):
                    if not run_asset_selection or k.asset_key in run_asset_selection:
                        result.add(k.asset_key)

    return result


def _resolve_downstream_top_k(
    repo_ref: dg.RepositoryDefinition,
    asset_keys: Set[dg.AssetKey],
    run_asset_selection: Optional[Set[dg.AssetKey]],
    k: int = 10,
) -> Tuple[List[dg.AssetKey], int]:
    """Resolver downstream activos, limitando a la selección del run."""
    seen: List[dg.AssetKey] = []
    seen_set: Set[dg.AssetKey] = set()
    for ak in asset_keys:
        sel = dg.AssetSelection.assets(ak).downstream()
        for d in sel.resolve(repo_ref.asset_graph):
            if d not in seen_set and (
                not run_asset_selection or d in run_asset_selection
            ):
                seen_set.add(d)
                seen.append(d)
    more = max(0, len(seen) - k)
    return seen[:k], more


def _collect_owners_map(
    asset_keys: Sequence[dg.AssetKey], context: dg.RunFailureSensorContext
) -> Dict[dg.AssetKey, List[str]]:
    owners_map: Dict[dg.AssetKey, List[str]] = {}
    for ak in asset_keys:
        owners = get_asset_owners(ak, context) or []
        owners_map[ak] = list(owners)
    return owners_map


def _filter_asset_keys_by_tag(
    asset_keys: Set[dg.AssetKey],
    context: dg.RunFailureSensorContext,
    filter_tag: dict[str, str],
) -> Set[dg.AssetKey]:
    """Filtrar activos que contienen la etiqueta especificada."""
    filtered: Set[dg.AssetKey] = set()
    repo_ref = context.repository_def
    if not repo_ref:
        return asset_keys

    for ak in asset_keys:
        asset_def = repo_ref.assets_defs_by_key.get(ak)
        if asset_def:
            tags = asset_def.tags_by_key.get(ak, {})
            # Verificar si alguna etiqueta de filtro está presente en las etiquetas del activo
            has_filter_tag = any(
                k in tags and tags[k] == v for k, v in filter_tag.items()
            )
            if not has_filter_tag:
                filtered.add(ak)
        else:
            # Si no hay definición, incluir por defecto
            filtered.add(ak)
    return filtered


def _format_email_spanish(
    run_id: str,
    job_names: str,
    failed_assets: Set[dg.AssetKey],
    downstream_list: List[dg.AssetKey],
    more_count: int,
    owners_map: Dict[dg.AssetKey, List[str]],
    run_start_iso: Optional[str] = None,
    run_end_iso: Optional[str] = None,
    initiator: Optional[str] = None,
    error: Optional[str] = None,
) -> Tuple[str, str]:
    failed_lines = "\n".join(
        f"- {ak.to_user_string()}"
        for ak in sorted(failed_assets, key=lambda x: x.to_user_string())
    )
    downstream_lines = []
    for da in downstream_list:
        owners = owners_map.get(da) or []
        owners_str = ", ".join(owners) if owners else "Sin dueño definido."
        downstream_lines.append(f"- {da.to_user_string()}: {owners_str}")
    more_line = (
        f"\nAdemás hay {more_count} activos descendentes adicionales no listados."
        if more_count
        else ""
    )
    # Incluir fecha/hora de inicio y fin del run en el cuerpo
    run_start_line = (
        f"Hora inicio run ({default_timezone_teg}): {run_start_iso}\n"
        if run_start_iso
        else ""
    )
    run_end_line = (
        f"Hora fin run ({default_timezone_teg}): {run_end_iso}\n\n"
        if run_end_iso
        else ""
    )

    # Si se proporcionó iniciador, incluirlo al inicio del cuerpo
    initiator_line = f"Iniciador: {initiator}\n\n" if initiator else ""

    body = (
        f"{initiator_line}Se ha producido un fallo en la(s) materialización(es) del/los activo(s):\n"
        f"{failed_lines}\n\n"
        f"Run id: {run_id}\n"
        f"Job(s): {job_names or 'N/A'}\n"
        f"{run_start_line}{run_end_line}"
        f"Los siguientes activos descendentes no se ejecutarán (top {len(downstream_list)}):\n"
        f"{chr(10).join(downstream_lines)}{more_line}\n\n"
        f"Por favor, revise el error y tome las medidas necesarias."
        f"{f'Error: {error}' if error else ''}"
    )
    subject = f"[analiticastetl][Run Failure][{env_str}] {job_names or 'N/A'} - {initiator or 'N/A'} - Run {run_id}"
    return subject, body


@dg.run_failure_sensor(
    default_status=running_default_schedule_status,
    minimum_interval_seconds=get_for_current_env(
        {"local": 60 * 5, "dev": 60 * 5, "prd": 60 * 5}
    ),
)
def failed_asset_notification_sensor(
    context: dg.RunFailureSensorContext,
    enviador_correo_e_analitica_farinter: EmailSenderResource,
):
    from dagster._core.execution.plan.objects import StepFailureData

    failed_run = context.dagster_run
    failure_event = context.failure_event
    step_failure_events = context.get_step_failure_events()
    repo_ref = context.repository_def
    run_id = failed_run.run_id
    error = failure_event.message
    # Extraer mensajes de error detallados de eventos de fallo de pasos
    steps_messages = []
    for step in step_failure_events:
        # Verificar si el evento es de tipo STEP_FAILURE y tiene datos específicos
        if step.event_type == dg.DagsterEventType.STEP_FAILURE:
            if isinstance(step.event_specific_data, StepFailureData):
                error_info = step.event_specific_data.error
                if error_info and error_info.message:
                    steps_messages.append(error_info.message)
    errors_steps = ";/n".join(steps_messages)
    if errors_steps:
        error = f"{error};/n{errors_steps}"
    if failed_run.is_success:
        return

    # Obtener selección de activos del run para limitar el grafo
    run_asset_selection = (
        set(failed_run.asset_selection) if failed_run.asset_selection else None
    )

    rr_start_l = context.instance.get_run_records(
        dg.RunsFilter(run_ids=[run_id]), limit=1, order_by="start_time"
    )
    rr_end_l = context.instance.get_run_records(
        dg.RunsFilter(run_ids=[run_id]), limit=1, order_by="end_time"
    )
    rr_start = rr_start_l[0] if rr_start_l else None
    rr_end = rr_end_l[0] if rr_end_l else None

    failed_asset_keys: Set[dg.AssetKey] = set()
    # Extraer activos fallidos desde eventos de fallo de pasos
    job_name_primary = failed_run.job_name
    job_def = repo_ref.get_job(job_name_primary) if repo_ref else None

    if repo_ref:
        for ev in step_failure_events:
            keys = _extract_failed_asset_keys(
                ev, repo_ref, job_def, run_asset_selection
            )
            failed_asset_keys.update(keys)

    # Record failed assets before filtering
    has_failed_asset_keys_before_filter = len(failed_asset_keys) > 0

    # Filtrar activos que tienen la etiqueta de filtro
    if failed_asset_keys:
        failed_asset_keys = _filter_asset_keys_by_tag(
            failed_asset_keys, context, FILTER_OUT_TAG
        )

    # Tiempos del run
    rr_start_time = rr_start.start_time if rr_start else None
    rr_end_time = rr_end.end_time if rr_end else None
    # Normalizar zona horaria
    tzinfo_obj = default_timezone_teg
    if isinstance(tzinfo_obj, str):
        tzinfo_obj = ZoneInfo(tzinfo_obj)
    else:
        tzinfo_obj = ZoneInfo(str(tzinfo_obj))

    run_start_iso = (
        datetime.fromtimestamp(rr_start_time, tz=timezone.utc)
        .astimezone(tzinfo_obj)
        .isoformat()
        if rr_start_time
        else None
    )
    run_end_iso = (
        datetime.fromtimestamp(rr_end_time, tz=timezone.utc)
        .astimezone(tzinfo_obj)
        .isoformat()
        if rr_end_time
        else None
    )

    # Extraer posible iniciador del run
    try:
        run_initiator = _get_run_initiator(failed_run)
    except Exception:
        run_initiator = None

    if repo_ref and failed_asset_keys:
        # Resolver downstream limitando a la selección del run
        downstream_top, more_count = _resolve_downstream_top_k(
            repo_ref, failed_asset_keys, run_asset_selection, k=10
        )
        owners_map = _collect_owners_map(
            list(downstream_top) + list(failed_asset_keys), context
        )
        recipients: Set[str] = set(DEFAULT_EMAILS)
        for ak in failed_asset_keys:
            recipients.update(owners_map.get(ak, []))
        for da in downstream_top:
            recipients.update(owners_map.get(da, []))
        subject, body = _format_email_spanish(
            run_id=run_id,
            job_names=job_name_primary,
            failed_assets=failed_asset_keys,
            downstream_list=downstream_top,
            more_count=more_count,
            owners_map=owners_map,
            run_start_iso=run_start_iso,
            run_end_iso=run_end_iso,
            initiator=run_initiator,
            error=error,
        )
        enviador_correo_e_analitica_farinter.send_email(
            email_to=sorted(recipients), email_subject=subject, email_body=body
        )
        context.log.info(
            f"Notificación (assets) enviada a: {', '.join(sorted(recipients))} para run {run_id}"
        )
    elif not has_failed_asset_keys_before_filter:
        # Email simple de run solo si no hay activos fallidos antes del filtro (run sin assets o fallos no relacionados con assets)
        subject, body = _format_run_failure_email_simple(
            run_id=run_id,
            job_name=job_name_primary,
            run_start_iso=run_start_iso,
            run_end_iso=run_end_iso,
            initiator=run_initiator,
            error=error,
        )
        enviador_correo_e_analitica_farinter.send_email(
            email_to=sorted(DEFAULT_RUN_FAILURE_EMAILS),
            email_subject=subject,
            email_body=body,
        )
        context.log.info(
            f"Notificación (run simple) enviada a lista run-failure para run {run_id}"
        )


if __name__ == "__main__":
    # Ejecutar pytest en un proceso hijo y mostrar logs de Dagster
    import os
    import sys
    import subprocess

    os.environ.setdefault("DAGSTER_LOG_LEVEL", "DEBUG")
    env = os.environ.copy()
    env.setdefault("DAGSTER_LOG_LEVEL", "DEBUG")
    env.setdefault("JUPYTER_PLATFORM_DIRS", "1")

    pytest_args = [
        "-q",
        "-k",
        "shared_failed_sensors",
        "dagster-shared-gf/dagster_shared_gf_tests/test_shared_failed_sensors.py",
        "-ra",
        "-s",
    ]

    cmd = [sys.executable, "-m", "pytest"] + pytest_args

    try:
        proc = subprocess.run(cmd, env=env)
        rc = proc.returncode
    except Exception as exc:  # pragma: no cover - tooling/runtime
        print("Failed to spawn pytest process:", exc, file=sys.stderr)
        rc = 2

    sys.exit(rc)
