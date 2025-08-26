from typing import Sequence, Set, Dict, List, Optional, Tuple
import dagster as dg
from dagster_shared_gf.resources.correo_e import EmailSenderResource
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.shared_variables import env_str, default_timezone_teg
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


# ----------------- Helpers reutilizables -----------------


def _filter_failed_runs_since(
    context: dg.SensorEvaluationContext, since: datetime
) -> List[object]:
    """Obtiene runs que han fallado (status=FAILURE) actualizados después de 'since'.

    NOTA: aún no se usa en el flujo principal; se integrará en refactor posterior.
    """
    from dagster._core.storage.dagster_run import RunsFilter  # type: ignore

    records = context.instance.get_run_records(
        filters=RunsFilter(statuses=[dg.DagsterRunStatus.FAILURE], updated_after=since)
    )
    return list(records)


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
    """Try to determine who/what initiated the run using standard Dagster tags.

    Returns a short string like 'sensor:NAME', 'schedule:NAME', 'user:EMAIL',
    'tick:ID', 'external_source:VALUE' or None when unknown.
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

        tags = getattr(run, "tags", {}) or {}
        # sensor takes precedence over schedule
        if tags.get(SENSOR_NAME_TAG):
            return f"sensor:{tags.get(SENSOR_NAME_TAG)}"
        if tags.get(SCHEDULE_NAME_TAG):
            return f"schedule:{tags.get(SCHEDULE_NAME_TAG)}"
        if tags.get(TICK_ID_TAG):
            return f"tick:{tags.get(TICK_ID_TAG)}"
        if tags.get(EXTERNAL_JOB_SOURCE_TAG_KEY):
            return f"external_source:{tags.get(EXTERNAL_JOB_SOURCE_TAG_KEY)}"
        # automation condition is boolean-like tag; if present, mark as automation
        if tags.get(AUTOMATION_CONDITION_TAG) == "true":
            return "automation"
        # user tags: both legacy 'user' and 'dagster/user'
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
) -> Set[dg.AssetKey]:
    # Preferir asset_key explícito del evento y normalizar a AssetKey
    if getattr(event, "asset_key", None):
        ak = event.asset_key
        underlying = getattr(ak, "asset_key", None)
        if underlying:
            ak = underlying
        if isinstance(ak, dg.AssetKey):
            return {ak}
        try:
            return {dg.AssetKey(str(ak))}
        except Exception:
            return set()

    # Fallback: intentar resolver desde node_handle cuando esté disponible
    try:
        dag_event = event
        node_handle = getattr(dag_event, "node_handle", None) if dag_event else None
        if node_handle and job_def:
            keys = job_def.asset_layer.get_selected_entity_keys_for_node(node_handle)
            result: Set[dg.AssetKey] = set()
            for k in keys or []:
                try:
                    base = getattr(k, "asset_key", None) or k
                    if isinstance(base, dg.AssetKey):
                        result.add(base)
                    else:
                        result.add(dg.AssetKey(str(base)))
                except Exception:
                    continue
            return result
    except Exception:
        return set()
    return set()


def _resolve_downstream_top_k(
    repo_ref: dg.RepositoryDefinition, asset_keys: Set[dg.AssetKey], k: int = 10
) -> Tuple[List[dg.AssetKey], int]:
    seen: List[dg.AssetKey] = []
    seen_set: Set[dg.AssetKey] = set()
    for ak in asset_keys:
        sel = dg.AssetSelection.assets(ak).downstream()
        for d in sel.resolve(repo_ref.asset_graph):
            if d not in seen_set:
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
    failed_run = context.dagster_run
    failure_event = context.failure_event
    step_failure_events = context.get_step_failure_events()
    repo_ref = context.repository_def
    run_id = failed_run.run_id
    error = failure_event.message
    steps_messages = [step.message for step in step_failure_events if step.message]
    errors_steps = ";/n".join(steps_messages)
    if errors_steps is not None:
        error = f"{error};/n{errors_steps}"
    if failed_run.is_success:
        return

    rr_start_l = context.instance.get_run_records(
        dg.RunsFilter(run_ids=[run_id]), limit=1, order_by="start_time"
    )
    rr_end_l = context.instance.get_run_records(
        dg.RunsFilter(run_ids=[run_id]), limit=1, order_by="end_time"
    )
    rr_start = rr_start_l[0] if rr_start_l else None
    rr_end = rr_end_l[0] if rr_end_l else None

    failed_asset_keys: Set[dg.AssetKey] = set()
    # Primero intento directo desde STEP_FAILURE
    job_name_primary = failed_run.job_name
    job_def = repo_ref.get_job(job_name_primary) if repo_ref else None

    if repo_ref:
        for ev in step_failure_events:
            keys = _extract_failed_asset_keys(ev, repo_ref, job_def)
            failed_asset_keys.update(keys)

    # Fallback ligero: si hubo STEP_FAILURE pero no derivamos asset keys, mirar último ASSET_CHECK_EVALUATION
    if step_failure_events and not failed_asset_keys:
        try:
            check_conn = context.instance.event_log_storage.get_records_for_run(  # type: ignore
                run_id, of_type=dg.DagsterEventType.ASSET_CHECK_EVALUATION
            )
            check_records = getattr(check_conn, "records", [])
            for rec in reversed(check_records):  # del más reciente hacia atrás
                try:
                    dag_ev = rec.event_log_entry.dagster_event  # type: ignore
                    ak = getattr(dag_ev, "asset_key", None)
                    if ak:
                        base = getattr(ak, "asset_key", None) or ak
                        if isinstance(base, dg.AssetKey):
                            failed_asset_keys.add(base)
                        else:
                            failed_asset_keys.add(dg.AssetKey(str(base)))
                        break
                except Exception:
                    continue
        except Exception:
            pass
    # Tiempos run
    rr_start_time = rr_start.start_time if rr_start else None
    rr_end_time = rr_end.end_time if rr_end else None
    # Normalizar default_timezone_teg a un tzinfo (acepta ya sea un objeto tzinfo o un string tipo "UTC")
    tzinfo_obj = default_timezone_teg
    # si viene como string, crear ZoneInfo; si ya es tzinfo (tiene utcoffset), usarlo
    if isinstance(tzinfo_obj, str):
        tzinfo_obj = ZoneInfo(tzinfo_obj)
    elif not hasattr(tzinfo_obj, "utcoffset"):
        # desconocido, intentar crear ZoneInfo desde su representación string
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

    # extraer posible iniciador/lanzador del run (sensor, schedule, user, external)
    try:
        run_initiator = _get_run_initiator(failed_run)
    except Exception:
        run_initiator = None

    if repo_ref and failed_asset_keys:
        downstream_top, more_count = _resolve_downstream_top_k(
            repo_ref, failed_asset_keys, k=10
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
    else:
        # Email simple de run
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
