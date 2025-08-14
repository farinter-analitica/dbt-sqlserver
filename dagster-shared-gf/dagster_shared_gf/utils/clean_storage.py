import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import yaml
import os
from typing import Callable

from dagster import OpExecutionContext, RunsFilter, graph, op
from dagster_shared_gf.shared_functions import get_for_current_env

SETTINGS = {
    "local_storage": {"retention_period": get_for_current_env({"dev": 40, "prd": 31})}
}
DAGSTER_HOME = os.environ.get("DAGSTER_HOME") or "."


@dataclass
class RetentionPolicy:
    base_now: datetime
    run_cutoff: datetime
    extended_cutoff: datetime
    protected_cutoff: datetime


def build_retention_policy(retention_days: float) -> RetentionPolicy:
    now = datetime.now()
    return RetentionPolicy(
        base_now=now,
        run_cutoff=now - timedelta(days=retention_days),
        extended_cutoff=now - timedelta(days=retention_days + 5),
        protected_cutoff=now - timedelta(days=retention_days + 10),
    )


def classify_basic(path: Path) -> str:
    """Clasificación básica de un path.

    Devuelve:
        - "protected" si el nombre empieza por '__'
        - "unrelated" en caso contrario
    """
    if path.name.startswith("__"):
        return "protected"
    return "unrelated"


@dataclass
class CleanupStats:
    normal_deleted: int = 0  # Archivos normales eliminados
    protected_deleted: int = 0  # Archivos/árboles protegidos eliminados
    run_dirs_deleted: int = 0  # Directorios de run eliminados (conteo por directorio)
    empty_dirs_removed: int = 0  # Directorios vacíos eliminados


def generic_cleanup(
    root: Path,
    context: OpExecutionContext,
    policy: RetentionPolicy,
    classify: Callable[[Path], str],
    remove_root_if_empty: bool,
) -> CleanupStats:
    """Limpieza genérica basada en una política de retención.

    Recorre el árbol desde 'root', determina qué archivos/directorios exceden sus umbrales
    de tiempo (según la clasificación) y los elimina. Permite eliminaciones parciales en
    directorios que mezclan contenido viejo y reciente, y luego elimina directorios vacíos.
    """
    stats = CleanupStats()
    deletable_dirs: list[tuple[Path, str, int]] = []
    files_to_delete: list[tuple[Path, str]] = []
    all_dirs: list[Path] = []

    def threshold_for(klass: str) -> datetime:
        if klass == "run":
            return policy.run_cutoff
        if klass == "protected":
            return policy.protected_cutoff
        return policy.extended_cutoff

    def scan(path: Path, inherited_protected: bool) -> tuple[bool, int]:
        """Escaneo recursivo.

        Retorna:
          (subarbol_totalmente_eliminable, numero_total_de_archivos_hoja)
        """
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
        except FileNotFoundError:
            return True, 0
        klass = classify(path)
        effective_klass = (
            "protected" if inherited_protected or klass == "protected" else klass
        )
        thr = threshold_for(effective_klass)
        if path.is_file():
            if effective_klass == "run":
                eff_thr = threshold_for("unrelated")
                if mtime < eff_thr:
                    files_to_delete.append((path, "unrelated"))
                    return True, 1
                return False, 1
            if mtime < thr:
                files_to_delete.append((path, effective_klass))
                return True, 1
            return False, 1
        # Directorio
        all_dirs.append(path)
        if effective_klass == "run":
            if mtime < thr:
                file_count = _count_files(path)
                deletable_dirs.append((path, "run", file_count))
                return True, file_count
            total_files = 0
            for child in path.iterdir():
                if child.is_file():
                    total_files += 1
                else:
                    total_files += _count_files(child)
            return False, total_files
        all_children_deletable = True
        total_files = 0
        for child in list(path.iterdir()):
            child_all, child_files = scan(
                child, inherited_protected or klass == "protected"
            )
            total_files += child_files
            if not child_all:
                all_children_deletable = False
        if all_children_deletable and mtime < thr:
            deletable_dirs.append((path, effective_klass, total_files))
            return True, total_files
        return False, total_files

    scan(root, inherited_protected=False)

    for dir_path, klass, file_count in sorted(
        deletable_dirs, key=lambda x: len(x[0].parts), reverse=True
    ):
        if not dir_path.exists():
            continue
        try:
            shutil.rmtree(dir_path)
            if klass == "protected":
                stats.protected_deleted += file_count
            elif klass == "run":
                stats.run_dirs_deleted += 1
            else:
                stats.normal_deleted += file_count
                context.log.debug(
                    f"Eliminado directorio: {dir_path} (tipo={klass}, archivos={file_count})"
                )
        except Exception as e:
            context.log.error(f"Error al eliminar directorio {dir_path}: {e}")

    deleted_dirs = {d[0] for d in deletable_dirs}
    for file_path, klass in files_to_delete:
        if any(parent in deleted_dirs for parent in file_path.parents):
            continue
        if not file_path.exists():
            continue
        try:
            file_path.unlink()
            if klass == "protected":
                stats.protected_deleted += 1
            else:
                stats.normal_deleted += 1
            context.log.debug(f"Eliminado archivo: {file_path} (tipo={klass})")
        except Exception as e:
            context.log.error(f"Error al eliminar archivo {file_path}: {e}")

    for d in sorted(all_dirs, key=lambda p: len(p.parts), reverse=True):
        if not d.exists():
            continue
        if is_directory_empty(d) and (d != root or remove_root_if_empty):
            try:
                d.rmdir()
                stats.empty_dirs_removed += 1
            except Exception:
                pass
    return stats


@op
def clean_dbt_targets_old_files(context: OpExecutionContext) -> None:
    dbt_project_path = Path(DAGSTER_HOME) / "dbt_dwh_farinter" / "dbt_project.yml"
    if not dbt_project_path.exists():
        raise FileNotFoundError(f"No se encontró dbt_project.yml en {dbt_project_path}")
    with dbt_project_path.open("r", encoding="utf-8") as f:
        dbt_config = yaml.safe_load(f)
    clean_targets = dbt_config.get("clean-targets", [])
    policy = build_retention_policy(
        float(SETTINGS["local_storage"]["retention_period"])
    )
    project_dir = dbt_project_path.parent
    total_stats = CleanupStats()
    for rel in clean_targets:
        root = project_dir / rel
        if not (root.exists() and root.is_dir()):
            continue
        stats = generic_cleanup(
            root=root,
            context=context,
            policy=policy,
            classify=lambda p: classify_basic(p),
            remove_root_if_empty=True,
        )
        total_stats.normal_deleted += stats.normal_deleted
        total_stats.protected_deleted += stats.protected_deleted
        total_stats.empty_dirs_removed += stats.empty_dirs_removed
    context.log.info(
        f"Limpieza DBT clean-targets: normales={total_stats.normal_deleted} protegidos={total_stats.protected_deleted} directorios_vacios={total_stats.empty_dirs_removed}"
    )


@op
def delete_old_event_logs(context: OpExecutionContext) -> None:
    """Elimina logs de eventos (runs) más antiguos que el período de retención."""
    instance = context.instance
    retention_days = float(SETTINGS["local_storage"]["retention_period"])
    date_from = datetime.now() - timedelta(days=retention_days)
    batch_size = 100

    runs_deleted = 0

    while True:
        old_run_records = instance.get_run_records(
            filters=RunsFilter(created_before=date_from),
            ascending=True,
            limit=batch_size,
        )

        if len(old_run_records) == 0:
            break

        for record in old_run_records:
            path = Path(instance._local_artifact_storage.storage_dir) / Path(
                record.dagster_run.run_id
            )
            if path.is_dir():
                shutil.rmtree(path)

            instance.delete_run(record.dagster_run.run_id)

        runs_deleted += len(old_run_records)

    context.log.info(f"Eliminados {runs_deleted} runs")


@op
def delete_old_event_storage(context: OpExecutionContext) -> None:
    """Limpieza unificada del storage usando generic_cleanup.

    Política aditiva (NO multiplicativa):
      - run (directorio cuyo nombre corresponde a un run id válido): borrar directorio completo si mtime < cutoff de retention_days.
      - normal (no relacionado, no protegido, no run): borrar si mtime < retention_days+5.
      - protected (nombre empieza por '__'): borrar si mtime < retention_days+10.

    Detalles:
      - Un directorio se borra entero sólo si todo el subárbol es eliminable o el propio directorio supera el umbral; en caso contrario se eliminan hijos viejos individualmente y luego se podan directorios vacíos.
      - Se usa mtime (st_mtime) intencionalmente; tocar un archivo/directorio lo rejuvenece.
      - Se agregan conteos en CleanupStats para observabilidad.
    """
    instance = context.instance
    retention_days = float(SETTINGS["local_storage"]["retention_period"])
    policy = build_retention_policy(retention_days)
    storage_base_path = Path(instance._local_artifact_storage.storage_dir)
    context.log.info(f"Iniciando limpieza de storage en: {storage_base_path}")

    def classifier(path: Path) -> str:
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
        except Exception:
            mtime = None
        is_run = path.is_dir() and instance.get_run_by_id(path.name) is not None
        klass = "run" if is_run else classify_basic(path)
        context.log.debug(f"Clasificando {path}: tipo={klass} mtime={mtime}")
        return klass

    stats = generic_cleanup(
        root=storage_base_path,
        context=context,
        policy=policy,
        classify=classifier,
        remove_root_if_empty=False,
    )
    context.log.info(
        "Resumen limpieza storage: "
        f"runs_eliminados={stats.run_dirs_deleted} normales_eliminados={stats.normal_deleted} "
        f"protegidos_eliminados={stats.protected_deleted} dirs_vacios={stats.empty_dirs_removed}"
    )


## Función antigua delete_old_files_in_directory eliminada en favor de generic_cleanup


def _count_files(directory_path: Path) -> int:
    """Cuenta todos los archivos dentro de un árbol de directorios."""
    return sum(1 for _ in directory_path.rglob("*") if _.is_file())


def is_directory_empty(directory_path: Path) -> bool:
    """Determina si un directorio está vacío."""
    # Retorna True si no hay elementos
    return not any(directory_path.iterdir())


@graph
def clean_storage_graph():
    delete_old_event_storage()
    clean_dbt_targets_old_files()


clean_storage_job = clean_storage_graph.to_job(name="clean_storage_job")


if __name__ == "__main__":
    from dagster import instance_for_test
    from datetime import datetime
    import warnings
    import os

    # Determinar entorno
    env_str = os.environ.get("ENV", "local")

    start_time = datetime.now()

    with instance_for_test() as instance:
        if env_str == "local":
            warnings.warn(
                "Ejecutando en modo local con conexión de base de datos de prueba"
            )

        # Ejecutar el job
        result = clean_storage_job.execute_in_process(instance=instance)

        # Imprimir la salida de la operación
        print(result.output_for_node("delete_old_event_storage", output_name="result"))

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
