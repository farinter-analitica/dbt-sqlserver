import shutil
from datetime import datetime, timedelta
from pathlib import Path

from dagster import OpExecutionContext, RunsFilter, graph, op

settings = {"local_storage": {"retention_period": 90}}


@op
def delete_old_event_logs(context: OpExecutionContext) -> None:
    """
    Deletes event logs from logs and storage that are older than retention_period
    """
    instance = context.instance
    retention_days = float(settings["local_storage"]["retention_period"])
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

    context.log.info(f"Deleted {runs_deleted} runs")


@op
def delete_old_event_storage(context: OpExecutionContext) -> None:
    """
    Deletes storage files older than retention period by checking file creation dates.
    For Dagster run storage: deletes if older than retention_period.
    For unrelated files/folders: deletes if older than twice the retention_period.
    """
    instance = context.instance
    retention_days = float(settings["local_storage"]["retention_period"])
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    extended_cutoff_date = datetime.now() - timedelta(
        days=retention_days * 2
    )  # Double retention period for unrelated files

    storage_dirs_deleted = 0
    unrelated_items_deleted = 0

    storage_base_path = Path(instance._local_artifact_storage.storage_dir)

    # Scan all directories in storage
    for path in storage_base_path.glob("*"):
        if path.is_dir():
            # Verify this is a run storage directory by checking:
            # 1. If directory name exists as a run ID
            # 2. Or if it contains compute_logs directory
            is_run_storage = (
                instance.get_run_by_id(path.name) is not None
                or (path / "compute_logs").exists()
            )

            creation_time = datetime.fromtimestamp(path.stat().st_mtime)

            if is_run_storage:
                # For run storage, use standard retention period
                if creation_time < cutoff_date:
                    shutil.rmtree(path)
                    storage_dirs_deleted += 1
            else:
                # For unrelated storage, use extended retention period
                if creation_time < extended_cutoff_date:
                    shutil.rmtree(path)
                    unrelated_items_deleted += 1
        elif path.is_file():
            # Handle files with extended retention period
            creation_time = datetime.fromtimestamp(path.stat().st_mtime)
            if creation_time < extended_cutoff_date:
                path.unlink()
                unrelated_items_deleted += 1

    context.log.info(
        f"Deleted {storage_dirs_deleted} storage directories older than {retention_days} days"
    )
    context.log.info(
        f"Deleted {unrelated_items_deleted} unrelated files/folders older than {retention_days * 2} days"
    )


@graph
def clean_storage_graph():
    delete_old_event_storage()


clean_storage_job = clean_storage_graph.to_job(name="clean_storage_job")
