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
    For unrelated files/folders: deletes individual files older than twice the retention_period,
    and only removes directories when they become empty.
    Files or directories starting with '__' are preserved.
    """
    instance = context.instance
    retention_days = float(settings["local_storage"]["retention_period"])
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    extended_cutoff_date = datetime.now() - timedelta(
        days=retention_days * 2
    )  # Double retention period for unrelated files

    storage_dirs_deleted = 0
    unrelated_files_deleted = 0
    unrelated_empty_dirs_deleted = 0
    protected_items_skipped = 0

    storage_base_path = Path(instance._local_artifact_storage.storage_dir)

    context.log.info(f"Starting to clean storage at path: {storage_base_path}")

    # Scan all directories in storage
    for path in storage_base_path.iterdir():
        # Skip files/directories starting with '__'
        if path.name.startswith("__"):
            protected_items_skipped += 1
            continue

        if path.is_dir():
            # Verify this is a run storage directory by checking:
            # 1. If directory name exists as a run ID
            # 2. Or if it contains compute_logs directory
            is_run_storage = (
                instance.get_run_by_id(path.name) is not None
                or (path / "compute_logs").exists()
            )

            if is_run_storage:
                # For run storage, use standard retention period and delete entire directory
                creation_time = datetime.fromtimestamp(path.stat().st_mtime)
                if creation_time < cutoff_date:
                    shutil.rmtree(path)
                    storage_dirs_deleted += 1
            else:
                # For unrelated storage, delete old files first
                files_deleted, skipped = delete_old_files_in_directory(
                    path, extended_cutoff_date, context
                )
                unrelated_files_deleted += files_deleted
                protected_items_skipped += skipped

                # Check if directory is now empty and can be deleted
                if is_directory_empty(path):
                    path.rmdir()
                    unrelated_empty_dirs_deleted += 1
                    context.log.debug(f"Deleted empty directory: {path}")
        elif path.is_file():
            # Skip files starting with '__'
            if path.name.startswith("__"):
                protected_items_skipped += 1
                continue

            # Handle files with extended retention period
            creation_time = datetime.fromtimestamp(path.stat().st_mtime)
            if creation_time < extended_cutoff_date:
                path.unlink()
                unrelated_files_deleted += 1

    context.log.info(
        f"Deleted {storage_dirs_deleted} storage directories older than {retention_days} days"
    )
    context.log.info(
        f"Deleted {unrelated_files_deleted} unrelated files older than {retention_days * 2} days"
    )
    context.log.info(
        f"Deleted {unrelated_empty_dirs_deleted} empty unrelated directories"
    )
    context.log.info(
        f"Skipped {protected_items_skipped} protected items (starting with '__')"
    )


def delete_old_files_in_directory(
    directory_path: Path, cutoff_date: datetime, context: OpExecutionContext
) -> tuple[int, int]:
    """
    Recursively deletes files older than the cutoff date in the given directory.
    Skips files and directories starting with '__'.

    Args:
        directory_path: Path to the directory to clean
        cutoff_date: Files older than this date will be deleted
        context: Dagster execution context for logging

    Returns:
        Tuple of (number of files deleted, number of protected items skipped)
    """
    files_deleted = 0
    protected_items_skipped = 0

    # Process all files in this directory
    for item in directory_path.glob("*"):
        # Skip files/directories starting with '__'
        if item.name.startswith("__"):
            protected_items_skipped += 1
            continue

        if item.is_file():
            creation_time = datetime.fromtimestamp(item.stat().st_mtime)
            if creation_time < cutoff_date:
                try:
                    item.unlink()
                    files_deleted += 1
                    context.log.debug(f"Deleted old file: {item}")
                except Exception as e:
                    context.log.error(f"Failed to delete file {item}: {str(e)}")
        elif item.is_dir():
            # Recursively process subdirectories
            sub_deleted, sub_skipped = delete_old_files_in_directory(
                item, cutoff_date, context
            )
            files_deleted += sub_deleted
            protected_items_skipped += sub_skipped

            # Check if directory is now empty and can be deleted
            if is_directory_empty(item):
                try:
                    item.rmdir()
                    context.log.debug(f"Deleted empty subdirectory: {item}")
                except Exception as e:
                    context.log.error(f"Failed to delete directory {item}: {str(e)}")

    return files_deleted, protected_items_skipped


def is_directory_empty(directory_path: Path) -> bool:
    """
    Checks if a directory is empty.

    Args:
        directory_path: Path to the directory to check

    Returns:
        True if the directory is empty, False otherwise
    """
    # Check if any items exist in the directory
    return not any(directory_path.iterdir())


@graph
def clean_storage_graph():
    delete_old_event_storage()


clean_storage_job = clean_storage_graph.to_job(name="clean_storage_job")


if __name__ == "__main__":
    from dagster import instance_for_test
    from datetime import datetime
    import warnings
    import os

    # Determine environment
    env_str = os.environ.get("ENV", "local")

    start_time = datetime.now()

    with instance_for_test() as instance:
        if env_str == "local":
            warnings.warn("Running in local mode with test database connection")

        # Run the job
        result = clean_storage_job.execute_in_process(
            instance=instance,
        )

        # Print the output of the operation
        print(result.output_for_node("delete_old_event_storage", output_name="result"))

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
