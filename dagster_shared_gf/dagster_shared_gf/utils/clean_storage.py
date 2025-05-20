import shutil
from datetime import datetime, timedelta
from pathlib import Path
import yaml
import os

from dagster import OpExecutionContext, RunsFilter, graph, op
from dagster_shared_gf.shared_functions import get_for_current_env

SETTINGS = {
    "local_storage": {"retention_period": get_for_current_env({"dev": 40, "prd": 31})}
}
DAGSTER_HOME = os.environ.get("DAGSTER_HOME") or "."


@op
def clean_dbt_targets_old_files(context: OpExecutionContext) -> None:
    """
    Cleans old files inside dbt clean-targets directories as defined in dbt_project.yml,
    using the same retention logic as delete_old_event_storage.
    """
    dbt_project_path = Path(DAGSTER_HOME) / "dbt_dwh_farinter" / "dbt_project.yml"
    if not dbt_project_path.exists():
        raise FileNotFoundError(f"dbt_project.yml not found at {dbt_project_path}")

    with dbt_project_path.open("r", encoding="utf-8") as f:
        dbt_config = yaml.safe_load(f)

    clean_targets = dbt_config.get("clean-targets", [])
    project_dir = dbt_project_path.parent

    retention_days = float(SETTINGS["local_storage"]["retention_period"])
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    protected_cutoff_date = datetime.now() - timedelta(days=retention_days * 3)

    total_files_deleted = 0
    total_protected_skipped = 0
    total_empty_dirs_deleted = 0

    for rel_path in clean_targets:
        abs_path = project_dir / rel_path
        if abs_path.exists() and abs_path.is_dir():
            context.log.info(
                f"Cleaning old files in dbt clean-target directory: {abs_path}"
            )
            files_deleted, protected_skipped = delete_old_files_in_directory(
                abs_path, cutoff_date, context, protected_cutoff_date
            )
            total_files_deleted += files_deleted
            total_protected_skipped += protected_skipped

            # Optionally, remove empty directories after cleaning
            if is_directory_empty(abs_path):
                try:
                    abs_path.rmdir()
                    total_empty_dirs_deleted += 1
                    context.log.info(
                        f"Deleted empty clean-target directory: {abs_path}"
                    )
                except Exception as e:
                    context.log.error(
                        f"Failed to delete empty directory {abs_path}: {e}"
                    )
        else:
            context.log.debug(
                f"dbt clean-target does not exist or is not a directory: {abs_path}"
            )

    context.log.info(
        f"Deleted {total_files_deleted} old files in dbt clean-targets, "
        f"skipped {total_protected_skipped} protected items, "
        f"deleted {total_empty_dirs_deleted} empty clean-target directories."
    )


@op
def delete_old_event_logs(context: OpExecutionContext) -> None:
    """
    Deletes event logs from logs and storage that are older than retention_period
    """
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

    context.log.info(f"Deleted {runs_deleted} runs")


@op
def delete_old_event_storage(context: OpExecutionContext) -> None:
    """
    Deletes storage files older than retention period by checking file creation dates.
    For Dagster run storage: deletes if older than retention_period.
    For unrelated files/folders: deletes individual files older than twice the retention_period,
    and only removes directories when they become empty.
    Files or directories starting with '__' are deleted if older than triple the retention period.
    """
    instance = context.instance
    retention_days = float(SETTINGS["local_storage"]["retention_period"])
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    extended_cutoff_date = datetime.now() - timedelta(
        days=retention_days * 2
    )  # Double retention period for unrelated files
    triple_extended_cutoff_date = datetime.now() - timedelta(
        days=retention_days * 3
    )  # Triple retention period for protected items

    storage_dirs_deleted = 0
    unrelated_files_deleted = 0
    unrelated_empty_dirs_deleted = 0
    protected_items_deleted = 0

    storage_base_path = Path(instance._local_artifact_storage.storage_dir)

    context.log.info(f"Starting to clean storage at path: {storage_base_path}")

    # Collect all paths first to avoid modification during iteration
    paths_to_process = list(storage_base_path.iterdir())

    # Scan all directories in storage
    for path in paths_to_process:
        # Skip if path no longer exists (might have been deleted as part of another directory)
        if not path.exists():
            continue

        creation_time = datetime.fromtimestamp(path.stat().st_mtime)
        if path.is_dir():
            # Verify this is a run storage directory by checking:
            # 1. If directory name exists as a run ID
            is_run_storage = instance.get_run_by_id(path.name) is not None

            if is_run_storage:
                # For run storage, use standard retention period and delete entire directory
                if creation_time < cutoff_date:
                    shutil.rmtree(path)
                    storage_dirs_deleted += 1
            else:
                # For unrelated storage, delete old files first
                files_deleted, protected_deleted = delete_old_files_in_directory(
                    path, extended_cutoff_date, context, triple_extended_cutoff_date
                )
                unrelated_files_deleted += files_deleted
                protected_items_deleted += protected_deleted

                # Check if directory is now empty and can be deleted
                # Only check if the directory still exists
                if path.exists() and is_directory_empty(path):
                    try:
                        path.rmdir()
                        unrelated_empty_dirs_deleted += 1
                        context.log.debug(f"Deleted empty directory: {path}")
                    except (FileNotFoundError, OSError) as e:
                        context.log.debug(f"Could not delete directory {path}: {e}")
        elif path.is_file():
            # Handle files with extended retention period
            if path.name.startswith("__"):
                if creation_time < triple_extended_cutoff_date:
                    try:
                        path.unlink()
                        protected_items_deleted += 1
                        context.log.debug(f"Deleted protected file: {path}")
                    except FileNotFoundError:
                        pass
                continue

            if creation_time < extended_cutoff_date:
                try:
                    path.unlink()
                    unrelated_files_deleted += 1
                except FileNotFoundError:
                    # File might have been deleted already
                    pass

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
        f"Deleted {protected_items_deleted} protected items (starting with '__') older than {retention_days * 3} days"
    )


def delete_old_files_in_directory(
    directory_path: Path,
    cutoff_date: datetime,
    context: OpExecutionContext,
    protected_cutoff_date: datetime,
) -> tuple[int, int]:
    """
    Efficiently deletes files older than the cutoff date in the given directory.
    If all files and subdirectories are eligible for deletion, deletes the whole directory tree at once.
    Files or directories starting with '__' are deleted if older than protected_cutoff_date.

    Returns:
        Tuple of (number of files deleted, number of protected items deleted)
    """
    files_to_delete: list[Path] = []
    fully_deletable_dirs: list[Path] = []
    protected_items_to_delete: list[Path] = []

    def mark_for_deletion(path: Path) -> bool:
        nonlocal protected_items_to_delete

        if path.name.startswith("__"):
            if protected_cutoff_date is not None:
                creation_time = datetime.fromtimestamp(path.stat().st_mtime)
                if creation_time < protected_cutoff_date:
                    if path.is_file():
                        protected_items_to_delete.append(path)
                        return True
                    elif path.is_dir():
                        # Recursively mark all children for deletion
                        all_deletable = True
                        for item in path.iterdir():
                            if not mark_for_deletion(item):
                                all_deletable = False
                        if all_deletable:
                            protected_items_to_delete.append(path)
                            return True
                        return False
            return False

        if path.is_file():
            creation_time = datetime.fromtimestamp(path.stat().st_mtime)
            if creation_time < cutoff_date:
                files_to_delete.append(path)
                return True
            else:
                return False

        # Directory
        all_deletable = True
        for item in path.iterdir():
            if not mark_for_deletion(item):
                all_deletable = False

        if all_deletable:
            fully_deletable_dirs.append(path)
            return True
        return False

    # First pass: mark files and dirs
    mark_for_deletion(directory_path)

    # Second pass: delete fully deletable dirs (deepest first)
    files_deleted = 0
    for dir_path in sorted(
        fully_deletable_dirs, key=lambda p: len(p.parts), reverse=True
    ):
        try:
            num_files = _count_files(dir_path)
            shutil.rmtree(dir_path)
            files_deleted += num_files
            context.log.debug(
                f"Deleted entire directory tree: {dir_path} ({num_files} files)"
            )
        except Exception as e:
            context.log.error(f"Failed to delete directory tree {dir_path}: {str(e)}")

    # Delete individual files not part of a fully deletable dir
    # (skip files inside dirs already deleted)
    deleted_dirs_set = set(fully_deletable_dirs)
    for file_path in files_to_delete:
        # If file is inside a dir already deleted, skip
        if any(parent in deleted_dirs_set for parent in file_path.parents):
            continue
        try:
            file_path.unlink()
            files_deleted += 1
            context.log.debug(f"Deleted old file: {file_path}")
        except Exception as e:
            context.log.error(f"Failed to delete file {file_path}: {str(e)}")

    # Delete protected items
    protected_deleted = 0
    for protected_path in sorted(
        protected_items_to_delete, key=lambda p: len(p.parts), reverse=True
    ):
        try:
            if protected_path.is_file():
                protected_path.unlink()
                protected_deleted += 1
                context.log.debug(f"Deleted protected file: {protected_path}")
            elif protected_path.is_dir():
                num_files = _count_files(protected_path)
                shutil.rmtree(protected_path)
                protected_deleted += num_files
                context.log.debug(
                    f"Deleted protected directory tree: {protected_path} ({num_files} files)"
                )
        except Exception as e:
            context.log.error(
                f"Failed to delete protected item {protected_path}: {str(e)}"
            )

    return files_deleted, protected_deleted


def _count_files(directory_path: Path) -> int:
    """Helper to count all files in a directory tree."""
    return sum(1 for _ in directory_path.rglob("*") if _.is_file())


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
    clean_dbt_targets_old_files()


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
