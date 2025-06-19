import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, PropertyMock

from dagster import build_op_context

from dagster_shared_gf.utils.clean_storage import SETTINGS, delete_old_event_storage


def test_delete_old_event_storage():
    # Create a temporary directory structure to simulate storage
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock storage structure
        storage_dir = Path(temp_dir) / "storage"
        storage_dir.mkdir()

        # Set up timestamps based on retention periods
        retention_days = float(SETTINGS["local_storage"]["retention_period"])
        old_time = (datetime.now() - timedelta(days=retention_days + 10)).timestamp()
        very_old_time = (
            datetime.now() - timedelta(days=retention_days * 2 + 10)
        ).timestamp()  # Beyond extended cutoff
        triple_cutoff_time = (
            datetime.now() - timedelta(days=retention_days * 3 + 10)
        ).timestamp()  # Beyond triple cutoff
        recent_time = datetime.now().timestamp()

        # Recent run directory that should be kept (treated as run storage)
        recent_run_dir = storage_dir / "recent_run"
        recent_run_dir.mkdir()
        (recent_run_dir / "compute_logs").mkdir()
        os.utime(recent_run_dir, (recent_time, recent_time))
        os.utime(recent_run_dir / "compute_logs", (recent_time, recent_time))

        # Old run directory that should be deleted (treated as run storage)
        old_run_dir = storage_dir / "old_run"
        old_run_dir.mkdir()
        (old_run_dir / "compute_logs").mkdir()
        os.utime(old_run_dir, (old_time, old_time))
        os.utime(old_run_dir / "compute_logs", (old_time, old_time))

        # Nested very old directory (not a run, should be deleted in one go)
        # Must be very old (beyond extended cutoff) since it's not a run directory
        nested_old_dir = storage_dir / "nested_old"
        nested_old_dir.mkdir()
        deep_old = nested_old_dir / "deep" / "deeper"
        deep_old.mkdir(parents=True)
        old_file = deep_old / "file.txt"
        old_file.touch()
        os.utime(nested_old_dir, (very_old_time, very_old_time))
        os.utime(deep_old, (very_old_time, very_old_time))
        os.utime(old_file, (very_old_time, very_old_time))

        # Mixed directory (should only delete very old file, keep new)
        mixed_dir = storage_dir / "mixed"
        mixed_dir.mkdir()
        old_mixed_file = mixed_dir / "old.txt"
        new_mixed_file = mixed_dir / "new.txt"
        very_old_mixed_file = mixed_dir / "very_old.txt"
        old_mixed_file.touch()
        new_mixed_file.touch()
        very_old_mixed_file.touch()
        os.utime(mixed_dir, (recent_time, recent_time))
        os.utime(old_mixed_file, (old_time, old_time))
        os.utime(new_mixed_file, (recent_time, recent_time))
        os.utime(very_old_mixed_file, (very_old_time, very_old_time))

        # Protected directory and file (should be deleted only after 3x cutoff)
        protected_dir = storage_dir / "__protected_dir"
        protected_dir.mkdir()
        # Add an inner file that does not start with __ to prevent empty dir deletion
        protected_inner_file = protected_dir / "not_protected.txt"
        protected_inner_file.touch()
        os.utime(protected_inner_file, (triple_cutoff_time, triple_cutoff_time))
        protected_file = storage_dir / "__protected_file.txt"
        protected_file.touch()
        os.utime(protected_dir, (triple_cutoff_time, triple_cutoff_time))
        os.utime(protected_file, (triple_cutoff_time, triple_cutoff_time))

        # Protected directory and file that are not yet old enough (should be preserved)
        protected_dir_recent = storage_dir / "__protected_dir_recent"
        protected_dir_recent.mkdir()
        # Add an inner file that does not start with __ to prevent empty dir deletion
        protected_inner_file_recent = protected_dir_recent / "not_protected.txt"
        protected_inner_file_recent.touch()
        os.utime(protected_inner_file_recent, (very_old_time, very_old_time))
        protected_file_recent = storage_dir / "__protected_file_recent.txt"
        protected_file_recent.touch()
        os.utime(protected_dir_recent, (very_old_time, very_old_time))
        os.utime(protected_file_recent, (very_old_time, very_old_time))

        # Capture the existence of directories BEFORE running the function
        before_state = {
            "recent_run_exists": recent_run_dir.exists(),
            "old_run_exists": old_run_dir.exists(),
            "nested_old_exists": nested_old_dir.exists(),
            "mixed_dir_exists": mixed_dir.exists(),
            "very_old_mixed_file_exists": very_old_mixed_file.exists(),
            "new_mixed_file_exists": new_mixed_file.exists(),
            "protected_dir_exists": protected_dir.exists(),
            "protected_file_exists": protected_file.exists(),
            "protected_dir_recent_exists": protected_dir_recent.exists(),
            "protected_file_recent_exists": protected_file_recent.exists(),
        }

        # Create mock context with our test storage
        context = build_op_context()

        # Mock the storage_dir property
        mock_storage = Mock()
        type(mock_storage).storage_dir = PropertyMock(return_value=str(storage_dir))
        context.instance._local_artifact_storage = mock_storage

        # Mock get_run_by_id to identify run directories
        # Only recent_run_dir and old_run_dir are treated as run storage
        context.instance.get_run_by_id = Mock(
            side_effect=lambda run_id: object()
            if run_id in ["recent_run", "old_run"]
            else None
        )

        # Execute storage cleanup
        delete_old_event_storage(context)

        # --- Assertions ---
        # Check that directories existed before the test
        assert before_state["recent_run_exists"], (
            "Recent run directory should exist before test"
        )
        assert before_state["old_run_exists"], (
            "Old run directory should exist before test"
        )
        assert before_state["nested_old_exists"], (
            "Nested old directory should exist before test"
        )
        assert before_state["mixed_dir_exists"], (
            "Mixed directory should exist before test"
        )
        assert before_state["very_old_mixed_file_exists"], (
            "Very old mixed file should exist before test"
        )
        assert before_state["new_mixed_file_exists"], (
            "New mixed file should exist before test"
        )
        assert before_state["protected_dir_exists"], (
            "Protected directory should exist before test"
        )
        assert before_state["protected_file_exists"], (
            "Protected file should exist before test"
        )
        assert before_state["protected_dir_recent_exists"], (
            "Protected recent directory should exist before test"
        )
        assert before_state["protected_file_recent_exists"], (
            "Protected recent file should exist before test"
        )

        # Check the expected state after running the function
        # Recent run directory should be preserved
        assert recent_run_dir.exists(), "Recent run directory should be preserved"
        assert (recent_run_dir / "compute_logs").exists(), (
            "Recent compute_logs should be preserved"
        )

        # Old run directory should be deleted
        assert not old_run_dir.exists(), "Old run directory should be deleted"

        # For non-run directories, we need to check if they're still accessible
        # If they're not, we can assume they were deleted as expected
        try:
            nested_old_exists = nested_old_dir.exists()
            # If we get here, the directory still exists
            assert not nested_old_exists, "Very old nested directory should be deleted"
        except FileNotFoundError:
            # If we get a FileNotFoundError, the directory was deleted as expected
            pass

        # Mixed directory should exist, very old file deleted, new file preserved
        assert mixed_dir.exists(), "Mixed directory should be preserved"
        try:
            very_old_mixed_file_exists = very_old_mixed_file.exists()
            assert not very_old_mixed_file_exists, (
                "Very old file in mixed directory should be deleted"
            )
        except FileNotFoundError:
            # File was deleted, which is expected
            pass
        assert new_mixed_file.exists(), (
            "New file in mixed directory should be preserved"
        )

        # Protected directory and file should be deleted (since they're older than 3x cutoff)
        assert not protected_dir.exists(), (
            "Protected directory should be deleted after triple cutoff"
        )
        assert not protected_file.exists(), (
            "Protected file should be deleted after triple cutoff"
        )

        # Protected directory and file that are not yet old enough should be preserved
        assert protected_dir_recent.exists(), (
            "Protected recent directory should be preserved"
        )
        assert protected_file_recent.exists(), (
            "Protected recent file should be preserved"
        )
