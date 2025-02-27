from dagster import build_op_context
from dagster_shared_gf.utils.clean_storage import delete_old_event_storage, settings


import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, PropertyMock


def test_delete_old_event_storage():
    # Create a temporary directory structure to simulate storage
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock storage structure
        storage_dir = Path(temp_dir) / "storage"
        storage_dir.mkdir()

        # Create test directories with different dates
        # Recent directory that should be kept
        recent_dir = storage_dir / "recent_run"
        recent_dir.mkdir()
        (recent_dir / "compute_logs").mkdir()

        # Old directory that should be deleted
        old_dir = storage_dir / "old_run"
        old_dir.mkdir()
        old_compute_logs = old_dir / "compute_logs"
        old_compute_logs.mkdir()

        # Modify creation time of old directory and its compute_logs to be older than retention period
        retention_days = float(settings["local_storage"]["retention_period"])
        old_time = (datetime.now() - timedelta(days=retention_days + 10)).timestamp()
        os.utime(old_dir, (old_time, old_time))
        os.utime(old_compute_logs, (old_time, old_time))

        # Create recent timestamp
        recent_time = datetime.now().timestamp()
        os.utime(recent_dir, (recent_time, recent_time))
        os.utime(recent_dir / "compute_logs", (recent_time, recent_time))

        # Create mock context with our test storage
        context = build_op_context()
        
        # Mock the storage_dir property
        mock_storage = Mock()
        type(mock_storage).storage_dir = PropertyMock(return_value=str(storage_dir))
        context.instance._local_artifact_storage = mock_storage
        
        # Mock get_run_by_id to return None
        context.instance.get_run_by_id = Mock(return_value=None)

        # Execute storage cleanup
        delete_old_event_storage(context)

        # Verify results
        assert recent_dir.exists(), "Recent directory should be preserved"
        assert not old_dir.exists(), "Old directory should be deleted"
