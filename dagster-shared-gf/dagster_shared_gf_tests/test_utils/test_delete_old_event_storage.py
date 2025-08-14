import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, PropertyMock

from dagster import build_op_context

from dagster_shared_gf.utils.clean_storage import SETTINGS, delete_old_event_storage


def test_delete_old_event_storage():
    # Crear estructura temporal que simula el storage
    with tempfile.TemporaryDirectory() as temp_dir:
        storage_dir = Path(temp_dir) / "storage"
        storage_dir.mkdir()

        # Timestamps basados en períodos de retención
        retention_days = float(SETTINGS["local_storage"]["retention_period"])
        old_time = (datetime.now() - timedelta(days=retention_days + 10)).timestamp()
        extended_time = (
            datetime.now() - timedelta(days=retention_days + 6)
        ).timestamp()
        protected_old_time = (
            datetime.now() - timedelta(days=retention_days + 11)
        ).timestamp()
        protected_recent_time = (
            datetime.now() - timedelta(days=retention_days + 2)
        ).timestamp()
        old_unrelated_time = (
            datetime.now() - timedelta(days=retention_days + 2)
        ).timestamp()
        recent_time = datetime.now().timestamp()

        # Run reciente que debe mantenerse
        recent_run_dir = storage_dir / "recent_run"
        recent_run_dir.mkdir()
        (recent_run_dir / "compute_logs").mkdir()
        os.utime(recent_run_dir, (recent_time, recent_time))
        os.utime(recent_run_dir / "compute_logs", (recent_time, recent_time))

        # Run antiguo que debe eliminarse
        old_run_dir = storage_dir / "old_run"
        old_run_dir.mkdir()
        (old_run_dir / "compute_logs").mkdir()
        os.utime(old_run_dir, (old_time, old_time))
        os.utime(old_run_dir / "compute_logs", (old_time, old_time))

        # Directorio anidado muy viejo (no es run, se elimina completo)
        nested_old_dir = storage_dir / "nested_old"
        nested_old_dir.mkdir()
        deep_old = nested_old_dir / "deep" / "deeper"
        deep_old.mkdir(parents=True)
        deep_parent = nested_old_dir / "deep"
        old_file = deep_old / "file.txt"
        old_file.touch()
        os.utime(nested_old_dir, (extended_time, extended_time))
        os.utime(deep_parent, (extended_time, extended_time))
        os.utime(deep_old, (extended_time, extended_time))
        os.utime(old_file, (extended_time, extended_time))

        # Directorio mixto
        mixed_dir = storage_dir / "mixed"
        mixed_dir.mkdir()
        old_mixed_file = mixed_dir / "old.txt"
        new_mixed_file = mixed_dir / "new.txt"
        very_old_mixed_file = mixed_dir / "very_old.txt"
        old_mixed_file.touch()
        new_mixed_file.touch()
        very_old_mixed_file.touch()
        os.utime(mixed_dir, (recent_time, recent_time))
        os.utime(old_mixed_file, (old_unrelated_time, old_unrelated_time))
        os.utime(new_mixed_file, (recent_time, recent_time))
        os.utime(very_old_mixed_file, (extended_time, extended_time))

        # Directorio/archivo protegido
        protected_dir = storage_dir / "__protected_dir"
        protected_dir.mkdir()
        protected_inner_old = protected_dir / "old.txt"
        protected_inner_old.touch()
        os.utime(protected_inner_old, (protected_old_time, protected_old_time))
        protected_inner_recent = protected_dir / "recent.txt"
        protected_inner_recent.touch()
        os.utime(protected_inner_recent, (protected_recent_time, protected_recent_time))
        protected_file = storage_dir / "__protected_file.txt"
        protected_file.touch()
        os.utime(protected_dir, (protected_old_time, protected_old_time))
        os.utime(protected_file, (protected_old_time, protected_old_time))

        # Protected reciente
        protected_dir_recent = storage_dir / "__protected_dir_recent"
        protected_dir_recent.mkdir()
        protected_inner_file_recent = protected_dir_recent / "not_protected.txt"
        protected_inner_file_recent.touch()
        os.utime(
            protected_inner_file_recent, (protected_recent_time, protected_recent_time)
        )
        protected_file_recent = protected_dir_recent / "__protected_file_recent.txt"
        protected_file_recent.touch()
        os.utime(protected_dir_recent, (protected_recent_time, protected_recent_time))
        os.utime(protected_file_recent, (protected_recent_time, protected_recent_time))

        # Estado antes
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

        # Contexto y mocks
        context = build_op_context()
        mock_storage = Mock()
        type(mock_storage).storage_dir = PropertyMock(return_value=str(storage_dir))
        context.instance._local_artifact_storage = mock_storage
        context.instance.get_run_by_id = Mock(
            side_effect=lambda run_id: object()
            if run_id in ["recent_run", "old_run"]
            else None
        )

        # Ejecutar limpieza
        delete_old_event_storage(context)

        # DEBUG: listar contenido
        print("CONTENIDO_STORAGE:", sorted(p.name for p in storage_dir.iterdir()))

        # --- Asserts ---
        assert before_state["recent_run_exists"], "Run reciente debe existir antes"
        assert before_state["old_run_exists"], "Run antiguo debe existir antes"
        assert before_state["nested_old_exists"], (
            "Directorio anidado viejo debe existir antes"
        )
        assert before_state["mixed_dir_exists"], "Directorio mixto debe existir antes"
        assert before_state["very_old_mixed_file_exists"], (
            "Archivo muy viejo mixto debe existir antes"
        )
        assert before_state["new_mixed_file_exists"], (
            "Archivo nuevo mixto debe existir antes"
        )
        assert before_state["protected_dir_exists"], "Dir protegido debe existir antes"
        assert before_state["protected_file_exists"], (
            "Archivo protegido debe existir antes"
        )
        assert before_state["protected_dir_recent_exists"], (
            "Dir protegido reciente debe existir antes"
        )
        assert before_state["protected_file_recent_exists"], (
            "Archivo protegido reciente debe existir antes"
        )

        # Post-exec checks
        assert recent_run_dir.exists(), "Run reciente debe preservarse"
        assert (recent_run_dir / "compute_logs").exists(), (
            "compute_logs reciente debe preservarse"
        )
        assert not old_run_dir.exists(), "Run antiguo debe eliminarse"
        assert not nested_old_dir.exists(), (
            "Directorio anidado muy viejo debe eliminarse"
        )
        assert mixed_dir.exists(), "Directorio mixto debe preservarse"
        try:
            very_old_mixed_file_exists = very_old_mixed_file.exists()
            assert not very_old_mixed_file_exists, (
                "Archivo muy viejo en mixto debe borrarse"
            )
        except FileNotFoundError:
            pass
        assert old_mixed_file.exists(), "Archivo viejo mixto no debe borrarse aún"
        assert new_mixed_file.exists(), "Archivo nuevo mixto debe preservarse"
        assert not protected_file.exists(), "Archivo protegido viejo debe borrarse"
        assert protected_dir.exists(), "Dir protegido debe permanecer (hijo reciente)"
        assert not (protected_dir / "old.txt").exists(), (
            "Archivo viejo en dir protegido debe borrarse"
        )
        assert (protected_dir / "recent.txt").exists(), (
            "Archivo reciente en dir protegido debe preservarse"
        )
        assert protected_dir_recent.exists(), "Dir protegido reciente debe preservarse"
        assert protected_file_recent.exists(), (
            "Archivo protegido reciente debe preservarse"
        )
