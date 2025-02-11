from datetime import datetime
from io import BytesIO
import json
from pathlib import PureWindowsPath
import re
import time
from typing import Any
import polars as pl
from dagster import (
    AssetExecutionContext,
    Field,
    MaterializeResult,
    OpExecutionContext,
    op,
)

from dagster_shared_gf.resources.smb_resources import SMBResource
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import (
    clean_filename,
    clean_filename_to_key,
    get_all_locations_name,
    get_job_status,
    start_job_by_name,
    verify_location_name,
)
from dagster_shared_gf.shared_variables import (
    ErrorsOccurred,
    ExcelProcessConfig,
    FileException,
    NullsException,
    env_str,
)
from ydata_profiling import ProfileReport


def wait_if_job_running_to_execute_next_op(current_location_name: str) -> op:
    @op(
        description="Wait for a job to complete if running and afterwards execute another job.",
        config_schema={
            "wait_for_job": str,
            "job_to_execute": str,
            "current_location_name": Field(
                str, is_required=False, default_value=current_location_name
            ),
            "job_status_to_wait_change": Field(
                str, is_required=False, default_value="STARTED"
            ),
            "max_seconds_to_wait": Field(int, is_required=False, default_value=3600),
            "check_interval_on_seconds": Field(
                int, is_required=False, default_value=30
            ),
        },
    )
    def wait_if_job_running_to_execute_next_op(context: OpExecutionContext) -> None:
        wait_for_job: str = context.op_execution_context.op_config["wait_for_job"]
        max_seconds_to_wait: int = context.op_execution_context.op_config[
            "max_seconds_to_wait"
        ]
        check_interval_on_seconds: int = context.op_execution_context.op_config[
            "check_interval_on_seconds"
        ]
        job_status_to_wait_change = context.op_execution_context.op_config[
            "job_status_to_wait_change"
        ]
        job_to_execute: str = context.op_execution_context.op_config["job_to_execute"]
        current_location_name = context.op_execution_context.op_config[
            "current_location_name"
        ]
        if not verify_location_name(current_location_name):
            raise Exception(
                f"Invalid location name: {current_location_name}, list found on workspace.yaml: {get_all_locations_name()}"
            )

        context.log.info(
            f"Waiting for job '{wait_for_job}' to complete if running and afterwards execute another job."
        )
        try:
            while True:
                wait_for_job_status = get_job_status(wait_for_job)
                context.log.info(
                    f"Status of job '{wait_for_job}': {wait_for_job_status}"
                )
                if wait_for_job_status == job_status_to_wait_change:
                    context.log.info(
                        f"The job '{wait_for_job}' is currently {job_status_to_wait_change}. Waiting for change..."
                    )
                    time.sleep(check_interval_on_seconds)  # Wait for one minute
                    max_seconds_to_wait -= check_interval_on_seconds
                else:
                    context.log.info(f"The job '{wait_for_job}' is not running.")
                    break  # Exit the loop
                if max_seconds_to_wait <= 0:
                    context.log.info(
                        f"Timed out waiting for job '{wait_for_job}' to complete. Returning true to run the next job."
                    )
                    break
            context.log.info(f"Executing job '{job_to_execute}'")
            start_job_by_name(job_to_execute, location_name=current_location_name)
        except Exception as e:
            context.log.error(f"Failed to get status of job '{wait_for_job}': {e}")
        return

    return wait_if_job_running_to_execute_next_op


class ExcelFileProcessor:
    """
    A class to process Excel files and load them into a database.
    Provides a transform method to apply custom transformations to the data.

    Args:
        schema_config (ExcelLoadConfig): The configuration for the Excel schema.
        table (str): The name of the table to load the data into.
        db_schema (str): The name of the database schema.

    Attributes:
        schema_config (ExcelLoadConfig): The configuration for the Excel schema.
        table (str): The name of the table to load the data into.
        db_schema (str): The name of the database schema.
        directory_path (PureWindowsPath): The path to the directory containing the Excel files.
        context (AssetExecutionContext): The execution context.
        smb_resource (SMBResource): The SMB resource.
        dwh_resource (SQLServerResource): The DWH resource.
        v_metadata (dict[str, Any]): The metadata for the processed files.
        drop_table_count (int): The number of times the table has been dropped.
    """

    def __init__(
        self,
        config: ExcelProcessConfig,
        table: str,
        db_schema: str,
        directory_path: PureWindowsPath,
        context: AssetExecutionContext,
        smb_resource: SMBResource,
        dwh_resource: SQLServerResource,
        move_processed_files_to_folder: bool = True,
        add_metadata_columns: bool = True,
        filename_column: str = "nombre_archivo",
        date_loaded_column: str = "fecha_carga",
        date_updated_column: str = "fecha_actualizado",
    ):
        self.schema_config = config
        self.table = table
        self.db_schema = db_schema
        self.directory_path = directory_path
        self.context = context
        self.smb_resource = smb_resource
        self.dwh_resource = dwh_resource
        self.v_metadata: dict[str, Any] = {"Archivos": {}}
        self.drop_table_count = 0
        self.move_processed_files_to_folder = move_processed_files_to_folder
        self.add_metadata_columns = add_metadata_columns
        self.filename_column = filename_column
        self.date_loaded_column = date_loaded_column
        self.date_updated_column = date_updated_column

    def process_files(self, single_file: str | None = None) -> MaterializeResult:
        """
        Process all Excel files in the directory.
        Leaves log of errors and successes in a txt file.
        Moves processed files to a folder if configured.
        Args:
            single_file (str | None, optional): Process a single file. Defaults to None.
        Returns:
            MaterializeResult: The metadata of the processed files.
        """
        try:
            if single_file:
                file_descriptors = [
                    self.smb_resource.get_file_info(self.directory_path / single_file)
                ]
            else:
                file_descriptors = self.smb_resource.get_server_dirs(
                    directory=self.directory_path,
                    extension=".xlsx",
                    exclude=[self.schema_config.loaded_files_folder, "plantilla.xlsx"],
                )

            for file_descriptor in file_descriptors:
                self._process_single_file(file_descriptor)

            if self.v_metadata.get("Cant. Errores", 0) > 0:
                raise ErrorsOccurred(self.v_metadata)

        except (Exception, ErrorsOccurred) as e:
            self.context.log.info("log de carga de archivos:" + str(self.v_metadata))
            log_message = (
                f"ERROR, N/A en {env_str}, {datetime.now().isoformat()}, {str(e)}\n"
            )
            with self.smb_resource.client.open_file(
                path=self.directory_path.joinpath("logs_carga.txt"), mode="a"
            ) as file:
                file.write(log_message)
            raise e

        return MaterializeResult(metadata=self.v_metadata)

    def _process_single_file(self, file_descriptor):
        rows_inserted = 0
        nulls_count = 0
        current_file_path = self.smb_resource.get_full_server_path(file_descriptor.path)
        current_file_key = clean_filename_to_key(
            str(
                current_file_path.relative_to(
                    self.smb_resource.get_full_server_path(self.directory_path)
                )
            )
        )

        try:
            self.v_metadata["Archivos"][current_file_key] = {}
            df = self._read_and_validate_excel(current_file_path)
            df = self.transform_data(df)

            nulls_count = int(df.null_count().sum_horizontal().sum())
            row_count = df.height

            self._update_metadata(current_file_key, df, nulls_count, row_count)
            self._validate_nulls(current_file_key, df, nulls_count)
            if self.add_metadata_columns:
                df = self._add_metadata_columns(df, file_descriptor)

            rows_inserted = self._write_to_database(df)
            self._log_success(current_file_path, current_file_key, row_count)
            self._move_processed_file(current_file_path, file_descriptor)

            self.v_metadata["Archivos"][current_file_key]["Cargado"] = True
            self.v_metadata["Cant. Archivos Cargados"] = (
                self.v_metadata.get("Cant. Archivos Cargados", 0) + 1
            )

        except (NullsException, FileException, ErrorsOccurred) as ne:
            self._handle_known_error(
                ne, current_file_path, current_file_key, rows_inserted
            )
        except Exception as e:
            self._handle_unknown_error(
                e, current_file_path, current_file_key, rows_inserted
            )

    def _read_and_validate_excel(self, file_path) -> pl.DataFrame:
        with self.smb_resource.client.open_file(path=file_path, mode="rb") as file:
            dfd = pl.read_excel(
                BytesIO(file.read()),
                sheet_id=0,
                engine="calamine",
                raise_if_empty=False,
            )
            sheet_name_pattern = re.compile(
                r"\b" f"{self.schema_config.excel_sheet_name}" r".*", re.IGNORECASE
            )

            for sheet_name in dfd.keys():
                if sheet_name_pattern.match(sheet_name):
                    df = dfd[sheet_name]
                    break
            else:
                raise FileException(
                    f"No se encontro una hoja con el patron {sheet_name_pattern.pattern}"
                )

        if len(set(self.schema_config.expected_columns.keys()) - set(df.columns)) > 0:
            raise ErrorsOccurred(
                f"No se encontraron las columnas {set(self.schema_config.expected_columns.keys()) - set(df.columns)}"
            )

        df = df.cast({**self.schema_config.polars_schema})
        if self.schema_config.exclude_colums:
            df = df.drop(self.schema_config.exclude_colums, strict=False)

        if self.schema_config.primary_keys:
            unique_check_height = df.n_unique(subset=self.schema_config.primary_keys)
            if unique_check_height != df.height:
                raise ErrorsOccurred(
                    "Los registros no son únicos por las claves especificadas."
                )

        return df

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Override this method in subclasses to implement specific transformations"""
        return df

    def _update_metadata(
        self, current_file_key: str, df: pl.DataFrame, nulls_count: int, row_count: int
    ):
        self.v_metadata["Archivos"][current_file_key].update(
            {
                "Cargado": False,
                "Cant. Filas": row_count,
                "Cant. Valores en Blanco": nulls_count,
                "Perfil de Datos": json.loads(
                    ProfileReport(
                        df.to_pandas(), title="Pandas Profiling Report"
                    ).to_json()
                ).get("table", "error"),
            }
        )

    def _validate_nulls(
        self, current_file_key: str, df: pl.DataFrame, nulls_count: int
    ):
        if not self.schema_config.blanks_allowed and nulls_count > 0:
            raise NullsException(
                f"Archivo {current_file_key} tiene {nulls_count} valores en Blanco."
            )
        if self.schema_config.fill_nulls:
            df = df.fill_null(strategy="zero")

    def _write_to_database(self, df: pl.DataFrame) -> int:
        with self.dwh_resource.get_sqlalchemy_conn() as conn:
            if self.drop_table_count == 0:
                conn.execute(
                    self.dwh_resource.text(
                        f"IF OBJECT_ID('{self.db_schema}.{self.table}', 'U') IS NOT NULL BEGIN DROP TABLE {self.db_schema}.{self.table} END; "
                    )
                )
                self.drop_table_count += 1
            conn.commit()
            return df.write_database(
                table_name=f"{self.db_schema}.{self.table}",
                connection=conn,
                if_table_exists="append",
            )

    def _log_success(
        self, current_file_path: PureWindowsPath, current_file_key: str, row_count: int
    ):
        with self.smb_resource.client.open_file(
            path=current_file_path.parent.joinpath("logs_carga.txt"), mode="a"
        ) as file:
            file.write(
                f"INFO, CARGADO, {datetime.now().isoformat()} , Archivo {current_file_key} cargado con {row_count} filas.\n"
            )

    def _move_processed_file(self, current_file_path: PureWindowsPath, file_descriptor):
        if env_str in ["prd"]:
            self.smb_resource.copy_server_file(
                file_path=current_file_path,
                new_path=current_file_path.parent.joinpath(
                    self.schema_config.loaded_files_folder
                ).joinpath(clean_filename(file_descriptor.name)),
                move=self.move_processed_files_to_folder,
            )

    def _handle_known_error(
        self,
        error: NullsException | FileException | ErrorsOccurred,
        current_file_path: PureWindowsPath,
        current_file_key: str,
        rows_inserted: int,
    ):
        self.context.log.error(error)
        log_message = (
            f"ERROR, NO CARGADO en {env_str}, {datetime.now().isoformat()}, "
            + f"Archivo {current_file_key} error {str(error)}."
        )
        self.v_metadata["Archivos"][current_file_key]["Error"] = log_message
        self.v_metadata["Cant. Errores"] = self.v_metadata.get("Cant. Errores", 0) + 1
        with self.smb_resource.client.open_file(
            path=current_file_path.parent.joinpath("logs_carga.txt"), mode="a"
        ) as file:
            file.write(log_message + "\n")

    def _handle_unknown_error(
        self,
        error: Exception | BaseException,
        current_file_path: PureWindowsPath,
        current_file_key: str,
        rows_inserted: int,
    ):
        log_message = (
            f"ERROR, {'CARGADO' if rows_inserted > 0 else 'NO CARGADO'} en {env_str}, {datetime.now().isoformat()}, "
            + f"Archivo {current_file_key} error {str(error)}.\n"
        )
        self.v_metadata["Archivos"][current_file_key]["Error"] = (
            f"{error.__repr__() + f' Linea: {str(error.__traceback__.tb_lineno)}' if error.__traceback__ else ''}"
        )
        self.v_metadata["Cant. Errores"] = self.v_metadata.get("Cant. Errores", 0) + 1
        with self.smb_resource.client.open_file(
            path=current_file_path.parent.joinpath("logs_carga.txt"), mode="a"
        ) as file:
            file.write(log_message)

    def _add_metadata_columns(self, df: pl.DataFrame, file_descriptor) -> pl.DataFrame:
        return df.with_columns(
            pl.lit(clean_filename(file_descriptor.name)).alias(self.filename_column),
            pl.lit(datetime.now()).alias(self.date_loaded_column),
            pl.lit(datetime.now()).alias(self.date_updated_column),
        )
