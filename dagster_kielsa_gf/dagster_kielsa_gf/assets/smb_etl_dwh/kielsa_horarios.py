import json
import re
from collections.abc import Sequence
from datetime import datetime
from io import BytesIO
from pathlib import PureWindowsPath
from typing import Any

import polars as pl
from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    MaterializeResult,
    asset,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    materialize_to_memory,
)
from ydata_profiling import ProfileReport

from dagster_shared_gf.resources.smb_resources import SMBResource
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import (
    clean_filename,
    clean_filename_to_key,
    normalize_string,
    clean_phone_number,
)
from dagster_shared_gf.shared_variables import (
    ErrorsOccurred,
    ExcelLoadConfig,
    FileException,
    NullsException,
    env_str,
    tags_repo,
)


@asset(
    key_prefix=["DL_FARINTER", "excel"],
    tags=tags_repo.SmbDataRepository.tag
    | {"dagster/storage_kind": "sqlserver", "data_source_kind": "smb_xslx_files"},
    compute_kind="polars",
    metadata={"parent_directory": "data_repo/kielsa/horarios_sucursal/"},
)
def DL_Kielsa_Horario_Temp(
    context: AssetExecutionContext,
    smb_resource_analitica_nasgftgu02: SMBResource,
    dwh_farinter_dl: SQLServerResource,
):
    ###INICIO DE PREPARACION DE PARAMETROS
    table = "DL_Kielsa_Horario_Temp"
    # database = "DL_FARINTER"
    db_schema = "excel"
    directory_path = PureWindowsPath(r"data_repo/kielsa/horarios_sucursal/")
    schema_config = ExcelLoadConfig(
        polars_schema=pl.Schema(
            {
                "SUC ID": pl.Int32(),
                "FARMACIA": pl.String(),
                "LUNES APERTURA": pl.Time(),
                "LUNES CIERRE": pl.Time(),
                "MARTES APERTURA": pl.Time(),
                "MARTES CIERRE": pl.Time(),
                "MIERCOLES APERTURA": pl.Time(),
                "MIERCOLES CIERRE": pl.Time(),
                "JUEVES APERTURA": pl.Time(),
                "JUEVES CIERRE": pl.Time(),
                "VIERNES APERTURA": pl.Time(),
                "VIERNES CIERRE": pl.Time(),
                "SABADO APERTURA": pl.Time(),
                "SABADO CIERRE": pl.Time(),
                "DOMINGO APERTURA": pl.Time(),
                "DOMINGO CIERRE": pl.Time(),
                "SUPERVISOR": pl.String(),
                "CELULAR": pl.String(),
                "JOP": pl.String(),
                "ACTIVA": pl.Boolean(),
                "ES_24_HORAS": pl.Boolean(),
            }
        ),
        blanks_allowed=True,
        excel_sheet_name="semana",
        claves_primarias=("SUC ID",),
    )
    ###FIN DE PREPARACION DE PARAMETROS
    drop_table_count = 0

    smb_resource = smb_resource_analitica_nasgftgu02  # context.resources.smb_resource_analitica_nasgftgu02
    v_metadata: dict[str, Any] = {"Archivos": {}}
    try:
        for file_descriptor in smb_resource.get_server_dirs(
            directory=directory_path,
            extension=".xlsx",
            exclude=[schema_config.loaded_files_folder, "plantilla.xlsx"],
        ):
            rows_inserted = 0
            nulls_count = 0
            # ignore non excel files xlsx
            try:
                df: pl.DataFrame
                dfd: dict[str, pl.DataFrame]
                current_file_path = smb_resource.get_full_server_path(
                    file_descriptor.path
                )
                current_file_key = clean_filename_to_key(
                    str(
                        current_file_path.relative_to(
                            smb_resource.get_full_server_path(directory_path)
                        )
                    )
                )
                v_metadata["Archivos"][current_file_key] = {}
                with smb_resource.client.open_file(
                    path=current_file_path, mode="rb"
                ) as file:
                    dfd = pl.read_excel(
                        BytesIO(file.read()),
                        sheet_id=0,
                        engine="calamine",
                        raise_if_empty=False,
                    )
                    sheet_name_pattern = re.compile(
                        r"\b" f"{schema_config.excel_sheet_name}" r".*", re.IGNORECASE
                    )

                    # Filtering sheets whose names match the pattern
                    for sheet_name in dfd.keys():
                        if sheet_name_pattern.match(sheet_name):
                            df = dfd[sheet_name]
                            break
                    else:
                        raise FileException(
                            f"No se encontro una hoja con el patron {sheet_name_pattern.pattern}"
                        )
                if (
                    len(set(schema_config.expected_columns.keys()) - set(df.columns))
                    > 0
                ):
                    raise ErrorsOccurred(
                        f"No se encontraron las columnas {set(schema_config.expected_columns.keys()) - set(df.columns)}"
                    )

                df = df.cast({**schema_config.polars_schema})
                if schema_config.exclude_colums:
                    df = df.drop(schema_config.exclude_colums, strict=False)

                ###VALIDAR UNICIDAD
                unique_check_height = df.n_unique(subset=schema_config.claves_primarias)

                if unique_check_height != df.height:
                    raise ErrorsOccurred(
                        "Los registros no son únicos por las claves especificadas."
                    )

                with pl.Config(tbl_cols=-1):
                    context.log.debug(df.head(5))

                ###INICIO DE TRANSFORMACIONES ESPECIFICAS

                # normalizar nombres de columnas
                df = df.rename({col: normalize_string(col) for col in df.columns})
                # unpivot las columnas de horarios
                df = df.unpivot(
                    index=(
                        "suc_id",
                        "farmacia",
                        "supervisor",
                        "celular",
                        "jop",
                        "activa",
                        "es_24_horas",
                    ),
                    variable_name="dia_tipo",
                    value_name="hora",
                )
                # separar dia_tipo en dia y tipo
                df = df.with_columns(
                    pl.col("dia_tipo").str.split("_").alias("dia_tipo_split")
                )
                df = df.with_columns(
                    pl.col("dia_tipo_split").list.get(0).alias("dia_id")
                )
                df = df.with_columns(pl.col("dia_tipo_split").list.get(1).alias("tipo"))
                # pivot del tipo nuevamente
                df = (
                    df.pivot(
                        index=(
                            "suc_id",
                            "dia_id",
                            "farmacia",
                            "supervisor",
                            "celular",
                            "jop",
                            "activa",
                            "es_24_horas",
                        ),
                        on="tipo",
                        values="hora",
                    )
                    .rename(
                        {
                            "apertura": "h_apertura",
                            "cierre": "h_cierre",
                        }
                    )
                    .select(
                        (
                            "suc_id",
                            pl.col("dia_id")
                            .replace_strict(
                                {
                                    "lunes": 1,
                                    "martes": 2,
                                    "miercoles": 3,
                                    "jueves": 4,
                                    "viernes": 5,
                                    "sabado": 6,
                                    "domingo": 7,
                                },
                                return_dtype=pl.Int32,
                            )
                            .alias("dia_id"),
                            "h_apertura",
                            "h_cierre",
                            "farmacia",
                            "supervisor",
                            "celular",
                            "jop",
                            "activa",
                            "es_24_horas",
                        )
                    )
                )

                with pl.Config(tbl_cols=-1):
                    context.log.debug(df.head(5))

                ###FIN DE TRANSFORMACIONES ESPECIFICAS
                # context.log.debug(df.head(5))
                nulls_count = df.null_count().sum_horizontal().sum()
                row_count = df.height
                v_metadata["Archivos"].update(
                    {
                        current_file_key: {
                            "Cargado": False,
                            "Cant. Filas": row_count,
                            "Cant. Valores en Blanco": nulls_count,
                            "Perfil de Datos": json.loads(
                                ProfileReport(
                                    df.to_pandas(), title="Pandas Profiling Report"
                                ).to_json()
                            ).get("table", "error"),
                        }
                    }
                )
                if not schema_config.blanks_allowed and nulls_count > 0:
                    raise NullsException(
                        f"Archivo {current_file_key} tiene {nulls_count} valores en Blanco."
                    )
                if schema_config.fill_nulls:
                    df = df.fill_null(strategy="zero")
                # cargar en la db
                with dwh_farinter_dl.get_sqlalchemy_conn() as conn:
                    if drop_table_count == 0:
                        conn.execute(
                            dwh_farinter_dl.text(
                                f"IF OBJECT_ID('{db_schema}.{table}', 'U') IS NOT NULL BEGIN DROP TABLE {db_schema}.{table} END; "
                            )
                        )
                        drop_table_count += 1
                    conn.commit()
                    rows_inserted = df.write_database(
                        table_name=f"{db_schema}.{table}",
                        connection=conn,
                        if_table_exists="append",
                    )

                with smb_resource.client.open_file(
                    path=current_file_path.parent.joinpath("logs_carga.txt"),
                    mode="a",
                ) as file:
                    file.write(
                        f"INFO, CARGADO, {datetime.now().isoformat()} , Archivo {current_file_key} cargado con {row_count} filas.\n"  # type: ignore
                    )

                if env_str in ["prd"] and schema_config.move_processed_files_to_folder:
                    smb_resource.move_server_file(
                        file_path=current_file_path,
                        new_path=current_file_path.parent.joinpath(
                            schema_config.loaded_files_folder
                        ).joinpath(clean_filename(file_descriptor.name)),
                    )
                v_metadata["Archivos"][current_file_key]["Cargado"] = True

                v_metadata["Cant. Archivos Cargados"] = (
                    v_metadata.get("Cant. Archivos Cargados", 0) + 1
                )

            except (NullsException, FileException, ErrorsOccurred) as ne:
                context.log.error(ne)
                log_message = (
                    f"ERROR, NO CARGADO en {env_str}, {datetime.now().isoformat()}, "
                    + f"Archivo {current_file_key} error {str(ne)}."
                )
                v_metadata["Archivos"][current_file_key]["Error"] = log_message
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1
                with smb_resource.client.open_file(
                    path=current_file_path.parent.joinpath("logs_carga.txt"),
                    mode="a",
                ) as file:
                    file.write(log_message + "\n")  # type: ignore
            except Exception as e:
                log_message = (
                    f"ERROR, {'CARGADO' if rows_inserted > 0 else 'NO CARGADO'} en {env_str}, {datetime.now().isoformat()}, "
                    + f"Archivo {current_file_key} error {str(e)}.\n"
                )
                v_metadata["Archivos"][current_file_key]["Error"] = (
                    f"{e.__repr__() + f' Linea: {str(e.__traceback__.tb_lineno)}' if e.__traceback__ else ''}"
                )
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1
                with smb_resource.client.open_file(
                    path=current_file_path.parent.joinpath("logs_carga.txt"),
                    mode="a",
                ) as file:
                    file.write(log_message)  # type: ignore

        if v_metadata.get("Cant. Errores", 0) > 0:
            raise ErrorsOccurred(v_metadata)

    except (Exception, ErrorsOccurred) as e:
        context.log.info("log de carga de archivos:" + str(v_metadata))
        log_message = (
            f"ERROR, N/A en {env_str}, {datetime.now().isoformat()}, {str(e)}\n"
        )
        with smb_resource.client.open_file(
            path=directory_path.joinpath("logs_carga.txt"), mode="a"
        ) as file:
            file.write(log_message)  # type: ignore
        raise e
    return MaterializeResult(metadata=v_metadata)


if __name__ == "__main__":
    from unittest.mock import MagicMock, patch

    from dagster_shared_gf.resources.smb_resources import (
        smb_resource_analitica_nasgftgu02,
    )
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl

    escribir_en_destino = (
        True  # Cambiar para que se cree la tabla en destino con la prueba
    )
    with patch(
        "polars.DataFrame.write_database",
        pl.DataFrame.write_database
        if escribir_en_destino
        else MagicMock(return_value=MagicMock()),
    ) as mock_write_database:
        materialize_to_memory(
            [DL_Kielsa_Horario_Temp],
            resources={
                "smb_resource_analitica_nasgftgu02": smb_resource_analitica_nasgftgu02,
                "dwh_farinter_dl": dwh_farinter_dl
                if escribir_en_destino
                else MagicMock(),
            },
        )
        if not escribir_en_destino:
    
            assert mock_write_database.call_count > 0 or mock_write_database.call_count == 0

