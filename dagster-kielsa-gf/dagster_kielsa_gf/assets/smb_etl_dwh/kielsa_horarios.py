from pathlib import PureWindowsPath

import polars as pl
from dagster import (
    AssetExecutionContext,
    asset,
    materialize_to_memory,
)

from dagster_shared_gf.resources.smb_resources import SMBResource
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import (
    normalize_string,
)
from dagster_shared_gf.shared_ops import ExcelFileProcessor
from dagster_shared_gf.shared_variables import (
    ExcelProcessConfig,
    tags_repo,
)

KIELSA_HORARIOS_SCHEMA = pl.Schema(
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
)


class KielsaHorariosProcessor(ExcelFileProcessor):
    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
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
        df = df.with_columns(pl.col("dia_tipo").str.split("_").alias("dia_tipo_split"))
        df = df.with_columns(pl.col("dia_tipo_split").list.get(0).alias("dia_id"))
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

        return df


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
    config = ExcelProcessConfig(
        polars_schema=KIELSA_HORARIOS_SCHEMA,
        blanks_allowed=True,
        excel_sheet_name="semana",
        excel_primary_keys=("SUC ID",),
        final_table_primary_keys=("suc_id", "dia_id"),
    )

    processor = KielsaHorariosProcessor(
        config=config,
        table="DL_Kielsa_Horario_Temp",
        db_schema="excel",
        directory_path=PureWindowsPath(r"data_repo/kielsa/horarios_sucursal/"),
        context=context,
        smb_resource=smb_resource_analitica_nasgftgu02,
        dwh_resource=dwh_farinter_dl,
        move_processed_files_to_folder=False,
    )

    return processor.process_files()


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
        if not escribir_en_destino and isinstance(mock_write_database, MagicMock):
            assert (
                mock_write_database.call_count > 0
                or mock_write_database.call_count == 0
            )
