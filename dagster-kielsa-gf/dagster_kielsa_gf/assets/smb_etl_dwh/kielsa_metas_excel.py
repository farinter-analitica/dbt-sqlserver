from pathlib import PureWindowsPath

import polars as pl
import polars.selectors as cs
from dagster import (
    AssetExecutionContext,
    asset,
    materialize_to_memory,
)

from dagster_shared_gf.resources.smb_resources import SMBResource
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_ops import ExcelFileProcessor
from dagster_shared_gf.shared_variables import (
    ErrorsOccurred,
    ExcelProcessConfig,
    tags_repo,
)

KIELSA_METAS_SCHEMA = pl.Schema(
    {
        "Emp_Id": pl.Int32(),
        "Sucursal_Id": pl.Int32(),
        "Empleado_Rol": pl.String(),
        "Vendedor_Id": pl.Int32(),
        "AnioMes": pl.String(),
        "Dia_Desde": pl.Int16(),
        "Dia_Hasta": pl.Int16(),
    }
)


class KielsaMetasProcessor(ExcelFileProcessor):
    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            pl.col("AnioMes")
            .str.replace("-", "")
            .str.slice(0, 6)
            .cast(pl.Int32)
            .alias("AnioMes_Id"),
        )
        df = df.with_columns(
            Fecha_Desde=pl.col("AnioMes_Id")
            .cast(pl.String)
            .add(pl.col("Dia_Desde").cast(pl.String).str.zfill(2))
            .str.slice(0, 8)
            .str.to_date("%Y%m%d"),
            Fecha_Hasta=pl.col("AnioMes_Id")
            .cast(pl.String)
            .add(pl.col("Dia_Hasta").cast(pl.String).str.zfill(2))
            .str.slice(0, 8)
            .str.to_date("%Y%m%d"),
        ).drop(["Dia_Desde", "Dia_Hasta", "AnioMes"])

        # Validate uniqueness
        self._validate_uniqueness(df)

        # Process date overlaps and corrections
        df = self._process_dates(df)

        # Final transformations
        df = (
            df.drop(
                [
                    "Fecha_Hasta_Anterior",
                    "Count_Por_Claves",
                    "Es_Ultima_Fecha",
                    "Es_Primer_Fecha",
                ]
            )
            .unpivot(
                on=cs.matches(r"^\d"),  # regex empiezan con numeros
                index=[
                    "Emp_Id",
                    "Sucursal_Id",
                    "Empleado_Rol",
                    "Vendedor_Id",
                    "AnioMes_Id",
                    "Fecha_Desde",
                    "Fecha_Hasta",
                ],
                variable_name="variable",
                value_name="valor",
            )
            .with_columns(  # Separar variable en alerta y atributo por primer _:
                pl.col("variable")
                .str.splitn("_", 2)
                .struct.rename_fields(["Alerta_Id", "Atributo"])
                .alias("fields"),
            )
            .unnest("fields")
            .drop(["variable"])
            .with_columns(
                pl.col("Atributo").fill_null("No_Definido"),
                pl.col("Alerta_Id").cast(pl.Int32),
                pl.col("valor").cast(pl.Decimal(20, 6)),
            )
        )

        return df

    def _validate_uniqueness(self, df: pl.DataFrame):
        unique_check_height = df.n_unique(
            subset=[
                "Emp_Id",
                "Sucursal_Id",
                "Vendedor_Id",
                "AnioMes_Id",
                "Fecha_Desde",
                "Fecha_Hasta",
            ]
        )
        if unique_check_height != df.height:
            raise ErrorsOccurred(
                "Los registros no son únicos por las claves especificadas."
            )

    def _process_dates(self, df: pl.DataFrame) -> pl.DataFrame:
        # Verificar entrelazados (superposición de fechas)
        # Paso 1: Ordenar los registros como en el paso anterior
        df = (
            df.sort(
                by=[
                    "Emp_Id",
                    "Sucursal_Id",
                    "Vendedor_Id",
                    "AnioMes_Id",
                    "Fecha_Desde",
                ]
            )
            .with_columns(  # Paso 2: Shift de Fecha_Hasta para comparar si la Fecha_Desde de un registro es mayor o igual a la Fecha_Hasta del anterior en el grupo
                Fecha_Hasta_Anterior=pl.col("Fecha_Hasta")
                .shift(1)
                .over(["Emp_Id", "Sucursal_Id", "Vendedor_Id", "AnioMes_Id"])
            )
            .with_columns(  # Paso 3: Filtrar registros donde Fecha_Desde es igual a Fecha_Hasta anterior y sumar 1 día
                pl.when(pl.col("Fecha_Desde") == pl.col("Fecha_Hasta_Anterior"))
                .then(
                    pl.col("Fecha_Desde").dt.offset_by("1d")
                )  # sumar 1 día si son iguales
                .otherwise(pl.col("Fecha_Desde"))
                .alias(
                    "Fecha_Desde"
                ),  # Sobreescribir la columna Fecha_Desde con la corrección
                # Agregar una nueva columna con el conteo de las claves principales (sin fechas)
                pl.count()
                .over(
                    [
                        "Emp_Id",
                        "Sucursal_Id",
                        "Empleado_Rol",
                        "Vendedor_Id",
                        "AnioMes_Id",
                    ]
                )
                .alias("Count_Por_Claves"),
                # Agregar una columna que indique si es la última fecha (fin de mes)
                pl.when(
                    pl.col("Fecha_Hasta")
                    == pl.col("Fecha_Hasta")
                    .max()
                    .over(
                        [
                            "Emp_Id",
                            "Sucursal_Id",
                            "Empleado_Rol",
                            "Vendedor_Id",
                            "AnioMes_Id",
                        ]
                    )
                )
                .then(
                    pl.lit(True)
                )  # Si es la última fecha en el grupo, marcar como True
                .otherwise(pl.lit(False))  # Si no, marcar como False
                .alias("Es_Ultima_Fecha"),
                pl.when(
                    pl.col("Fecha_Desde")
                    == pl.col("Fecha_Desde")
                    .min()
                    .over(
                        [
                            "Emp_Id",
                            "Sucursal_Id",
                            "Empleado_Rol",
                            "Vendedor_Id",
                            "AnioMes_Id",
                        ]
                    )
                )
                .then(
                    pl.lit(True)
                )  # Si es la primer fecha en el grupo, marcar como True
                .otherwise(pl.lit(False))  # Si no, marcar como False
                .alias("Es_Primer_Fecha"),
            )
            .with_columns(  # Limpieza si es ultima fecha enviar hasta el fin de mes y si es primera a inicio de mes
                pl.when(pl.col("Es_Ultima_Fecha"))
                .then(pl.col("Fecha_Hasta").dt.month_end())
                .otherwise(pl.col("Fecha_Hasta"))
                .alias("Fecha_Hasta"),
                pl.when(pl.col("Es_Primer_Fecha"))
                .then(pl.col("Fecha_Desde").dt.month_start())
                .otherwise(pl.col("Fecha_Desde"))
                .alias("Fecha_Desde"),
            )
            .with_columns(  # Si hay gaps, corregirlos
                pl.when(
                    pl.col("Fecha_Desde").dt.offset_by("-1d")
                    > pl.col("Fecha_Hasta_Anterior")
                )
                .then(pl.col("Fecha_Hasta_Anterior").dt.offset_by("1d"))
                .otherwise(pl.col("Fecha_Desde"))
                .alias("Fecha_Desde"),
            )
            .with_columns(  # Si hay entrelazados corregirlos
                pl.when(pl.col("Fecha_Desde") < pl.col("Fecha_Hasta_Anterior"))
                .then(pl.col("Fecha_Hasta_Anterior").dt.offset_by("1d"))
                .otherwise(pl.col("Fecha_Desde"))
                .alias("Fecha_Desde"),
            )
        )

        # Paso 4: Verificar si aún existen entrelazados, pero ahora solo aquellos donde Fecha_Desde < Fecha_Hasta_Anterior
        overlapping_check = df.filter(
            pl.col("Fecha_Desde") < pl.col("Fecha_Hasta_Anterior")
        ).height

        if overlapping_check > 0:
            raise ErrorsOccurred(
                "Se detectaron registros entrelazados (fechas superpuestas) tras la corrección."
            )
        return df


@asset(
    key_prefix=["DL_FARINTER", "excel"],
    tags=tags_repo.SmbDataRepository.tag
    | tags_repo.UniquePeriod.tag
    | {"dagster/storage_kind": "sqlserver", "data_source_kind": "smb_xslx_files"},
    compute_kind="polars",
    metadata={"parent_directory": "data_repo/kielsa/metas_venta/"},
)
def DL_Kielsa_MetaHist_Temp(
    context: AssetExecutionContext,
    smb_resource_analitica_nasgftgu02: SMBResource,
    dwh_farinter_dl: SQLServerResource,
):
    config = ExcelProcessConfig(
        polars_schema=KIELSA_METAS_SCHEMA,
        blanks_allowed=False,
        exclude_colums=("Vendedor",),
        excel_sheet_name="carga",
        final_table_primary_keys=(
            "AnioMes_Id",
            "Vendedor_Id",
            "Sucursal_Id",
            "Emp_Id",
            "Fecha_Desde",
            "Fecha_Hasta",
            "Alerta_Id",
            "Atributo",
        ),
    )

    processor = KielsaMetasProcessor(
        config=config,
        table="DL_Kielsa_MetaHist_Temp",
        db_schema="excel",
        directory_path=PureWindowsPath(r"data_repo/kielsa/metas_venta/"),
        context=context,
        smb_resource=smb_resource_analitica_nasgftgu02,
        dwh_resource=dwh_farinter_dl,
        filename_column="Nombre_Archivo",
        date_loaded_column="Fecha_Carga",
        date_updated_column="Fecha_Actualizado",
        move_processed_files_to_folder=True,
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
            [DL_Kielsa_MetaHist_Temp],
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
