import json
from types import TracebackType
from unittest.mock import MagicMock, patch
from dagster import (asset
                     , AssetChecksDefinition 
                     , AssetExecutionContext
                     , build_last_update_freshness_checks
                     , load_assets_from_current_module
                     , load_asset_checks_from_current_module
                     , Config
                     , MaterializeResult
                     , materialize_to_memory
                     )
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource 
from dagster_shared_gf.resources.smb_resources import SMBResource
from dagster_shared_gf.shared_variables import env_str, TagsRepositoryGF as tags_repo
from dagster_shared_gf.shared_functions import filter_assets_by_tags, clean_filename, clean_string_to_key
from pathlib import PureWindowsPath
from pydantic import Field
from typing import Any
from collections.abc import    Mapping, Sequence, Iterator
from datetime import datetime, date, timedelta
import polars as pl, re
from io import BytesIO
from ydata_profiling import ProfileReport

##
class ExcelSchemaConfig(Config):
    expected_columns: dict[str, str] = Field(description="Columns New Name : Column File Name", default_factory=dict)
    polars_schema: dict[str, pl.DataType | Any] = Field(description="polars_schema", default_factory=dict)
    exclude_colums: list[str] = Field(description="Exclude columns", default_factory=list)
    blanks_allowed: bool = Field(description="Allow blanks", default=True)
    blanks_on_type_error: bool = Field(description="Convert type error to blanks", default=False)

@asset(
    key_prefix=["DL_FARINTER", "excel"],
    tags=tags_repo.SmbDataRepository.tag | {"dagster/storage_kind": "sqlserver", "data_source_kind": "smb_xslx_files"},
    compute_kind="polars",
    metadata={"parent_directory": "data_repo/kielsa/metas_venta/"},
)
def DL_Kielsa_MetaHist_Temp(context: AssetExecutionContext, smb_resource_analitica_nasgftgu02: SMBResource, dwh_farinter_dl: SQLServerResource):
    ###INICIO DE PREPARACION DE PARAMETROS
    table = "DL_Kielsa_MetaHist_Temp"
    database = "DL_FARINTER"
    schema = "excel"
    directory_path = PureWindowsPath(r"data_repo/kielsa/metas_venta/")
    schema_config = ExcelSchemaConfig(
        polars_schema={
            "Emp_Id": pl.Int32,
            "Sucursal_Id": pl.Int32,
            "Empleado_Rol": pl.String,
            "Vendedor_Id": pl.Int32,
            "AnioMes": pl.String,
            "Dia_Desde": pl.Int16,
            "Dia_Hasta": pl.Int16,
        },
        blanks_allowed=False,
        exclude_colums=["Vendedor"]
    )
    ###FIN DE PREPARACION DE PARAMETROS
    drop_table_count = 0
    class NullsException(BaseException):
        pass
    class FileException(BaseException):
        pass
    class ErrorsOccurred(BaseException):
        pass
    smb_resource = smb_resource_analitica_nasgftgu02 #context.resources.smb_resource_analitica_nasgftgu02
    v_metadata = {"Archivos": {}}
    try:
        for file_descriptor in smb_resource.get_server_dirs(directory=directory_path, extension=".xlsx", exclude=["cargados", "plantilla.xlsx"]):
            rows_inserted = 0
            nulls_count = 0
            # ignore non excel files xlsx
            try:
                df: pl.DataFrame
                dfd: dict[str, pl.DataFrame]
                current_file_path = smb_resource.get_full_server_path(file_descriptor.path)
                current_file_key = clean_string_to_key(current_file_path.relative_to(smb_resource.get_full_server_path(directory_path)))
                v_metadata["Archivos"][current_file_key] = {}
                with smb_resource.open_server_file(file_path=current_file_path, mode="rb") as file:
                    file_content = BytesIO(file.read())
                    dfd = pl.read_excel(file_content
                                    , sheet_id=0
                                    #, sheet_name='carga'
                                    , infer_schema_length=0
                                    #, columns=list(schema_config.expected_columns.values())
                                    , engine="calamine"
                                )
                    sheet_name_pattern = re.compile(r'\bcarga.*', re.IGNORECASE)

                    # Filtering sheets whose names match the pattern
                    for sheet_name in dfd.keys():
                        if sheet_name_pattern.match(sheet_name):
                            df = dfd[sheet_name]
                            break
                    else:
                        raise FileException(
                            f"No se encontro una hoja con el patron {sheet_name_pattern.pattern}"
                        )
                ###INICIO DE TRANSFORMACIONES ESPECIFICAS
                df=df.cast(schema_config.polars_schema)
                df=df.drop(schema_config.exclude_colums, strict=False)
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
                ).drop(["Dia_Desde", "Dia_Hasta"])
                context.log.debug(df)

                # Validar unicidad
                unique_check_height = df.n_unique(subset=["Emp_Id", "Sucursal_Id", "Vendedor_Id", "AnioMes_Id", "Fecha_Desde", "Fecha_Hasta"])

                if unique_check_height != df.height:
                    raise ErrorsOccurred("Los registros no son únicos por las claves especificadas.")

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
                        pl.when(pl.col("Fecha_Desde").dt.offset_by("-1d") > pl.col("Fecha_Hasta_Anterior"))
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
                with pl.Config(tbl_cols=-1):
                    context.log.debug(df.head(5))

                # Paso 4: Verificar si aún existen entrelazados, pero ahora solo aquellos donde Fecha_Desde < Fecha_Hasta_Anterior
                overlapping_check = df.filter(
                    pl.col("Fecha_Desde") < pl.col("Fecha_Hasta_Anterior")
                ).height

                if overlapping_check > 0:
                    raise ErrorsOccurred("Se detectaron registros entrelazados (fechas superpuestas) tras la corrección.")
               

                df=df.drop(["Fecha_Hasta_Anterior", "Count_Por_Claves", "Es_Ultima_Fecha"])

                df = (
                    df.unpivot(
                        index=["Emp_Id", "Sucursal_Id", "Empleado_Rol", "Vendedor_Id", "AnioMes_Id", "Fecha_Desde", "Fecha_Hasta"],
                        variable_name="variable",
                        value_name="valor",
                    )
                    .with_columns(  # Separar variable en alerta y atributo por primer _:
                        pl.col("variable")
                        .str.splitn("_", 2)
                        .struct.rename_fields(["Alerta_Id", "Atributo"])
                        .alias("fields"),
                        Nombre_Archivo=pl.lit(clean_filename(file_descriptor.name)),
                        Fecha_Carga=pl.lit(datetime.now()),
                        Fecha_Actualizado=pl.lit(datetime.now()),
                    )
                    .unnest("fields")
                    .drop(["variable"])
                    .with_columns(
                        pl.col("Atributo").fill_null("No Definido"),
                    )
                )

                context.log.debug(df.get_column("Atributo").value_counts().head(20))

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
                    raise NullsException(f"Archivo {current_file_key} tiene {nulls_count} valores en Blanco.")
                else:
                    df.fill_null(strategy='zero')
                # cargar en la db
                with dwh_farinter_dl.get_sqlalchemy_conn() as conn:
                    if drop_table_count == 0:
                        conn.execute( dwh_farinter_dl.text(f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL BEGIN DROP TABLE {schema}.{table} END; "))
                        drop_table_count += 1
                    conn.commit()
                    rows_inserted = df.write_database(table_name=f"{schema}.{table}", connection=conn, if_table_exists="append")

                with smb_resource.open_server_file(file_path=current_file_path.parent.joinpath("logs_carga.txt"), mode="a") as file:
                    file.write(f"INFO, CARGADO, {datetime.now().isoformat()} , Archivo {current_file_key} cargado con {row_count} filas.\n")

                if env_str in ["prd"]:
                    smb_resource.move_server_file(
                        file_path=current_file_path,
                        new_path=current_file_path.parent.joinpath("cargados").joinpath(
                            clean_filename(file_descriptor.name)
                        ),
                    )
                v_metadata["Archivos"][current_file_key]["Cargado"] = True

                v_metadata["Cant. Archivos Cargados"] = v_metadata.get("Cant. Archivos Cargados", 0) + 1

            except (NullsException, FileException, ErrorsOccurred) as ne:
                context.log.error(ne)
                log_message = (f"ERROR, NO CARGADO en {env_str}, {datetime.now().isoformat()}, " +
                            f"Archivo {current_file_key} error { str(ne) }.")
                v_metadata["Archivos"][current_file_key]["Error"] = log_message
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1
                with smb_resource.open_server_file(file_path=current_file_path.parent.joinpath("logs_carga.txt"), mode="a") as file:
                    file.write(log_message + f"\n")
            except Exception as e:
                log_message = (f"ERROR, {'CARGADO' if rows_inserted > 0 else 'NO CARGADO'} en {env_str}, {datetime.now().isoformat()}, " +
                            f"Archivo {current_file_key} error { str(e) }.\n")
                v_metadata["Archivos"][current_file_key]["Error"] = f"{e.__repr__() + f" Linea: {str(e.__traceback__.tb_lineno)}" if e.__traceback__ else '' }"
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1
                with smb_resource.open_server_file(file_path=current_file_path.parent.joinpath("logs_carga.txt"), mode="a") as file:
                    file.write(log_message)

        if v_metadata.get("Cant. Errores", 0) > 0:
            raise ErrorsOccurred(v_metadata)

    except (Exception, ErrorsOccurred) as e:
        context.log.info("log de carga de archivos:" + str(v_metadata))
        log_message = (f"ERROR, N/A en {env_str}, {datetime.now().isoformat()}, { str(e) }\n")
        with smb_resource.open_server_file(file_path=directory_path.joinpath("logs_carga.txt"), mode="a") as file:
            file.write(log_message)
        raise e    
    return MaterializeResult(metadata=v_metadata)


if __name__ == '__main__':
    from dagster_shared_gf.resources.smb_resources import smb_resource_analitica_nasgftgu02
    with patch("polars.DataFrame.write_database", MagicMock(return_value=MagicMock())) as mock_write_database:
        materialize_to_memory([DL_Kielsa_MetaHist_Temp], resources={"smb_resource_analitica_nasgftgu02": smb_resource_analitica_nasgftgu02, "dwh_farinter_dl": MagicMock()})
        assert mock_write_database.call_count > 0


else:
    all_assets = load_assets_from_current_module(group_name="smb_etl_dwh")

    all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
        assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
        lower_bound_delta=timedelta(hours=26),
        deadline_cron="0 9 * * 1-6",
    )
    all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
        assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
        lower_bound_delta=timedelta(hours=13),
        deadline_cron="0 10-16 * * 1-6",
    )

    all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
    all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
