import json
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
from dagster_shared_gf.shared_functions import filter_assets_by_tags, clean_filename
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
    metadata={"parent_directory": "data_repo/grupo_farinter/presupuesto_ventas_finanzas"},
)
def DL_Finanzas_Presupuesto_Temp(context: AssetExecutionContext, smb_resource_analitica_nasgftgu02: SMBResource, dwh_farinter_dl: SQLServerResource):
    ###INICIO DE PREPARACION DE PARAMETROS
    table = "DL_Finanzas_Presupuesto_Temp"
    database = "DL_FARINTER"
    schema = "excel"
    directory_path = PureWindowsPath("data_repo/grupo_farinter/presupuesto_ventas_finanzas")
    schema_config = ExcelSchemaConfig(
        expected_columns={
            "Sociedad_Id": "Cod Sociedad",
            "Zona_Id": "Cod Territorio",
            "Division_Id": "Division",
            "Articulo_Id": "Cod Material",
            "Cliente_Id": "Cod Cliente",
            "Casa_Id": "Cod Grupo Articulos",
            "Vendedor_Id": "Cod Vendedor",
            "Fecha_Id": "Mes",
            "Monto": "Valor",
        },
        polars_schema={
            "Sociedad_Id": pl.String,
            "Zona_Id": pl.String,
            "Division_Id": pl.String,
            "Articulo_Id": pl.String,
            "Cliente_Id": pl.String,
            "Casa_Id": pl.String,
            "Vendedor_Id": pl.String,
            "Fecha_Id": pl.String,
            "Monto": pl.Decimal(16,4),
        },
    )
    # recopilar division de la base de datos
    dfdc: pl.DataFrame
    with dwh_farinter_dl.get_connection(engine="sqlalchemy") as conn:
        # conn.execute(f"USE {database}; SELECT Division_Id, Division_Nombre FROM dbo.DL_Edit_Division_SAP")
        dfdc = pl.read_database("SELECT Division_Id, Division_Nombre FROM dbo.DL_Edit_Division_SAP", connection=conn)
    dfdc = dfdc.select(["Division_Nombre","Division_Id"])
    division_id_map_reversed: dict = dict(dfdc.rows())        
    context.log.info(dfdc)
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
            try:
                df: pl.DataFrame
                dfd: dict[str, pl.DataFrame]
                current_file_path = smb_resource.get_full_server_path(file_descriptor.path)
                current_file_key = clean_filename(current_file_path.relative_to(smb_resource.get_full_server_path(directory_path)))
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
                df=df.select(**schema_config.expected_columns)
                df=df.cast(schema_config.polars_schema)
                # Reemplazar farma y cualquier otro nombre en division_id por su id y limpieza de datos:
                df=df.with_columns(Division_Id = pl.col("Division_Id").replace(division_id_map_reversed, default="00"),
                                    Zona_Id = pl.col("Zona_Id").str.zfill(6),
                                    Cliente_Id = pl.col("Cliente_Id").str.zfill(10),
                                    Vendedor_Id = pl.col("Vendedor_Id").str.zfill(3),
                                    Articulo_Id = pl.col("Articulo_Id").str.zfill(18),
                                    Fecha_Id = pl.col("Fecha_Id").str.slice(0, 10).str.to_date('%F').cast(pl.Date),
                                    AnioMes_Id = pl.col("Fecha_Id").str.slice(0, 10).str.to_date().dt.to_string("%Y%m").cast(pl.Int32),
                                    Nombre_Archivo = pl.lit(clean_filename(file_descriptor.name)),
                                    Fecha_Carga = pl.lit(datetime.now()),
                                    Fecha_Actualizado = pl.lit(datetime.now()),
                                    )
                ###FIN DE TRANSFORMACIONES ESPECIFICAS
                # context.log.debug(df.head(5))
                nulls_count = df.null_count().sum_horizontal().sum()
                row_count = df.height
                v_metadata["Archivos"].update(
                    {
                        file_descriptor.name: {
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
                with dwh_farinter_dl.get_connection(engine="sqlalchemy") as conn:
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

            except NullsException as ne:
                context.log.error(ne)
                log_message = (f"ERROR, NO CARGADO en {env_str}, {datetime.now().isoformat()}, " +
                            f"Archivo {current_file_key} tiene {nulls_count} valores en Blanco.\n")
                v_metadata["Archivos"][current_file_key]["Error"] = log_message
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1
                with smb_resource.open_server_file(file_path=current_file_path.parent.joinpath("logs_carga.txt"), mode="a") as file:
                    file.write(log_message)
            except FileException as fe:
                context.log.error(fe)
                log_message = (f"ERROR, 'NO CARGADO en {env_str}, {datetime.now().isoformat()}, " +
                            f"Archivo {current_file_key} error {str(fe)}.\n")
                v_metadata["Archivos"][current_file_key]["Error"] = log_message
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1 #
                with smb_resource.open_server_file(file_path=current_file_path.parent.joinpath("logs_carga.txt"), mode="a") as file:
                    file.write(log_message) #
            except Exception as e:
                log_message = (f"ERROR, {'CARGADO' if rows_inserted > 0 else 'NO CARGADO'} en {env_str}, {datetime.now().isoformat()}, " +
                            f"Archivo {current_file_key} error {str(e)}.\n")
                v_metadata["Archivos"][current_file_key]["Error"] = e
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1
                with smb_resource.open_server_file(file_path=current_file_path.parent.joinpath("logs_carga.txt"), mode="a") as file:
                    file.write(log_message)
                
        if v_metadata.get("Cant. Errores", 0) > 0:
            raise ErrorsOccurred(v_metadata)

    except Exception as e:
        context.log.info("log de carga de archivos:" + str(v_metadata))
        log_message = (f"ERROR, N/A en {env_str}, {datetime.now().isoformat()}, { str(e)}\n")
        with smb_resource.open_server_file(file_path=directory_path.joinpath("logs_carga.txt"), mode="a") as file:
            file.write(log_message)
        raise e    
    return MaterializeResult(metadata=v_metadata)

@asset(
    key_prefix=["BI_FARINTER", "dbo"],
    deps=[DL_Finanzas_Presupuesto_Temp],
    compute_kind="sqlserver",
    tags={"dagster/storage_kind": "sqlserver"},
)
def BI_SAP_Hecho_PresupuestoHist(context: AssetExecutionContext, dwh_farinter_bi: SQLServerResource):
    with dwh_farinter_bi.get_connection(engine="sqlalchemy") as conn:
        conn.execute(dwh_farinter_bi.text(f"EXEC BI_FARINTER.dbo.BI_paCargarSAP_Hecho_PresupuestoHist")) 

if __name__ == '__main__':
    from dagster_shared_gf.resources.smb_resources import smb_resource_analitica_nasgftgu02
    with patch("polars.DataFrame.write_database", MagicMock(return_value=MagicMock())) as mock_write_database:
        materialize_to_memory([DL_Finanzas_Presupuesto_Temp], resources={"smb_resource_analitica_nasgftgu02": smb_resource_analitica_nasgftgu02, "dwh_farinter_dl": MagicMock()})
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

