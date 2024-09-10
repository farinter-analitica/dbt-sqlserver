import json
from unittest.mock import MagicMock, patch
from dagster import (asset
                     , AssetChecksDefinition 
                     , AssetExecutionContext, build_op_context, build_resources
                     , op 
                     , OpExecutionContext
                     , build_last_update_freshness_checks
                     , build_asset_context
                     , load_assets_from_current_module
                     , load_asset_checks_from_current_module
                     , Config
                     , Output
                     , MaterializeResult
                     , materialize_to_memory
                     )
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource 
from dagster_shared_gf.resources.smb_resources import SMBResource
from dagster_shared_gf.shared_variables import env_str, TagsRepositoryGF as tags_repo
from dagster_shared_gf.shared_functions import filter_assets_by_tags, clean_filename
import dagster_sap_gf.assets.dbt_dwh_sap as dbt_dwh_sap
from pathlib import PurePath
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

def directory_files(directory: PurePath, smb_resource: SMBResource) -> Iterator[SMBResource.SMBDirEntry]:
    directory_path = PurePath(f"//{smb_resource.server_ip}").joinpath(directory)
    return smb_resource.scandir(directory_path)

def open_file(file_path: PurePath, smb_resource: SMBResource
              , mode: str="rb"):
    """
    A function to open a file using the provided file path, SMB resource, and mode.
    
    Args:
        file_path (Path): The path to the file to be opened.
        smb_resource (SMBResource): The SMB resource used to access the file.
        mode (str, optional): The mode in which the file should be opened. Defaults to "rb".
        Open Modes:
                    'r': Open for reading (default).
                    'w': Open for writing, truncating the file first.
                    'x': Open for exclusive creation, failing if the file already exists.
                    'a': Open for writing, appending to the end of the file if it exists.
                    '+': Open for updating (reading and writing), can be used in conjunction with any of the above. Open Type - can be specified with the OpenMode
                    't': Text mode (default).
                    'b': Binary mode.
    Returns:
        The opened file using the specified mode.
    """
    file_path = PurePath(f"//{smb_resource.server_ip}").joinpath(file_path)
    return smb_resource.open_file(path=file_path, mode=mode)

def move_file(context: AssetExecutionContext, file_path: PurePath, smb_resource: SMBResource, new_path: PurePath):
    def get_unique_dst_path(dst_path: PurePath):
        counter = 1
        new_dst_path = dst_path
        
        # Check if file already exists
        while smb_resource.path.exists(new_dst_path):
            new_dst_path = dst_path.with_name(f"{dst_path.stem}_{counter}{dst_path.suffix}")
            counter += 1
            
        return new_dst_path
    file_path = PurePath(f"//{smb_resource.server_ip}").joinpath(file_path)
    new_path = PurePath(f"//{smb_resource.server_ip}").joinpath(new_path)
    #if exists add a number
    new_path = get_unique_dst_path(new_path)
    context.log.info(f"Moving {str(file_path.as_posix())} to {str(new_path.as_posix())}")
    #smbsession.makedirs(new_path.parent, exist_ok=True)
    smb_resource.renames(file_path.as_posix(),new_path.as_posix())

def get_files_dirs(directory: PurePath, smb_resource: SMBResource, recursive_depth: int | None = None, extension: str = ".xlsx", exclude: list[str] | None = None) -> Iterator[SMBResource.SMBDirEntry]:
    def _scan_dir(current_dir: PurePath, current_depth: int) -> Iterator[SMBResource.SMBDirEntry]:
        # Generate the full path for the SMB directory
        directory_path = PurePath(f"//{smb_resource.server_ip}").joinpath(current_dir)
        
        # List all files and directories in the current directory
        for file_descriptor in smb_resource.scandir(directory_path):
            # Ignore non-excel files or specific filenames
            if file_descriptor.name.lower().endswith(extension) and file_descriptor.name.lower() not in [x.lower() for x in exclude]:
                yield file_descriptor  # Yield the valid file
            
            # If the item is a directory and the depth limit hasn't been reached, recurse into it
            if file_descriptor.is_dir() and (recursive_depth is None or current_depth < recursive_depth):
                # Recursively scan subdirectories
                yield from _scan_dir(current_dir.joinpath(file_descriptor.name), current_depth + 1)

    # Start scanning the directory from depth 0
    return _scan_dir(directory, 0)

@asset(
    key_prefix=["DL_FARINTER", "excel"],
    tags=tags_repo.SmbDataRepository.tag | {"dagster/storage_kind": "sqlserver", "data_source_kind": "smb_xslx_files"},
    compute_kind="polars",

    # required_resource_keys={"smb_resource_analitica_nasgftgu02"},
    
)
def DL_Kielsa_MetasHist_Temp(context: AssetExecutionContext, smb_resource_analitica_nasgftgu02: SMBResource, dwh_farinter_dl: SQLServerResource):
    table = "DL_Kielsa_MetasHist_Temp"
    database = "DL_FARINTER"
    schema = "excel"
    class NullsException(BaseException):
        pass
    class FileException(BaseException):
        pass
    class ErrorsOccurred(BaseException):
        pass
    directory_path = PurePath("data_repo/kielsa/metas_venta/")
    smbres: SMBResource = smb_resource_analitica_nasgftgu02 #context.resources.smb_resource_analitica_nasgftgu02
    schema_config = ExcelSchemaConfig(
        polars_schema={
            "Emp_Id": pl.Int16,
            "Sucursal_Id": pl.Int16,
            "Empleado_Rol": pl.String,
            "Vendedor_Id": pl.Int32,
            "AnioMes": pl.String,
            "Dia_Desde": pl.Int16,
            "Dia_Hasta": pl.Int16,
        },
        blanks_allowed=False,
        exclude_colums=["Vendedor"]
    )
    drop_table_count = 0
    v_metadata = {}
    try:
        rows_inserted = 0
        nulls_count = 0
        for file_descriptor in get_files_dirs(directory=directory_path, smb_resource=smbres, extension=".xlsx", exclude=["cargados", "plantilla.xlsx"]):
            # ignore non excel files xlsx
            try:
                df: pl.DataFrame
                dfd: dict[str, pl.DataFrame]
                with open_file(file_path=file_descriptor.path, smb_resource=smbres) as file:
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

                df=df.cast(schema_config.polars_schema)
                df=df.drop(schema_config.exclude_colums, strict=False)
                df=df.unpivot(index=schema_config.polars_schema.keys(), variable_name="variable", value_name="valor")
                context.log.debug(df.head(5))
                # Separar variable en alerta y atributo por primer _:
                df = (
                    df.with_columns(
                        pl.col("variable")
                        .str.split_exact("_", 1)
                        .struct.rename_fields(["Alerta_Id", "Atributo"])
                        .alias("fields"),
                        AnioMes_Id=pl.col("AnioMes")
                        .str.replace("-", "")
                        .str.slice(0, 6)
                        .cast(pl.Int32),
                        Nombre_Archivo=pl.lit(clean_filename(file_descriptor.name)),
                        Fecha_Carga=pl.lit(datetime.now()),
                        Fecha_Actualizado=pl.lit(datetime.now()),
                    ).unnest("fields")
                    .drop(["variable", "AnioMes"])
                    .with_columns(
                        pl.col("Atributo")
                        .fill_null("No Definido"),
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
                    )
                    .drop(["Dia_Desde", "Dia_Hasta"])
                )
                # context.log.debug(df.head(5))
                nulls_count = df.null_count().sum_horizontal().sum()
                row_count = df.height
                v_metadata.update(
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
                    raise NullsException(f"File {file_descriptor.path} tiene {nulls_count} valores en Blanco.")
                else:
                    df.fill_null(strategy='zero')
                # cargar en la db
                with dwh_farinter_dl.get_connection(engine="sqlalchemy") as conn:
                    if drop_table_count == 0:
                        conn.execute( dwh_farinter_dl.text(f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL BEGIN DROP TABLE {schema}.{table} END; "))
                        drop_table_count += 1
                    conn.commit()
                    rows_inserted = df.write_database(table_name=f"{schema}.{table}", connection=conn, if_table_exists="append")
                    # conn.execute(dwh_farinter_dl.text(f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY (Sociedad_Id, Zona_Id, Division_Id, Articulo_Id, Cliente_Id, Casa_Id, Vendedor_Id, AnioMes_Id)"))
                # with pl.Config(tbl_cols=-1):
                #     context.log.info('schema: ',df.schema)
                #     context.log.info('count: ',row_count)
                #     print('head: ',df.head(10))
                with open_file(file_path=PurePath(file_descriptor.path).parent.joinpath("logs_carga.txt"), smb_resource=smbres, mode="a") as file:
                    file.write(f"INFO, CARGADO, {datetime.now().isoformat()} , Archivo {file_descriptor.path} cargado con {row_count} filas.\n")

                if env_str in ["prd"]:
                    move_file(
                        context=context,
                        file_path=file_descriptor.path,
                        smb_resource=smbres,
                        new_path=PurePath(file_descriptor.path).parent.joinpath("cargados").joinpath(
                            clean_filename(file_descriptor.name)
                        ),
                    )
                v_metadata[file_descriptor.name]["Cargado"] = True

                v_metadata["Cant. Archivos Cargados"] = v_metadata.get("Cant. Archivos Cargados", 0) + 1

            except NullsException as ne:
                context.log.error(ne)
                log_message = (f"ERROR, NO CARGADO en {env_str}, {datetime.now().isoformat()}, " +
                            f"Archivo {file_descriptor.path} tiene {nulls_count} valores en Blanco.\n")
                v_metadata[file_descriptor.name]["Error"] = log_message
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1
                with open_file(file_path=PurePath(file_descriptor.path).parent.joinpath("logs_carga.txt"), smb_resource=smbres, mode="a") as file:
                    file.write(log_message)
            except FileException as fe:
                context.log.error(fe)
                log_message = (f"ERROR, 'NO CARGADO en {env_str}, {datetime.now().isoformat()}, " +
                            f"Archivo {file_descriptor.path} error {str(fe)}.\n")
                v_metadata[file_descriptor.name]["Error"] = log_message
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1 #
                with open_file(file_path=PurePath(file_descriptor.path).parent.joinpath("logs_carga.txt"), smb_resource=smbres, mode="a") as file:
                    file.write(log_message) #
            except Exception as e:
                log_message = (f"ERROR, {'CARGADO' if rows_inserted > 0 else 'NO CARGADO'} en {env_str}, {datetime.now().isoformat()}, " +
                            f"Archivo {file_descriptor.path} error {str(e)}.\n")
                v_metadata[file_descriptor.name]["Error"] = log_message
                v_metadata["Cant. Errores"] = v_metadata.get("Cant. Errores", 0) + 1
                with open_file(file_path=PurePath(file_descriptor.path).parent.joinpath("logs_carga.txt"), smb_resource=smbres, mode="a") as file:
                    file.write(log_message)
                
        if v_metadata.get("Cant. Errores", 0) > 0:
            raise ErrorsOccurred(v_metadata)

    except Exception as e:
        context.log.info("log de carga de archivos:" + str(v_metadata))
        log_message = (f"ERROR, N/A en {env_str}, {datetime.now().isoformat()}, { str(e)}\n")
        with open_file(file_path=directory_path.joinpath("logs_carga.txt"), smb_resource=smbres, mode="a") as file:
            file.write(log_message)
        raise e    
    return MaterializeResult(metadata=v_metadata)
##

# @asset(
#     key_prefix=["BI_FARINTER", "dbo"],
#     deps=[DL_Finanzas_Presupuesto_Temp],
#     compute_kind="sqlserver",
#     tags={"dagster/storage_kind": "sqlserver"},
# )
# def BI_SAP_Hecho_PresupuestoHist(context: AssetExecutionContext, dwh_farinter_bi: SQLServerResource):
#     with dwh_farinter_bi.get_connection(engine="sqlalchemy") as conn:
#         conn.execute(dwh_farinter_bi.text(f"EXEC BI_FARINTER.dbo.BI_paCargarSAP_Hecho_PresupuestoHist"))

if __name__ == '__main__':
    from dagster_shared_gf.resources.smb_resources import smb_resource_analitica_nasgftgu02
    with patch("polars.DataFrame.write_database", MagicMock(return_value=MagicMock())) as mock_write_database:
        materialize_to_memory([DL_Kielsa_MetasHist_Temp], resources={"smb_resource_analitica_nasgftgu02": smb_resource_analitica_nasgftgu02, "dwh_farinter_dl": MagicMock()})
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
