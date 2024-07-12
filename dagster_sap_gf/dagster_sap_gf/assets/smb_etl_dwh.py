from dagster import (asset
                     , AssetChecksDefinition 
                     , AssetExecutionContext
                     , op 
                     , OpExecutionContext
                     , build_last_update_freshness_checks
                     , build_asset_context
                     , load_assets_from_current_module
                     , load_asset_checks_from_current_module
                     , Config
                     , Output
                     , MaterializeResult
                     )
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource 
from dagster_shared_gf.resources.smb_resources import SMBResource, smbclient
from dagster_shared_gf.shared_variables import env_str, TagsRepositoryGF as tags_repo
from dagster_shared_gf.shared_functions import filter_assets_by_tags, get_all_instances_of_class
import dagster_sap_gf.assets.dbt_dwh_sap as dbt_dwh_sap
from smbclient import SMBDirEntry
from pathlib import Path
from pydantic import Field
from typing import List, Dict, Any, Mapping, Sequence, Union, Iterator, Literal
from datetime import datetime, date, timedelta
import polars as pl
from io import BytesIO
##
class ExcelSchemaConfig(Config):
    expected_columns: Dict[str, str] = Field(description="Columns", default_factory=Dict)
    polars_schema: Dict[str, pl.DataType | Any] = Field(description="polars_schema", default_factory=Dict)
    blanks_allowed: bool = Field(description="Allow blanks", default=True)
    blanks_on_type_error: bool = Field(description="Convert type error to blanks", default=False)

def directory_files(directory: Path, smb_resource: SMBResource) -> Iterator[SMBDirEntry]:
    directory_path = Path(f"//{smb_resource.server_ip}").joinpath(directory).resolve()
    smbsession:smbclient = smb_resource.get_smbclient()
    return smbsession.scandir(directory_path)

def open_file(file_path: Path, smb_resource: SMBResource
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
    
    file_path = Path(f"//{smb_resource.server_ip}").joinpath(file_path).resolve()
    smbsession:smbclient = smb_resource.get_smbclient()
    return smbsession.open_file(file_path, mode=mode)

@asset(
    key_prefix=["DL_FARINTER"],
    tags=tags_repo.SmbDataRepository.tag,
    compute_kind="smb,sqlserver",
    # required_resource_keys={"smb_resource_analitica_nasgftgu02"},
)
def DL_Finanzas_Presupuesto_Temp(context: AssetExecutionContext, smb_resource_analitica_nasgftgu02: SMBResource, dwh_farinter_dl: SQLServerResource):
    table = "DL_Finanzas_Presupuesto_Temp"
    database = "DL_FARINTER"
    schema = "excel"
    class NullsException(Exception):
        pass
    directory_path = Path("data_repo/grupo_farinter/presupuesto_ventas_finanzas")
    smbres: SMBResource = smb_resource_analitica_nasgftgu02 #context.resources.smb_resource_analitica_nasgftgu02
    schema_config = ExcelSchemaConfig(
        expected_columns={
            "Sociedad_Id": "Cod Sociedad",
            "Zona_Id": "Cod Territorio",
            "Division_Id": "Division",
            "Articulo_Id": "Cod Material",
            "Cliente_Id": "Codigo Cliente",
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
    drop_table_count = 0
    try:
        for file_descriptor in list(directory_files(directory=directory_path, smb_resource=smbres)):
            #ignore non excel files xlsx
            if not file_descriptor.name.endswith(".xlsx"):
                continue
            try:
                df: pl.DataFrame
                with open_file(file_path=file_descriptor.path, smb_resource=smbres) as file:
                    file_content = BytesIO(file.read())
                    df: pl.DataFrame =pl.read_excel(file_content,sheet_name='carga'
                                    , infer_schema_length=0
                                    , columns=list(schema_config.expected_columns.values())
                                )           
                df=df.select(**schema_config.expected_columns)
                df=df.cast(schema_config.polars_schema)
                # recopilar division de la base de datos
                dfdc: pl.DataFrame
                with dwh_farinter_dl.get_connection(engine="sqlalchemy") as conn:
                    #conn.execute(f"USE {database}; SELECT Division_Id, Division_Nombre FROM dbo.DL_Edit_Division_SAP") 
                    dfdc = pl.read_database("SELECT Division_Id, Division_Nombre FROM dbo.DL_Edit_Division_SAP", connection=conn)
                dfdc = dfdc.select(["Division_Nombre","Division_Id"])
                division_id_map_reversed: Dict = dict(dfdc.rows())        
                context.log.info(dfdc)
                # Reemplazar farma y cualquier otro nombre en division_id por su id y limpieza de datos: 
                df=df.with_columns(Division_Id = pl.col("Division_Id").replace(division_id_map_reversed, default="00"),
                                    Zona_Id = pl.col("Zona_Id").str.zfill(6),
                                    Cliente_Id = pl.col("Cliente_Id").str.zfill(10),
                                    Vendedor_Id = pl.col("Vendedor_Id").str.zfill(3),
                                    Fecha_Id = pl.col("Fecha_Id").str.slice(0, 10).str.to_date('%F').cast(pl.Date),
                                    AnioMes_Id = pl.col("Fecha_Id").str.slice(0, 10).str.to_date().dt.to_string("%Y%m").cast(pl.Int32),
                                    Nombre_Archivo = pl.lit(file_descriptor.name),
                                    Fecha_Carga = pl.lit(datetime.now()),
                                    Fecha_Actualizado = pl.lit(datetime.now()),
                                    )
                nulls_count = df.null_count().sum_horizontal().sum()
                row_count = df.height
                if not schema_config.blanks_allowed and nulls_count > 0:
                    raise NullsException(f"File {file_descriptor.path} tiene {nulls_count} valores en Blanco.")
                else:
                    df.fill_null(0)
                #cargar en la db
                with dwh_farinter_dl.get_connection(engine="sqlalchemy") as conn:
                    if drop_table_count == 0:
                        conn.execute( dwh_farinter_dl.text(f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL BEGIN DROP TABLE {schema}.{table} END; "))
                        drop_table_count += 1
                    conn.commit()
                    rows_inserted = df.write_database(table_name=f"{schema}.{table}", connection=conn, if_table_exists="append")
                    #conn.execute(dwh_farinter_dl.text(f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY (Sociedad_Id, Zona_Id, Division_Id, Articulo_Id, Cliente_Id, Casa_Id, Vendedor_Id, AnioMes_Id)"))
                # with pl.Config(tbl_cols=-1):
                #     context.log.info('schema: ',df.schema)
                #     context.log.info('count: ',row_count)
                #     print('head: ',df.head(10))
                with open_file(file_path=directory_path.joinpath("logs_carga.txt"), smb_resource=smbres, mode="a") as file:
                    file.write(f"INFO, CARGADO, {datetime.now().isoformat()} , Archivo {file_descriptor.path} cargado con {row_count} filas.\n")

            except NullsException as e:
                context.log.error(e)
                with open_file(file_path=directory_path.joinpath("logs_carga.txt"), smb_resource=smbres, mode="a") as file:
                    file.write(f"ERROR, NO CARGADO, {datetime.now().isoformat()} , Archivo {file_descriptor.path} tiene {nulls_count} valores en Blanco.\n")
        with dwh_farinter_dl.get_connection(engine="sqlalchemy") as conn:
            conn.execute(dwh_farinter_dl.text(f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY (Sociedad_Id, Zona_Id, Division_Id, Articulo_Id, Cliente_Id, Cuenta_Id, Vendedor_Id, AnioMes_Id)"))
    except Exception as e:
        with open_file(file_path=directory_path.joinpath("logs_carga.txt"), smb_resource=smbres, mode="a") as file:
            file.write(f"ERROR, {'CARGADO' if rows_inserted > 0 else 'NO CARGADO'}, {datetime.now().isoformat()} , {e}\n")
        raise e    
    return MaterializeResult(metadata={
        "row_count": row_count, "null_count": nulls_count, "schema": df.schema})
##

if not __name__ == '__main__':
    all_assets = load_assets_from_current_module(group_name="smb_etl_dwh")

    all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
        assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
        lower_bound_delta=timedelta(hours=26),
        deadline_cron="0 9 * * 1-6",
    )
    all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
        assets=filter_assets_by_tags(all_assets, tags=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
        lower_bound_delta=timedelta(hours=13),
        deadline_cron="0 10-16 * * 1-6",
    )

    all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
    all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks

