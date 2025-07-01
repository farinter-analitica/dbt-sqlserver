import dagster as dg
import datetime as dt
import dagster_shared_gf.resources.smb_resources as smbr
from pydantic import Field
from dagster_shared_gf.shared_variables import env_str, tags_repo

from dagster_kielsa_gf.assets.control_incentivos import (
    procesamiento_incentivos as incentivos,
    config as cfg_incentivos,
)
import dagster_shared_gf.resources.sql_server_resources as sqlsr
from dagster_shared_gf.shared_helpers import DataframeSQLTableManager
from dagster_shared_gf import automation as auto_def
import polars as pl

# A continuacion se procesan condicionalmente ya sea todos los assets juntos o uno por uno
# Las reglas y condiciones pueden depender de los mismos dataframes y por eso se hace
# un multiasset que consolida los pasos iniciales comunes y luego llama a los assets individuales


class IncentivosConfig(dg.Config):
    fecha_inicio: str = Field(
        default_factory=lambda: (dt.date.today() - dt.timedelta(days=31)).strftime(
            "%Y-%m-%d"
        ),
        description="Fecha de inicio para el procesamiento de incentivos inclusive (por defecto: 31 días atrás)",
    )
    fecha_fin: str = Field(
        default_factory=lambda: (dt.date.today() - dt.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        ),
        description="Fecha de fin para el procesamiento de incentivos inclusive (por defecto: ayer)",
    )
    empresas_id: list[int] = Field(
        default_factory=lambda: list(cfg_incentivos.EMPRESAS_ID),
        description="Lista de IDs de empresas a procesar (por defecto: [1, 2, ...])",
    )
    update: bool = Field(
        default=False,
        description="Si es falso, nunca se sobreescriben registros existentes.",
    )
    drop_target: bool = Field(
        default=False,
        description="Si es verdadero, la tabla destino se elimina antes de la carga",
    )
    drop_temp: bool = Field(
        default=True,
        description="Si es verdadero, la tabla temporal se elimina después de la carga",
    )


assetkey_regalias_incentivo = dg.AssetKey(
    ("DL_FARINTER", "dbo", "DL_Kielsa_Regalia_Incentivo")
)
assetkey_detalle_incentivo = dg.AssetKey(
    ("DL_FARINTER", "dbo", "DL_Kielsa_Detalle_Incentivo")
)


def create_csv_file_on_smb(
    smb_resource_staging_dagster_dwh: smbr.SMBResource,
    df_clientes: pl.DataFrame,
    name: str,
) -> str:
    smbr = smb_resource_staging_dagster_dwh
    file_path = smbr.get_full_server_path(f"\\staging_dagster\\{name}.csv")
    # format_file_path = smbr.get_full_server_path("\\staging_dagster\\kielsa_clientes.fmt")

    # Write the CSV file
    with smbr.client.open_file(file_path, mode="w") as f:
        df_clientes.cast({pl.Boolean: pl.Int8}).write_csv(
            f,
            include_bom=False,
            include_header=False,
            separator=",",
            line_terminator="\r\n",  # BULK INSERT luego solo usa \n pero es necesario \r\n aqui.
            quote_char='"',
            quote_style="non_numeric",
        )

    return str(file_path)


def cargar_asset_incentivos(
    context: dg.AssetExecutionContext,
    lazyframe_meta: cfg_incentivos.LazyFrameWithMeta,
    dwh_farinter_dl: sqlsr.SQLServerResource,
    assetkey: dg.AssetKey,
    fecha_inicio: dt.datetime,
    fecha_fin: dt.datetime,
    drop_temp: bool,
    drop_target: bool,
    update: bool,
    smb_resource_staging_dagster_dwh: smbr.SMBResource,
) -> dict:
    t0 = dt.datetime.now()

    df = lazyframe_meta.frame.collect(engine="streaming")

    table_name = assetkey.path[-1]
    db_schema = assetkey.path[-2]

    manager = DataframeSQLTableManager(
        df=df,
        db_schema=db_schema,
        table_name=table_name,
        sqla_engine=dwh_farinter_dl.get_sqlalchemy_engine(),
        primary_keys=lazyframe_meta.primary_keys,
        load_date_col=None,  # Mejorar rendimiento, es tabla grande
        update_date_col="Fecha_Actualizado",
    )

    # Tenemos que hacer carga masiva por los datos grandes
    file_path = create_csv_file_on_smb(
        smb_resource_staging_dagster_dwh=smb_resource_staging_dagster_dwh,
        df_clientes=manager.generator.df,
        name=f"{db_schema}_{table_name}",
    )

    manager.upsert_dataframe(
        drop_temp=drop_temp, drop_target=drop_target, update=update, file_path=file_path
    )
    filas = df.height
    filas_total_tabla = manager.filas_total_tabla
    filas_tabla_temp = manager.filas_tabla_temp

    t1 = dt.datetime.now()
    duracion = (t1 - t0).total_seconds()

    context.log.info(f"Materialización completada en {duracion:.2f} segundos.")

    return {
        "filas": filas,
        "filas_tabla_temp": filas_tabla_temp,
        "filas_total_tabla": filas_total_tabla,
        "duracion_segundos": duracion,
        "fecha_inicio": str(fecha_inicio),
        "fecha_fin": str(fecha_fin),
    }


@dg.multi_asset(
    outs={
        assetkey_regalias_incentivo.to_python_identifier(): dg.AssetOut(
            key=assetkey_regalias_incentivo,
            description="Tabla de incentivos procesados para regalias",
            tags=tags_repo.AutomationDaily | tags_repo.DetenerCarga,
            automation_condition=auto_def.automation_daily_delta_2_cron,
            is_required=False,
        ),
        assetkey_detalle_incentivo.to_python_identifier(): dg.AssetOut(
            key=assetkey_detalle_incentivo,
            description="Tabla de incentivos procesados para detalle consolidado",
            tags=tags_repo.AutomationDaily | tags_repo.DetenerCarga,
            automation_condition=auto_def.automation_daily_delta_2_cron,
            is_required=False,
        ),
    },
    can_subset=True,
    op_tags=tags_repo.AutomationDaily | tags_repo.DetenerCarga,
    deps=(
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Hecho_Regalia_Detalle")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Hecho_Regalia_Encabezado")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Dim_Calendario_Dinamico")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Dim_Articulo")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Hecho_FacturaEncabezado")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Hecho_FacturaPosicion")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Dim_Sucursal")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Dim_Vendedor")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Dim_Usuario")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Dim_UsuarioSucursal")),
    ),
    group_name="incentivos_kielsa",
)
def procesamiento_y_carga_incentivos(
    context: dg.AssetExecutionContext,
    dwh_farinter_dl: sqlsr.SQLServerResource,
    dwh_farinter_bi: sqlsr.SQLServerResource,
    config: IncentivosConfig,
    smb_resource_staging_dagster_dwh: smbr.SMBResource,
):
    """
    Multi asset Dagster para procesar y cargar incentivos.
    Permite subsetting y retorna información de materialización y duración.
    """

    fecha_inicio = dt.datetime.strptime(config.fecha_inicio, "%Y-%m-%d")
    fecha_fin = dt.datetime.strptime(config.fecha_fin, "%Y-%m-%d")
    empresas_id = set(config.empresas_id)
    drop_target = config.drop_target

    context.log.info(f"Procesando incentivos desde {fecha_inicio} hasta {fecha_fin}")

    # Procesar por empresa y mes
    resultados_regalias = []
    resultados_detalle = []

    current = fecha_inicio.replace(day=1)
    end = fecha_fin.replace(day=1)
    while current <= end:
        mes_inicio = current
        mes_fin = (current.replace(day=28) + dt.timedelta(days=4)).replace(
            day=1
        ) - dt.timedelta(days=1)
        if mes_fin > fecha_fin:
            mes_fin = fecha_fin

        for empresa in empresas_id:
            context.log.info(
                f"Procesando empresa {empresa}, mes {mes_inicio.strftime('%Y-%m')}"
            )
            proc = incentivos.ProcesamientoIncentivos(
                connection_str=dwh_farinter_bi.get_arrow_odbc_conn_string(),
                fecha_inicio=mes_inicio,
                fecha_fin=mes_fin,
                empresas_id={empresa},
            )
            proc = proc.procesar()

            if assetkey_regalias_incentivo in context.selected_asset_keys:
                meta_regalias = cargar_asset_incentivos(
                    context=context,
                    lazyframe_meta=proc.dfm_output.regalias_incentivo,
                    assetkey=assetkey_regalias_incentivo,
                    dwh_farinter_dl=dwh_farinter_dl,
                    fecha_inicio=mes_inicio,
                    fecha_fin=mes_fin,
                    drop_temp=config.drop_temp,
                    drop_target=drop_target,
                    update=config.update,
                    smb_resource_staging_dagster_dwh=smb_resource_staging_dagster_dwh,
                )
                meta_regalias["empresa_id"] = empresa
                meta_regalias["mes"] = mes_inicio.strftime("%Y-%m")
                resultados_regalias.append(meta_regalias)

            if assetkey_detalle_incentivo in context.selected_asset_keys:
                if not proc.dfm_output.detalle_incentivo:
                    context.log.warning(
                        f"No hay datos para detalle_incentivo empresa {empresa} mes {mes_inicio.strftime('%Y-%m')}"
                    )
                    continue
                meta_detalle = cargar_asset_incentivos(
                    context=context,
                    lazyframe_meta=proc.dfm_output.detalle_incentivo,
                    assetkey=assetkey_detalle_incentivo,
                    dwh_farinter_dl=dwh_farinter_dl,
                    fecha_inicio=mes_inicio,
                    fecha_fin=mes_fin,
                    drop_temp=config.drop_temp,
                    drop_target=drop_target,
                    update=config.update,
                    smb_resource_staging_dagster_dwh=smb_resource_staging_dagster_dwh,
                )
                meta_detalle["empresa_id"] = empresa
                meta_detalle["mes"] = mes_inicio.strftime("%Y-%m")
                resultados_detalle.append(meta_detalle)

            drop_target = False

        next_month = (current.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        current = next_month

    # Consolidar métricas y yield para cada dataframe
    if assetkey_regalias_incentivo in context.selected_asset_keys:
        metricas_regalias = {
            "total_filas": sum(r["filas"] for r in resultados_regalias),
            "total_filas_tabla_temp": sum(
                r["filas_tabla_temp"] for r in resultados_regalias
            ),
            "total_filas_total_tabla": sum(
                r["filas_total_tabla"] for r in resultados_regalias
            ),
            "total_duracion_segundos": sum(
                r["duracion_segundos"] for r in resultados_regalias
            ),
            "procesos": resultados_regalias,
        }
        yield dg.Output(
            value=None,
            output_name=assetkey_regalias_incentivo.to_python_identifier(),
            metadata=metricas_regalias,
        )

    if assetkey_detalle_incentivo in context.selected_asset_keys:
        metricas_detalle = {
            "total_filas": sum(r["filas"] for r in resultados_detalle),
            "total_filas_tabla_temp": sum(
                r["filas_tabla_temp"] for r in resultados_detalle
            ),
            "total_filas_total_tabla": sum(
                r["filas_total_tabla"] for r in resultados_detalle
            ),
            "total_duracion_segundos": sum(
                r["duracion_segundos"] for r in resultados_detalle
            ),
            "procesos": resultados_detalle,
        }
        yield dg.Output(
            value=None,
            output_name=assetkey_detalle_incentivo.to_python_identifier(),
            metadata=metricas_detalle,
        )


if __name__ == "__main__":
    import warnings

    # Set up environment string if needed
    # env_str = "local"  # or fetch from env

    start_time = dt.datetime.now()

    with dg.instance_for_test() as instance:
        # Optionally warn or adjust for local/test mode
        if env_str == "local":
            warnings.warn(
                "Running in local mode, using test data and/or mock resources"
            )

        # Define a mock asset if your asset has dependencies
        @dg.asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1

        # Optionally mock resources if needed
        # from dagster import ResourceDefinition
        # mock_resource_a = ResourceDefinition.mock_resource()
        # mock_resource_b = ResourceDefinition.mock_resource()

        dict_config = dg.RunConfig(
            ops={
                procesamiento_y_carga_incentivos.node_def.name: IncentivosConfig(
                    fecha_inicio=(dt.date.today() - dt.timedelta(days=24)).strftime(
                        "%Y-%m-%d"
                    ),
                    # drop_temp=True,
                    # drop_target=True,
                    update=True,
                )
            }
        )

        result = dg.materialize(
            assets=[
                mock_between_asset,
                procesamiento_y_carga_incentivos,
                # Add other assets as needed
            ],
            instance=instance,
            resources={
                "dwh_farinter_bi": sqlsr.dwh_farinter_bi,
                "dwh_farinter_dl": sqlsr.dwh_farinter_dl,
                "smb_resource_staging_dagster_dwh": smbr.smb_resource_staging_dagster_dwh,
                # Add other resources as needed
            },
            run_config=dict_config,
            selection=[assetkey_regalias_incentivo],
        )
        # Print output for each asset/multi-asset node
        for assetmat in result.asset_materializations_for_node(
            procesamiento_y_carga_incentivos.node_def.name
        ):
            print(assetmat.metadata)
        # print(result.output_for_node(YOUR_MULTI_ASSET.node_def.name))

    end_time = dt.datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
