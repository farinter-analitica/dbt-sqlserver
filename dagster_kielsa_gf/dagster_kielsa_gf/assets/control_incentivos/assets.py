import dagster as dg
import datetime as dt
from pydantic import Field
from dagster_shared_gf.shared_variables import env_str, tags_repo

from dagster_kielsa_gf.assets.control_incentivos import (
    procesamiento_incentivos as incentivos,
    config as cfg_incentivos,
)
import dagster_shared_gf.resources.sql_server_resources as sqlsr
from dagster_shared_gf.shared_helpers import DataframeSQLTableManager
from dagster_shared_gf import automation as auto_def


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


def asset_incentivos_regalias(
    context: dg.AssetExecutionContext,
    proc: incentivos.ProcesamientoIncentivos,
    dwh_farinter_dl: sqlsr.SQLServerResource,
    fecha_inicio: dt.datetime,
    fecha_fin: dt.datetime,
    drop_temp: bool,
    drop_target: bool,
) -> dg.Output:
    t0 = dt.datetime.now()

    # Cargar incentivos al DWH usando el helper
    df_regalias_incentivo = proc.df_output.regalias_incentivo.frame.collect(
        engine="streaming"
    )

    # Nombre de tabla destino (ajusta según tu modelo)
    table_name = "DL_Kielsa_Regalia_Incentivo"
    db_schema = "dbo"

    # Usar el helper para upsert
    manager = DataframeSQLTableManager(
        df=df_regalias_incentivo,
        db_schema=db_schema,
        table_name=table_name,
        sqla_engine=dwh_farinter_dl.get_sqlalchemy_engine(),
        primary_keys=proc.dfm_regalias.primary_keys,
        load_date_col="Fecha_Carga",
        update_date_col="Fecha_Actualizado",
    )
    manager.upsert_dataframe(drop_temp=drop_temp, drop_target=drop_target)
    filas = df_regalias_incentivo.height
    filas_total_tabla = manager.filas_total_tabla
    filas_tabla_temp = manager.filas_tabla_temp

    t1 = dt.datetime.now()
    duracion = (t1 - t0).total_seconds()

    context.log.info(f"Materialización completada en {duracion:.2f} segundos.")

    # Retornar outputs con metadata
    return dg.Output(
        value=None,
        output_name=assetkey_regalias_incentivo.to_python_identifier(),
        metadata={
            "filas": filas,
            "filas_tabla_temp": filas_tabla_temp,
            "filas_total_tabla": filas_total_tabla,
            "duracion_segundos": duracion,
            "fecha_inicio": str(fecha_inicio),
            "fecha_fin": str(fecha_fin),
        },
    )

    # Procesar incentivos


# Puedes ajustar los nombres de los outputs según los dataframes de salida
@dg.multi_asset(
    outs={
        assetkey_regalias_incentivo.to_python_identifier(): dg.AssetOut(
            key=assetkey_regalias_incentivo,
            description="Tabla de incentivos procesados para regalias",
            tags=tags_repo.AutomationDaily,
            automation_condition=auto_def.automation_daily_delta_2_cron,
        ),
    },
    can_subset=True,
    op_tags=tags_repo.AutomationDaily,
    deps=(
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Hecho_Regalia_Detalle")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Hecho_Regalia_Encabezado")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Dim_Calendario_Dinamico")),
        dg.AssetKey(("BI_FARINTER", "dbo", "BI_Kielsa_Dim_Articulo")),
    ),
    group_name="incentivos_kielsa",
)
def procesamiento_y_carga_incentivos(
    context: dg.AssetExecutionContext,
    dwh_farinter_dl: sqlsr.SQLServerResource,
    dwh_farinter_bi: sqlsr.SQLServerResource,
    config: IncentivosConfig,
):
    """
    Multi asset Dagster para procesar y cargar incentivos.
    Permite subsetting y retorna información de materialización y duración.
    """

    # Permitir override por config/partition si se desea
    fecha_inicio = dt.datetime.strptime(config.fecha_inicio, "%Y-%m-%d")
    fecha_fin = dt.datetime.strptime(config.fecha_fin, "%Y-%m-%d")
    empresas_id = set(config.empresas_id)
    drop_temp = config.drop_temp
    drop_target = config.drop_target

    context.log.info(f"Procesando incentivos desde {fecha_inicio} hasta {fecha_fin}")

    # Procesar incentivos
    proc = incentivos.ProcesamientoIncentivos(
        dwh_farinter_bi=dwh_farinter_bi,  # Usamos el recurso del DWH
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        empresas_id=empresas_id,
    )
    proc.extract_dataframes().process_dataframes()

    # Regalias
    if assetkey_regalias_incentivo in context.selected_asset_keys:
        yield asset_incentivos_regalias(
            context=context,
            proc=proc,
            dwh_farinter_dl=dwh_farinter_dl,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            drop_temp=drop_temp,
            drop_target=drop_target,
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
                    # drop_temp=True,
                    # drop_target=True,
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
                # Add other resources as needed
            },
            run_config=dict_config,
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
