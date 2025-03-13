from collections import deque
import warnings
from datetime import datetime

from dagster_shared_gf.shared_helpers import SQLScriptGenerator
import pendulum as pdl
import polars as pl
from dagster import (
    AssetKey,
    AssetsDefinition,
    Field,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    asset,
    graph,
    instance_for_test,
    materialize,
    op,
)

from dagster_shared_gf.automation import automation_weekly_7_delta_1_cron
from dagster_shared_gf.resources.smb_resources import (
    smb_resource_staging_dagster_dwh,
)
from dagster_shared_gf.resources.sql_server_resources import (
    SQLServerResource,
)
from dagster_shared_gf.shared_variables import env_str, tags_repo


@op(
    ins={
        "BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia": In(
            dagster_type=Nothing,
        ),
    },
    out={
        "df_demanda": Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
    },
    config_schema={
        "meses_muestra": Field(int, is_required=False, default_value=12 * 5)
    },
)
def get_BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia_data(
    context: OpExecutionContext,
    dwh_farinter_bi: SQLServerResource,
) -> pl.DataFrame:
    meses_muestra = context.op_config["meses_muestra"]
    fecha_desde = pdl.today().subtract(months=meses_muestra)

    sql_query = f"""
    SELECT {"top 10000" if env_str == "local" else ""} 
        --[Sociedad_Id]
        [Centro_Almacen_Id]
        ,[Material_Id]
        ,[Gpo_Cliente]
        ,[Fecha_Id]
        ,[Gpo_Plan]
        ,[Material_Nombre]
        --,[Articulo_Id]
        ,[main]
        ,[Limpios_Forecast] as [Ctd_Demanda]
    FROM [BI_FARINTER].[dbo].[BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia]
    WHERE Fecha_Id >= '{fecha_desde.strftime("%Y%m%d")}'
    {"AND Centro_Almacen_Id = '1350-5001'" if env_str == "local" else ""} 
    """
    main_query = (
        pl.read_database(sql_query, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .cast({pl.Decimal: pl.Float64, pl.Date: pl.Datetime})
        .collect(streaming=True)
    )
    return main_query


@op(
    out={
        "df_demanda_procesada": Out(
            pl.DataFrame, io_manager_key="polars_parquet_io_manager"
        ),
    },
)
def procesar_forecast(
    context: OpExecutionContext,
    df_demanda: pl.DataFrame,
) -> pl.DataFrame:
    # Importar dentro de la función para evitar errores de carga de librerias externas
    # Esto efectivamente permite que la ubicacion de codigo cargue aunque la libreria
    # no este instalada.
    try:
        from statstools_gf.forecast.forecast_functions import forecast_dataframe
        import time
    except ImportError as e:
        raise ImportError(f"Required forecast library unavailable: {str(e)}")

    forecast_dfs = deque()
    partitions = df_demanda.partition_by("main")
    total = len(partitions)
    start_time = time.time()
    last_log = start_time
    processed = 0

    for main in partitions:
        processed += 1
        current_time = time.time()
        if processed % max(1, total // 10) == 0 or current_time - last_log >= 60 * 10:
            progress = (processed / total) * 100
            elapsed = current_time - start_time
            context.log.info(
                f"Progress: {progress:.1f}% ({processed}/{total}) - Time elapsed: {elapsed:.1f}s"
            )
            last_log = current_time

        forecast_dfs.append(
            forecast_dataframe(
                main,
                time_col="Fecha_Id",
                value_col="Ctd_Demanda",
                forecast_horizon=16,
                time_type="monthly",
            ).with_columns(
                pl.col("Fecha_Id").cast(pl.Date).dt.month_end().alias("Fecha_Id"),
                pl.selectors.by_name(
                    "Material_Id",
                    "Centro_Almacen_Id",
                    "Gpo_Cliente",
                    "main",
                ).forward_fill(),  # Rellenar por falta de datos en meses nuevos
            )
        )

    forecast = pl.concat(forecast_dfs, how="diagonal_relaxed")

    return forecast


@op
def save_forecast_procesado(
    dwh_farinter_bi: SQLServerResource, forecast_procesado: pl.DataFrame
) -> None:
    sg = SQLScriptGenerator(
        primary_keys=(
            "Material_Id",
            "Fecha_Id",
            "Centro_Almacen_Id",
            "Gpo_Cliente",
        ),
        db_schema="dbo",
        table_name="BI_SAP_Hecho_SocAlmArtGpoCli_Forecast",
        df=forecast_procesado,
        temp_table_name="BI_SAP_Hecho_SocAlmArtGpoCli_Forecast_NEW",
    )

    if env_str == "local":
        with pl.Config() as c:
            c.set_tbl_rows(-1)
            c.set_tbl_cols(-1)
            print(forecast_procesado.head(10))
            print(forecast_procesado.describe())
        return
    print(f"Por guardar {len(forecast_procesado)} registros")
    with dwh_farinter_bi.get_sqlalchemy_conn() as conn:
        dwh_farinter_bi.execute_and_commit(
            sg.drop_table_sql_script(temp=True), connection=conn
        )
        dwh_farinter_bi.execute_and_commit(
            sg.create_table_sql_script(temp=True), connection=conn
        )
        dwh_farinter_bi.execute_and_commit(
            sg.columnstore_table_sql_script(temp=True), connection=conn
        )

        # First write as regular table
        forecast_procesado.write_database(
            table_name=sg.temp_table_name,
            connection=conn,
            if_table_exists="append",
        )

        dwh_farinter_bi.execute_and_commit(
            sg.primary_key_table_sql_script(temp=True), connection=conn
        )

        dwh_farinter_bi.execute_and_commit(sg.swap_table_with_temp(), connection=conn)


@graph
def forecast_graph(BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia):
    df_demanda = get_BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia_data(
        BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia
    )
    forecast_procesado = procesar_forecast(df_demanda=df_demanda)
    return save_forecast_procesado(forecast_procesado=forecast_procesado)


BI_SAP_Hecho_SocAlmArtGpoCli_Forecast = AssetsDefinition.from_graph(
    graph_def=forecast_graph,
    keys_by_input_name={
        "BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia": AssetKey(
            ["BI_FARINTER", "dbo", "BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia"]
        ),
    },
    keys_by_output_name={
        "result": AssetKey(
            ["BI_FARINTER", "dbo", "BI_SAP_Hecho_SocAlmArtGpoCli_Forecast"]
        )
    },
    tags_by_output_name={
        "result": tags_repo.Weekly | tags_repo.UniquePeriod | tags_repo.AutomationOnly
    },
    automation_conditions_by_output_name={"result": automation_weekly_7_delta_1_cron},
    group_name="demanda",
)


if __name__ == "__main__":
    from dagster import instance_for_test
    from dagster_polars import PolarsParquetIOManager

    from dagster_shared_gf.resources.sql_server_resources import (
        dwh_farinter_dl,
        dwh_farinter_bi,
    )

    # print(DL_SAP_Articulo_ArticuloRelacionado.asset_deps)
    start_time = datetime.now()
    with instance_for_test() as instance:
        from dagster import ResourceDefinition

        if env_str == "local":
            warnings.warn("Running in local mode")

        @asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1

        mock_dwh_farinter_bi = ResourceDefinition.mock_resource()
        mock_dwh_farinter_dl = ResourceDefinition.mock_resource()

        result = materialize(
            assets=[mock_between_asset, BI_SAP_Hecho_SocAlmArtGpoCli_Forecast],
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dwh_farinter_bi": dwh_farinter_bi,
                "smb_resource_staging_dagster_dwh": smb_resource_staging_dagster_dwh,
                "polars_parquet_io_manager": PolarsParquetIOManager(),
            },
        )
        print(
            result.output_for_node(BI_SAP_Hecho_SocAlmArtGpoCli_Forecast.node_def.name)
        )

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )

    # SELECT TOP (1000) AR.*
    # 		,A.Articulo_Nombre
    # 	    ,A2.Articulo_Nombre AS Relacionado
    #   FROM [DL_FARINTER].[dbo].[DL_SAP_Articulo_ArticuloRelacionado] AR
    #   INNER JOIN BI_FARINTER.dbo.BI_SAP_Dim_Articulo A
    #   ON A.Emp_Id=1
    #   AND AR.Articulo_Id = A.Articulo_Id
    #     INNER JOIN BI_FARINTER.dbo.BI_SAP_Dim_Articulo A2
    #   ON A2.Emp_Id=1
    #   AND AR.Articulo_Id_Relacionado = A2.Articulo_Id
    #   WHERE A.Articulo_Id = '1110000125'
