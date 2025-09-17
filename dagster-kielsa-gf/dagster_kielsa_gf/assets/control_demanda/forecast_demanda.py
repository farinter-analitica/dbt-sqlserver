from collections import deque
from datetime import datetime

import dagster as dg
import pendulum as pdl
import polars as pl
from dagster_shared_gf.automation import automation_monthly_start_delta_1_cron
from dagster_shared_gf.resources.smb_resources import (
    smb_resource_staging_dagster_dwh,
)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_helpers import DataframeSQLTableManager
from dagster_shared_gf.shared_variables import env_str, tags_repo


@dg.op(
    ins={
        "BI_Kielsa_Hecho_SucArt_Demanda_Limpia": dg.In(
            dagster_type=dg.Nothing,
        ),
    },
    out={
        "df_demanda": dg.Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
    },
    config_schema={
        "meses_muestra": dg.Field(int, is_required=False, default_value=12 * 5),
        "sucursal_id": dg.Field(int, is_required=False, default_value=3),
    },
)
def get_kielsa_demanda_limpia(
    context: dg.OpExecutionContext, dwh_farinter_bi: SQLServerResource
) -> pl.DataFrame:
    meses_muestra = context.op_config["meses_muestra"]
    sucursal_id = context.op_config["sucursal_id"]
    fecha_desde = pdl.today().subtract(months=meses_muestra)

    sql_query = f"""
    SELECT {"TOP 100000" if env_str == "local" else ""}
        Material_Id,
        Centro_Almacen_Id,
        Gpo_Cliente,
        CAST(Fecha_Id AS date) AS Fecha_Id,
        main,
        Limpios_Forecast AS Ctd_Demanda
    FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_SucArt_Demanda_Limpia
    WHERE Fecha_Id >= '{fecha_desde.strftime("%Y-%m-%d")}'
      AND Centro_Almacen_Id = {sucursal_id}
    """

    df = (
        pl.read_database(sql_query, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .cast({pl.Decimal: pl.Float64, pl.Date: pl.Datetime})
        .collect(engine="streaming")
    )
    return df


@dg.op(
    out={
        "df_forecast": dg.Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager")
    }
)
def generar_forecast(
    context: dg.OpExecutionContext, df_demanda: pl.DataFrame
) -> pl.DataFrame:
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
        if processed % max(1, total // 10) == 0 or current_time - last_log >= 600:
            progress = (processed / total) * 100
            elapsed = current_time - start_time
            context.log.info(
                f"Progress: {progress:.1f}% ({processed}/{total}) - Time elapsed: {elapsed:.1f}s"
            )
            last_log = current_time
        fc_df = (
            forecast_dataframe(
                main,
                time_col="Fecha_Id",
                value_col="Ctd_Demanda",
                forecast_horizon=16,
                time_type="monthly",
            )
            .with_columns(
                pl.col("Fecha_Id").cast(pl.Date).dt.month_end().alias("Fecha_Id"),
                pl.selectors.by_name(
                    "Material_Id", "Centro_Almacen_Id", "Gpo_Cliente", "main"
                ).forward_fill(),
            )
            .filter(pl.col("Fecha_Id") >= pdl.today().subtract(years=3))
        )
        forecast_dfs.append(fc_df)

    forecast = pl.concat(forecast_dfs, how="diagonal_relaxed")
    return forecast


@dg.op
def save_kielsa_forecast(
    dwh_farinter_bi: SQLServerResource, forecast_procesado: pl.DataFrame
) -> None:
    if env_str == "local":
        with pl.Config() as c:
            c.set_tbl_rows(-1)
            c.set_tbl_cols(-1)
            print(forecast_procesado.head(10))
            print(forecast_procesado.describe())
        return

    print(f"Por guardar {len(forecast_procesado)} registros")

    engine = dwh_farinter_bi.get_sqlalchemy_engine()

    manager = DataframeSQLTableManager(
        df=forecast_procesado,
        db_schema="dbo",
        table_name="BI_Kielsa_Hecho_SucArt_Forecast",
        sqla_engine=engine,
        primary_keys=("Material_Id", "Centro_Almacen_Id", "Gpo_Cliente", "Fecha_Id"),
        temp_table_name="BI_Kielsa_Hecho_SucArt_Forecast_NEW",
    )

    manager.upsert_dataframe()


@dg.graph
def forecast_kielsa_graph(BI_Kielsa_Hecho_SucArt_Demanda_Limpia):
    df_demanda = get_kielsa_demanda_limpia(BI_Kielsa_Hecho_SucArt_Demanda_Limpia)
    df_forecast = generar_forecast(df_demanda)
    return save_kielsa_forecast(df_forecast)


BI_Kielsa_Hecho_SucArt_Forecast = dg.AssetsDefinition.from_graph(
    graph_def=forecast_kielsa_graph,
    keys_by_input_name={
        "BI_Kielsa_Hecho_SucArt_Demanda_Limpia": dg.AssetKey(
            ["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_SucArt_Demanda_Limpia"]
        )
    },
    keys_by_output_name={
        "result": dg.AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_SucArt_Forecast"])
    },
    tags_by_output_name={
        "result": tags_repo.AutomationMonthlyStart
        | tags_repo.UniquePeriod
        | tags_repo.AutomationOnly
    },
    automation_conditions_by_output_name={
        "result": automation_monthly_start_delta_1_cron
    },
    group_name="demanda",
)


if __name__ == "__main__":
    from dagster import instance_for_test
    from dagster_polars import PolarsParquetIOManager
    from dagster_shared_gf.resources.sql_server_resources import (
        dwh_farinter_bi,
        dwh_farinter_dl,
    )

    start_time = datetime.now()
    with instance_for_test() as instance:

        @dg.asset(name="between_asset")
        def mock_between_asset() -> int:  # noqa: F401
            return 1

        result = dg.materialize(
            assets=[mock_between_asset, BI_Kielsa_Hecho_SucArt_Forecast],
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dwh_farinter_bi": dwh_farinter_bi,
                "smb_resource_staging_dagster_dwh": smb_resource_staging_dagster_dwh,
                "polars_parquet_io_manager": PolarsParquetIOManager(),
            },
        )
        print(result.output_for_node(BI_Kielsa_Hecho_SucArt_Forecast.node_def.name))

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
