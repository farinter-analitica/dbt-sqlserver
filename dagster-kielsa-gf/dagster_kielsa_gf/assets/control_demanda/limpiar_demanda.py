import warnings
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


# ==============================================
# Lectura de datos base (ventas y stock)
# ==============================================


@dg.op(
    out={
        "df_demanda": dg.Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
    },
    config_schema={
        # Meses de historia a traer desde las fuentes
        "meses_muestra": dg.Field(int, is_required=False, default_value=12 * 5),
        "sucursal_id": dg.Field(int, is_required=False, default_value=3),
    },
)
def get_kielsa_demanda_data(
    context: dg.OpExecutionContext, dwh_farinter_bi: SQLServerResource
) -> pl.DataFrame:
    """
    Lee ventas desde BI_Kielsa_Hecho_FacturaPosicion y las agrega a nivel mensual
    por (Emp_Id, Suc_Id, Articulo_Id), separando demanda positiva y negativa.
    """
    meses_muestra = context.op_config["meses_muestra"]
    sucursal_id = context.op_config["sucursal_id"]
    fecha_desde = pdl.today().subtract(months=meses_muestra)

    sql_query = f"""
    WITH demanda AS (
        SELECT {"TOP 100000" if env_str == "local" else ""}
            FP.Suc_Id,
            FP.Articulo_Id,
            EOMONTH(CAST(FP.Factura_Fecha AS date)) AS Fecha_Id,
            SUM(CASE WHEN FP.Detalle_Cantidad > 0 THEN FP.Detalle_Cantidad ELSE 0 END) AS Demanda_Positiva,
            SUM(CASE WHEN FP.Detalle_Cantidad < 0 THEN FP.Detalle_Cantidad ELSE 0 END) AS Demanda_Negativa
        FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FP
        WHERE FP.Factura_Fecha >= '{fecha_desde.strftime("%Y-%m-%d")}'
          AND FP.Suc_Id = {sucursal_id}
        GROUP BY FP.Suc_Id, FP.Articulo_Id, EOMONTH(CAST(FP.Factura_Fecha AS date))
    )
    SELECT * FROM demanda
    """

    df = pl.read_database(sql_query, dwh_farinter_bi.get_arrow_odbc_conn_string())
    df = (
        df.lazy()
        .with_columns(
            # Mapear a nombres esperados por MDDME
            pl.col("Articulo_Id").alias("Material_Id"),
            pl.col("Suc_Id").alias("Centro_Almacen_Id"),
            pl.lit("KIELSA").alias("Gpo_Plan"),
            pl.lit(0).alias("Gpo_Cliente"),
            pl.lit(0).alias("Vencidos_Entrada"),
            # Placeholder para cumplir filtro del algoritmo (no afecta lógica en Kielsa)
            pl.lit("").alias("Material_Nombre"),
        )
        .with_columns(
            # Llave principal para el algoritmo (Gpo_Plan|Material_Id|Gpo_Cliente|Centro_Almacen_Id)
            (
                pl.col("Gpo_Plan").cast(pl.Utf8)
                + "|"
                + pl.col("Material_Id").cast(pl.Utf8)
                + "|"
                + pl.col("Gpo_Cliente").cast(pl.Utf8)
                + "|"
                + pl.col("Centro_Almacen_Id").cast(pl.Utf8)
            ).alias("main")
        )
        .with_columns(
            # Llave de stock (sin Gpo_Cliente) usada por MDDME para joins internos
            (
                pl.col("Gpo_Plan").cast(pl.Utf8)
                + "|"
                + pl.col("Material_Id").cast(pl.Utf8)
                + "|"
                + pl.col("Centro_Almacen_Id").cast(pl.Utf8)
            ).alias("main_stock")
        )
        .collect(engine="streaming")
    )
    # Reducir a columnas relevantes para MDDME
    columnas = [
        "Gpo_Plan",
        "Material_Id",
        "Gpo_Cliente",
        "Centro_Almacen_Id",
        "Fecha_Id",
        "Demanda_Positiva",
        "Demanda_Negativa",
        "Vencidos_Entrada",
        "main",
        "main_stock",
        "Material_Nombre",
        "Articulo_Id",
    ]
    return df.select([c for c in columnas if c in df.columns])


@dg.op(
    out={
        "df_stock": dg.Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
    },
    config_schema={
        "meses_muestra": dg.Field(int, is_required=False, default_value=12 * 5),
        "sucursal_id": dg.Field(int, is_required=False, default_value=3),
    },
)
def get_kielsa_stock_data(
    context: dg.OpExecutionContext, dwh_farinter_bi: SQLServerResource
) -> pl.DataFrame:
    """
    Lee stock desde DL_Kielsa_ExistenciaHist y genera un cierre mensual por
    (Emp_Id, Sucursal_Id, Articulo_Id) con columnas Stock_Cierre y DiasSin_Stock.

    Supuestos:
    - Se toma el último día disponible del mes (por combinación),
      agregando sobre todas las bodegas de la sucursal.
    - Stock_Cierre = suma de Cantidad_Existencia en ese último día.
    - DiasSin_Stock = mínimo de Dias_SinStock en ese último día (si cualquiera tuvo stock, el mínimo lo refleja).
    """
    meses_muestra = context.op_config["meses_muestra"]
    sucursal_id = context.op_config["sucursal_id"]
    fecha_hace_3anios = pdl.today().subtract(months=meses_muestra)

    # Usamos filtro por AnioMes_Id para reducir volumen desde SQL Server
    # Ej.: >= YYYYMM de la fecha desde
    anio_mes_min = int(fecha_hace_3anios.format("YYYYMM"))

    sql_query = f"""
    WITH base AS (
        SELECT
            EHN.Sucursal_Id,
            EHN.ArticuloPadre_Id AS Articulo_Id,
            CAST(EHN.Fecha_Id AS date) AS Fecha_Id,
            EHN.AnioMes_Id,
            EOMONTH(CAST(EHN.Fecha_Id AS date)) AS Fecha_Mes_Fin,
            EHN.CantidadPadre_Existencia,
            EHN.Dias_SinStock
        FROM DL_FARINTER.dbo.DL_Kielsa_ExistenciaHist EHN
        WHERE EHN.AnioMes_Id >= {anio_mes_min}
          AND EHN.Sucursal_Id = {sucursal_id}
    ), last_dates AS (
        SELECT
            Sucursal_Id,
            Articulo_Id,
            Fecha_Mes_Fin,
            MAX(Fecha_Id) AS Fecha_Ultima
        FROM base
        GROUP BY Sucursal_Id, Articulo_Id, Fecha_Mes_Fin
    )
    SELECT {"TOP 500000" if env_str == "local" else ""}
        b.Sucursal_Id,
        b.Articulo_Id,
        ld.Fecha_Mes_Fin AS Fecha_Id,
        SUM(b.CantidadPadre_Existencia) AS Stock_Cierre,
        MIN(b.Dias_SinStock) AS DiasSin_Stock
    FROM base b
    JOIN last_dates ld
      ON b.Sucursal_Id = ld.Sucursal_Id
     AND b.Articulo_Id = ld.Articulo_Id
     AND b.Fecha_Mes_Fin = ld.Fecha_Mes_Fin
     AND b.Fecha_Id = ld.Fecha_Ultima
    GROUP BY b.Sucursal_Id, b.Articulo_Id, ld.Fecha_Mes_Fin
    """

    df_stock = pl.read_database(sql_query, dwh_farinter_bi.get_arrow_odbc_conn_string())
    df_stock = (
        df_stock.lazy()
        .rename({"Sucursal_Id": "Centro_Almacen_Id"})
        .with_columns(
            pl.col("Articulo_Id").alias("Material_Id"),
            pl.lit("KIELSA").alias("Gpo_Plan"),
        )
        .with_columns(
            (
                pl.col("Gpo_Plan").cast(pl.Utf8)
                + "|"
                + pl.col("Material_Id").cast(pl.Utf8)
                + "|"
                + pl.col("Centro_Almacen_Id").cast(pl.Utf8)
            ).alias("main")
        )
        .with_columns(
            (
                pl.col("Gpo_Plan").cast(pl.Utf8)
                + "|"
                + pl.col("Material_Id").cast(pl.Utf8)
                + "|"
                + pl.col("Centro_Almacen_Id").cast(pl.Utf8)
            ).alias("main_stock")
        )
        .collect(engine="streaming")
    )

    columnas_stock = [
        "Gpo_Plan",
        "Material_Id",
        "Centro_Almacen_Id",
        "Fecha_Id",
        "Stock_Cierre",
        "DiasSin_Stock",
        "main",
        "main_stock",
        "Articulo_Id",
    ]
    return df_stock.select([c for c in columnas_stock if c in df_stock.columns])


# ==============================================
# Limpieza MDDME y guardado en tabla destino
# ==============================================


@dg.op(
    out={
        "df_demanda_procesada": dg.Out(
            pl.DataFrame, io_manager_key="polars_parquet_io_manager"
        )
    }
)
def procesar_con_mddme_op(
    df_demanda: pl.DataFrame, df_stock: pl.DataFrame
) -> pl.DataFrame:
    # Importar dentro de la función para evitar dependencias duras en import
    try:
        from algoritmo_mddme_gf.algoritmo_mddme import (
            procesar_con_mddme,
        )
    except ImportError as e:
        raise ImportError(f"Required forecast library unavailable: {str(e)}")

    # El algoritmo espera columnas con demanda positiva/negativa y stock
    df_out = procesar_con_mddme(
        df_demanda,
        df_stock,
        procesar_stats=True,
        umbral_min=0.4,
    )
    return df_out


@dg.op
def save_demanda_procesada(
    context: dg.OpExecutionContext,
    dwh_farinter_bi: SQLServerResource,
    demanda_procesada: pl.DataFrame,
    start_time: float,
) -> None:
    if env_str == "local":
        with pl.Config() as c:
            c.set_tbl_rows(-1)
            c.set_tbl_cols(-1)
            context.log.info(demanda_procesada.head(10).to_pandas().__repr__())
        return

    context.log.info(f"Por guardar {len(demanda_procesada)} registros")

    engine = dwh_farinter_bi.get_sqlalchemy_engine()

    manager = DataframeSQLTableManager(
        df=demanda_procesada,
        db_schema="dbo",
        table_name="BI_Kielsa_Hecho_SucArt_Demanda_Limpia",
        sqla_engine=engine,
        primary_keys=(
            "Material_Id",
            "Fecha_Id",
            "Centro_Almacen_Id",
            "Gpo_Cliente",
        ),
        temp_table_name="BI_Kielsa_Hecho_SucArt_Demanda_Limpia_NEW",
    )

    manager.upsert_dataframe()

    context.add_output_metadata(
        metadata={
            "rows": manager.filas_total_tabla or len(demanda_procesada),
            "start_time": start_time,
            "end_time": datetime.now().timestamp(),
            "duration_seconds": datetime.now().timestamp() - start_time,
        }
    )


@dg.op
def start_timer_op() -> float:
    return datetime.now().timestamp()


@dg.graph
def limpiar_demanda_kielsa_graph():
    start_time = start_timer_op()
    df_demanda = get_kielsa_demanda_data()
    df_stock = get_kielsa_stock_data()
    demanda_procesada = procesar_con_mddme_op(df_demanda, df_stock)
    return save_demanda_procesada(demanda_procesada, start_time)


BI_Kielsa_Hecho_SucArt_Demanda_Limpia = dg.AssetsDefinition.from_graph(
    graph_def=limpiar_demanda_kielsa_graph,
    keys_by_input_name={},
    keys_by_output_name={
        "result": dg.AssetKey(
            ["BI_FARINTER", "dbo", "BI_Kielsa_Hecho_SucArt_Demanda_Limpia"]
        )
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

    # env_str = "dev"

    start = datetime.now()
    with instance_for_test() as instance:
        warnings.warn(
            f"Running in env_str={env_str} mode{'", using TOP limits and not loading to SQL Server.' if env_str == 'local' else '.'}"
        )

        @dg.asset(name="between_asset")
        def mock_between_asset() -> int:  # noqa: F401
            return 1

        result = dg.materialize(
            assets=[mock_between_asset, BI_Kielsa_Hecho_SucArt_Demanda_Limpia],
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dwh_farinter_bi": dwh_farinter_bi,
                "smb_resource_staging_dagster_dwh": smb_resource_staging_dagster_dwh,
                "polars_parquet_io_manager": PolarsParquetIOManager(),
            },
        )
        print(
            result.output_for_node(BI_Kielsa_Hecho_SucArt_Demanda_Limpia.node_def.name)
        )

    end = datetime.now()
    print(f"Tiempo de ejecución: {end - start}, desde {start}, hasta {end}")
