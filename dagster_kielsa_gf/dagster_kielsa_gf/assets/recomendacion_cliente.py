from datetime import datetime, timedelta
from typing import Sequence
import warnings
import pendulum as pdl
import polars as pl
from dagster import (
    AssetChecksDefinition,
    AssetKey,
    AssetsDefinition,
    Out,
    asset,
    graph,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    op,
)

from dagster_shared_gf.automation import automation_daily_delta_2_cron
from dagster_shared_gf.resources.sql_server_resources import (
    SQLServerResource,
    dwh_farinter_dl,
)
from dagster_shared_gf.shared_variables import tags_repo

from dagster import (
    instance_for_test,
    materialize,
)

from dagster_shared_gf.resources.smb_resources import (
    smb_resource_staging_dagster_dwh,
)
from dagster_shared_gf.shared_variables import env_str




@op(out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"))
def get_customer_purchases(dwh_farinter_dl: SQLServerResource) -> pl.DataFrame:
    meses_muestra = 3
    lista_fechas_muestra = [pdl.today().subtract(months=i) for i in range(meses_muestra)]
    lista_aniomes  = [fecha.year * 100 + fecha.month for fecha in lista_fechas_muestra]

    sql_query = f"""
    SELECT 
        DC.Monedero_Id,
        FA.ArticuloPadre_Id,
        DC.Articulos
    FROM
        DL_FARINTER.dbo.DL_Kielsa_FacturasPosiciones FA
    INNER JOIN
        (SELECT --{"TOP 10000" if env_str == "local" else ""}
            M.Monedero_Id,
            COUNT(DISTINCT ArticuloPadre_Id) AS Articulos
        FROM
            DL_FARINTER.dbo.DL_Kielsa_FacturasPosiciones F
        INNER JOIN 
            DL_FARINTER.dbo.DL_Kielsa_Monedero M
            ON F.MonederoTarj_Id_Limpio = M.Monedero_Id
            AND F.Emp_Id = M.Emp_Id
        WHERE
            F.Emp_Id = 1 AND 
            F.AnioMes_Id IN ({', '.join(map(str, lista_aniomes))}) AND 
            F.TipoDoc_Id = 1 AND
            M.Activo_Indicador = 1
        GROUP BY
            M.Monedero_Id
        HAVING
            COUNT(DISTINCT ArticuloPadre_Id) > 1) DC 
    ON FA.MonederoTarj_Id_Limpio = DC.Monedero_Id
    WHERE
        FA.Emp_Id = 1 AND 
        FA.AnioMes_Id IN ({', '.join(map(str, lista_aniomes))}) AND 
        FA.TipoDoc_Id = 1
    GROUP BY
        DC.Monedero_Id,
        FA.ArticuloPadre_Id,
        DC.Articulos
    ORDER BY DC.Monedero_Id
    """
    return pl.read_database(sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string())

@op(out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"))
def generate_recommendations(df_purchases: pl.DataFrame) -> pl.DataFrame:
    """
    Genera recomendaciones de productos para cada cliente basadas en la co-ocurrencia de compras.
    
    El proceso es el siguiente:
      1. Se crea una matriz de co-ocurrencia mediante un self-join sobre 'Monedero_Id' que cuenta cuántos 
         clientes compraron cada par de artículos (excluyendo pares idénticos).
      2. Se calcula la frecuencia con la que cada cliente compró cada artículo.
      3. Para cada cliente se ordenan los artículos por frecuencia descendente y se limitan a los 5 más importantes;
         además, se utiliza el artículo con mayor frecuencia como base para la recomendación.
      4. Se une la base del cliente con la matriz de co-ocurrencia y se filtran aquellos artículos ya comprados por el cliente.
      5. Se ordenan las recomendaciones por la importancia (Clientes_Compraron) y se extraen las 5 principales por cliente.
    
    Parámetros:
        df_purchases (pl.DataFrame): DataFrame que debe contener al menos las columnas 
                                     'Monedero_Id' y 'ArticuloPadre_Id'.
    
    Retorna:
        pl.DataFrame: DataFrame con las recomendaciones por cliente.
    """
    # 1. Crear la matriz de co-ocurrencia (ordenada globalmente por importancia)
    cooccurrence = (
        df_purchases
        .join(df_purchases, on='Monedero_Id', suffix="_right")
        .filter(pl.col('ArticuloPadre_Id') != pl.col('ArticuloPadre_Id_right'))
        .group_by(['ArticuloPadre_Id', 'ArticuloPadre_Id_right'])
        .agg(pl.count().alias('Clientes_Compraron'))
        .sort('Clientes_Compraron', descending=True)
    )
    
    # 2. Calcular la frecuencia de compra por cliente y artículo
    customer_freq = (
        df_purchases
        .group_by(['Monedero_Id', 'ArticuloPadre_Id'])
        .agg(pl.count().alias("freq"))
    )
    
    # 3. Ordenar los artículos por frecuencia descendente y agrupar por cliente,
    #    limitando a los 5 artículos más importantes.
    customer_articles = (
        customer_freq
        .sort(["Monedero_Id", "freq"], descending=[False, True])
        .group_by("Monedero_Id")
        .agg([
            # Se limita a 5 artículos y se agrupan en una lista.
            pl.col("ArticuloPadre_Id").head(10).alias("Articulos_Id_Relacionados"),
            # Se toma el artículo con mayor frecuencia (el primero) para usar como base.
            pl.col("ArticuloPadre_Id").first().alias("Base_Articulo"),
            pl.lit(0).alias("Iteration")
        ])
    )
    
    # 4. Unir con la matriz de co-ocurrencia y filtrar aquellos artículos ya comprados por el cliente.
    customer_recs = (
        customer_articles
        .join(
            cooccurrence,
            left_on="Base_Articulo",
            right_on="ArticuloPadre_Id"
        )
        .filter(~pl.col("Articulos_Id_Relacionados").list.contains(pl.col("ArticuloPadre_Id_right")))
        # IMPORTANTE: Ordenar para asegurar que, para cada cliente, se consideren primero los relacionados
        # con mayor 'Clientes_Compraron'.
        .sort(["Monedero_Id", "Clientes_Compraron"], descending=[False, True])
    )
    
    # 5. Seleccionar las top 5 recomendaciones por cliente y expandir la lista para tener una fila por recomendación.
    final_recs = (
        customer_recs
        .group_by("Monedero_Id")
        .agg([
            pl.col("ArticuloPadre_Id_right").head(5).alias("Articulo_Id_Recomendado"),
            pl.col("Clientes_Compraron").head(5).alias("Clientes_Compraron"),
            pl.col("Articulos_Id_Relacionados").first(),
            pl.col("Iteration").first()
        ])
        .explode(["Articulo_Id_Recomendado", "Clientes_Compraron"])
    )
    
    return final_recs


@op
def save_recommendations(
    dwh_farinter_dl: SQLServerResource,
    recommendations: pl.DataFrame
) -> None:
    with dwh_farinter_dl.get_sqlalchemy_conn() as conn:
        recommendations.write_database(
            table_name='DL_Kielsa_Cliente_ArticuloRecomendado',
            connection=conn,
            if_table_exists='replace',
        )

@graph
def cliente_recomendacion_graph():
    df_purchases = get_customer_purchases()
    recommendations = generate_recommendations(df_purchases)
    return save_recommendations(recommendations)

DL_Kielsa_Cliente_ArticuloRecomendado = AssetsDefinition.from_graph(
    graph_def=cliente_recomendacion_graph,
    keys_by_output_name={
        "result": AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Cliente_ArticuloRecomendado"])
    },
    tags_by_output_name={
        "result": tags_repo.Daily.tag | tags_repo.UniquePeriod.tag
    },
    automation_conditions_by_output_name={"result": automation_daily_delta_2_cron}
)

all_assets = tuple(load_assets_from_current_module())
all_asset_checks: Sequence[AssetChecksDefinition] = tuple(load_asset_checks_from_current_module())


if __name__ == "__main__":
    from dagster import instance_for_test
    from dagster_polars import PolarsParquetIOManager

    start_time = datetime.now()
    with instance_for_test() as instance:
        from dagster import ResourceDefinition
        if env_str == "local":
            warnings.warn("Running in local mode, using top 10000 rows")
        @asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1

        mock_dwh_farinter_bi = ResourceDefinition.mock_resource()

        result = materialize(
            assets=[mock_between_asset, DL_Kielsa_Cliente_ArticuloRecomendado],
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dwh_farinter_bi": mock_dwh_farinter_bi,
                "smb_resource_staging_dagster_dwh": smb_resource_staging_dagster_dwh,
                "polars_parquet_io_manager": PolarsParquetIOManager(),
            },
        )
        print(result.output_for_node(DL_Kielsa_Cliente_ArticuloRecomendado.node_def.name))

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )


