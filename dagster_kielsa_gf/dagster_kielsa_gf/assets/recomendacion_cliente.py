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

from dagster_shared_gf.automation import automation_monthly_start_delta_1_cron
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


@op(
    out={
        "df_purchases": Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
        "monederos": Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
        "articulos": Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
    }
)
def get_customer_purchases(
    dwh_farinter_dl: SQLServerResource,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    meses_muestra = 2
    lista_fechas_muestra = [
        pdl.today().subtract(months=i+1) for i in range(meses_muestra)
    ]
    lista_aniomes = [fecha.year * 100 + fecha.month for fecha in lista_fechas_muestra]

    sql_query = f"""
    SELECT 
        DC.Monedero_Id,
        FA.ArticuloPadre_Id,
        --DC.Articulos,
        COUNT(*) AS Frecuencia
    FROM
        DL_FARINTER.dbo.DL_Kielsa_FacturasPosiciones FA WITH(NOLOCK)
    INNER JOIN
        (SELECT {"TOP 10000" if env_str == "local" else ""}
            M.Monedero_Id,
            COUNT(DISTINCT ArticuloPadre_Id) AS Articulos
        FROM
            DL_FARINTER.dbo.DL_Kielsa_FacturasPosiciones F WITH(NOLOCK)
        INNER JOIN 
            DL_FARINTER.dbo.DL_Kielsa_Monedero M WITH(NOLOCK)
            ON F.MonederoTarj_Id_Limpio = M.Monedero_Id
            AND F.Emp_Id = M.Emp_Id
        WHERE
            F.Emp_Id = 1 AND 
            F.AnioMes_Id IN ({", ".join(map(str, lista_aniomes))}) AND 
            F.TipoDoc_Id = 1 AND
            M.Activo_Indicador = 1 
        GROUP BY
            M.Monedero_Id
        HAVING
            COUNT(DISTINCT ArticuloPadre_Id) > 1) DC 
    ON FA.MonederoTarj_Id_Limpio = DC.Monedero_Id
    WHERE
        FA.Emp_Id = 1 AND 
        FA.AnioMes_Id IN ({", ".join(map(str, lista_aniomes))}) AND 
        FA.TipoDoc_Id = 1
    GROUP BY
        DC.Monedero_Id,
        FA.ArticuloPadre_Id,
        DC.Articulos
    ORDER BY DC.Monedero_Id, COUNT(*) DESC
    """
    main_query = pl.read_database(
        sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string()
    ).lazy().collect(streaming=True)


    pl.enable_string_cache()
    main_query = main_query.with_columns(
        pl.col("Monedero_Id").cast(pl.Categorical).alias("Monedero_Id"),
    )
    monederos = main_query.select(pl.col("Monedero_Id").unique().sort())
    main_query = main_query.with_columns(
        pl.col("ArticuloPadre_Id").cast(pl.Categorical).alias("ArticuloPadre_Id"),
    )
    articulos = main_query.select(pl.col("ArticuloPadre_Id").unique().sort())
    pl.disable_string_cache()

    return (main_query, monederos, articulos)


@op(out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"))
def generate_cooccurrence(
    df_purchases: pl.DataFrame, df_articulos: pl.DataFrame
) -> pl.DataFrame:
    """
    Crea una matriz de co-ocurrencia mediante un self-join sobre 'Monedero_Id' que cuenta cuántos
            clientes compraron cada par de artículos (excluyendo pares idénticos).
    """
    df_purchases_lazy = df_purchases.lazy()

    # Crear la matriz de co-ocurrencia (ordenada globalmente por importancia)
    cooccurrence = (
        df_purchases_lazy.join(
            df_purchases_lazy, on="Monedero_Id", suffix="_relacionado"
        )
        .filter(pl.col("ArticuloPadre_Id") != pl.col("ArticuloPadre_Id_relacionado"))
        .group_by(["ArticuloPadre_Id", "ArticuloPadre_Id_relacionado"])
        .agg(pl.len().alias("Clientes_Compraron"))
        .filter(
            pl.col("Clientes_Compraron") >= 5
        )  # Se limita a pares de artículos comprados por al menos 5 clientes.
        .sort("Clientes_Compraron", descending=True)
    )

    return cooccurrence.collect(streaming=True)


@op(out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"))
def generate_recommendations(
    df_purchases: pl.DataFrame, cooccurrence: pl.DataFrame, df_monederos: pl.DataFrame
) -> pl.DataFrame:
    """
    Genera recomendaciones de productos para cada cliente basadas en la co-ocurrencia de compras.

    El proceso es el siguiente:
      1. Se crea una matriz de co-ocurrencia mediante un self-join sobre 'Monedero_Id' que cuenta cuántos
         clientes compraron cada par de artículos (excluyendo pares idénticos).
      2. Se calcula la frecuencia con la que cada cliente compró cada artículo.
      3. Para cada cliente se ordenan los artículos por frecuencia descendente y se limitan a los 10 más importantes;
         además, se utiliza el artículo con mayor frecuencia como base para la recomendación.
      4. Se une la base del cliente con la matriz de co-ocurrencia y se filtran aquellos artículos ya comprados por el cliente.
      5. Se ordenan las recomendaciones por la importancia (Clientes_Compraron) y se extraen las 5 principales por cliente.

    Parámetros:
        df_purchases (pl.DataFrame): DataFrame que debe contener al menos las columnas
                                     'Monedero_Id' y 'ArticuloPadre_Id'.

    Retorna:
        pl.DataFrame: DataFrame con las recomendaciones por cliente.
    """
    df_purchases_lazy = df_purchases.lazy()
    cooccurrence_lazy = cooccurrence.lazy()

    final_recs: pl.DataFrame | None = None
    for client_chunk in df_monederos.iter_slices(n_rows=10000):
        chunk_rows = df_purchases_lazy.join(
            client_chunk.lazy(), on="Monedero_Id", how="inner"
        )

        # Unir con la matriz de co-ocurrencia y filtrar aquellos artículos ya comprados por el cliente.
        # Se usa un anti join para descartar recomendaciones que el cliente ya posee.
        inter = (
            chunk_rows.join(
                cooccurrence_lazy,
                left_on="ArticuloPadre_Id",
                right_on="ArticuloPadre_Id",
                # validate="m:m"
            )
            .join(
                chunk_rows,
                left_on=["Monedero_Id", "ArticuloPadre_Id_relacionado"],
                right_on=["Monedero_Id", "ArticuloPadre_Id"],
                how="anti",
            )
            .filter(
                pl.col("ArticuloPadre_Id") != pl.col("ArticuloPadre_Id_relacionado")
            )
            # Ordenar para que primero se consideren los candidatos con mayor "Clientes_Compraron"
            .sort(["Monedero_Id", "Clientes_Compraron"], descending=[False, True])
            # Seleccionar las top 5 recomendaciones por cliente e incluir la lista de artículos relacionados (los artículos base que originaron la recomendación)
            # Se agrupa por cliente y por el artículo recomendado, agrupando los artículos de base que dieron lugar a la recomendación.
            .with_columns(
                pl.col("ArticuloPadre_Id_relacionado").alias("Articulo_Id_Recomendado")
            )
            .group_by(["Monedero_Id", "Articulo_Id_Recomendado"])
            .agg(
                [
                    # Se toma el máximo de "Clientes_Compraron" (o se podría aplicar otra agregación)
                    pl.col("Clientes_Compraron").max().alias("Clientes_Compraron"),
                    # Agrupar los artículos base que originan la recomendación (sin duplicados)
                    pl.col("ArticuloPadre_Id")
                    .unique()
                    .alias("Articulos_Id_Relacionados"),
                ]
            )
            .sort(["Monedero_Id", "Clientes_Compraron"], descending=[False, True])
            # Para cada cliente, se eligen las 5 recomendaciones principales.
            .group_by("Monedero_Id")
            .agg(
                [
                    pl.col("Articulo_Id_Recomendado")
                    .head(5)
                    .alias("Articulo_Id_Recomendado"),
                    pl.col("Clientes_Compraron").head(5).alias("Clientes_Compraron"),
                    pl.col("Articulos_Id_Relacionados")
                    .head(5)
                    .alias("Articulos_Id_Relacionados"),
                ]
            )
            # Explota para obtener una fila por recomendación
            .explode(
                [
                    "Articulo_Id_Recomendado",
                    "Clientes_Compraron",
                    "Articulos_Id_Relacionados",
                ]
            )
            # Convertir la lista de Articulos_Id_Relacionados a cadena separada por comas
            .with_columns(
                pl.col("Articulos_Id_Relacionados")
                .list.eval(pl.element().cast(pl.Utf8))
                .list.join(",")
                .alias("Articulos_Id_Relacionados")
            )
            .with_columns(pl.lit(0).alias("Iteration"))
        )

        if final_recs is None:
            final_recs = inter.collect(streaming=True)
        else:
            final_recs = pl.concat([final_recs, inter.collect(streaming=True)])

    return final_recs if final_recs is not None else pl.DataFrame()


@op
def save_recommendations(
    dwh_farinter_dl: SQLServerResource, recommendations: pl.DataFrame
) -> None:
    with dwh_farinter_dl.get_sqlalchemy_conn() as conn:
        if env_str == "local":
            return
        recommendations.write_database(
            table_name="DL_Kielsa_Cliente_ArticuloRecomendado",
            connection=conn,
            if_table_exists="replace",
        )


@graph
def cliente_recomendacion_graph():
    df_purchases, df_monederos, df_articulos = get_customer_purchases()
    cooccurrence = generate_cooccurrence(df_purchases, df_articulos)
    recommendations = generate_recommendations(df_purchases, cooccurrence, df_monederos)
    return save_recommendations(recommendations)


DL_Kielsa_Cliente_ArticuloRecomendado = AssetsDefinition.from_graph(
    graph_def=cliente_recomendacion_graph,
    keys_by_output_name={
        "result": AssetKey(
            ["DL_FARINTER", "dbo", "DL_Kielsa_Cliente_ArticuloRecomendado"]
        )
    },
    tags_by_output_name={
        "result": tags_repo.Monthly
        | tags_repo.UniquePeriod
        | tags_repo.Automation
    },
    automation_conditions_by_output_name={"result": automation_monthly_start_delta_1_cron}
)

all_assets = tuple(load_assets_from_current_module())
all_asset_checks: Sequence[AssetChecksDefinition] = tuple(
    load_asset_checks_from_current_module()
)


if __name__ == "__main__":
    from dagster import instance_for_test
    from dagster_polars import PolarsParquetIOManager

    start_time = datetime.now()
    with instance_for_test() as instance:
        from dagster import ResourceDefinition

        if env_str == "local":
            warnings.warn(
                "Running in local mode, using top 10000 rows and no loading to SQL Server"
            )

        @asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1

        mock_dwh_farinter_bi = ResourceDefinition.mock_resource()
        mock_dwh_farinter_dl = ResourceDefinition.mock_resource()

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
        print(
            result.output_for_node(DL_Kielsa_Cliente_ArticuloRecomendado.node_def.name)
        )

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
