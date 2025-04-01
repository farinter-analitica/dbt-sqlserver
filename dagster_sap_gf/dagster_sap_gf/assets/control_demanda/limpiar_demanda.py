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
        "BI_SAP_Agr_SocAlmArt_Stock_Plan": In(
            dagster_type=Nothing,
        ),
    },
    out={
        "df_stock": Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
    },
    config_schema={
        "meses_muestra": Field(int, is_required=False, default_value=12 * 5)
    },
)
def get_BI_SAP_Agr_SocAlmArt_Stock_Plan_data(
    context: OpExecutionContext,
    dwh_farinter_bi: SQLServerResource,
) -> pl.DataFrame:
    meses_muestra = context.op_config["meses_muestra"]
    fecha_desde = pdl.today().subtract(months=meses_muestra)

    sql_query = f"""
    SELECT {"TOP 10000" if env_str == "local" else ""}
        [Sociedad_Id]
        ,[Centro_Almacen_Id]
        ,[Material_Id]
        --,[Anio_Id]
        --,[Mes_Id]
        ,[Fecha_Id]
        ,[Stock_Cierre]
        ,[DiasSin_Stock]
        --,[Gpo_Obs_Nombre_Corto]
        --,[Gpo_Obs_Id]
        ,[Gpo_Plan]
        --,[Sector]
        ,[Material_Nombre]
        ,[Articulo_Id]
    FROM [BI_FARINTER].[dbo].[BI_SAP_Agr_SocAlmArt_Stock_Plan]
    WHERE Fecha_Id >= '{fecha_desde.strftime("%Y%m%d")}'
    --AND Gpo_Obs_Nombre_Corto IN ('Farma_Imp_Cod','Farma_Imp_Exc','Cons_Imp_Exc')        
    """
    main_query = (
        pl.read_database(sql_query, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .cast({pl.Decimal: pl.Float64, pl.Date: pl.Datetime})
        .collect(engine="streaming")
    )
    return main_query


@op(
    ins={
        "BI_SAP_Agr_SocAlmArtGpoCli_Demanda_Plan": In(
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
def get_BI_SAP_Agr_SocAlmArtGpoCli_Demanda_Plan_data(
    context: OpExecutionContext,
    dwh_farinter_bi: SQLServerResource,
) -> pl.DataFrame:
    meses_muestra = context.op_config["meses_muestra"]
    fecha_desde = pdl.today().subtract(months=meses_muestra)

    sql_query = f"""
    SELECT {"TOP 10000" if env_str == "local" else ""}
        [Sociedad_Id]
        ,[Centro_Almacen_Id]
        ,[Material_Id]
        ,[Gpo_Cliente]
        --,[Anio_Id]
        --,[Mes_Id]
        ,[Fecha_Id]
        ,[Demanda_Positiva]
        ,[Demanda_Negativa]
        ,[Vencidos_Entrada]
        --,[Gpo_Obs_Nombre_Corto]
        --,[Gpo_Obs_Id]
        ,[Gpo_Plan]
        ,[Sector]
        ,[Material_Nombre]
        ,[Articulo_Id]
    FROM [BI_FARINTER].[dbo].[BI_SAP_Agr_SocAlmArtGpoCli_Demanda_Plan]
    WHERE Fecha_Id >= '{fecha_desde.strftime("%Y%m%d")}'
    --AND Gpo_Obs_Nombre_Corto IN ('Farma_Imp_Cod','Farma_Imp_Exc','Cons_Imp_Exc')        
    """
    main_query = (
        pl.read_database(sql_query, dwh_farinter_bi.get_arrow_odbc_conn_string())
        .lazy()
        .cast({pl.Decimal: pl.Float64, pl.Date: pl.Datetime})
        .collect(engine="streaming")
    )
    return main_query


@op(
    out={
        "df_demanda_procesada": Out(
            pl.DataFrame, io_manager_key="polars_parquet_io_manager"
        ),
    },
)
def procesar_con_mddme_op(
    df_demanda: pl.DataFrame,
    df_stock: pl.DataFrame,
) -> pl.DataFrame:
    # Importar dentro de la función para evitar errores de carga de librerias externas
    # Esto efectivamente permite que la ubicacion de codigo cargue aunque la libreria
    # no este instalada.
    try:
        from algoritmo_mddme_gf.algoritmo_mddme import (
            procesar_con_mddme,
        )
    except ImportError as e:
        raise ImportError(f"Required forecast library unavailable: {str(e)}")

    return procesar_con_mddme(df_demanda, df_stock, procesar_stats=True, umbral_min=0.4)


@op
def save_demanda_procesada(
    dwh_farinter_bi: SQLServerResource, demanda_procesada: pl.DataFrame
) -> None:
    if env_str == "local":
        with pl.Config() as c:
            c.set_tbl_rows(-1)
            c.set_tbl_cols(-1)
            print(demanda_procesada.head(10))
            print(demanda_procesada.describe())
        return
    print(f"Por guardar {len(demanda_procesada)} registros")
    with dwh_farinter_bi.get_sqlalchemy_conn() as conn:
        sg = SQLScriptGenerator(
            primary_keys=(
                "Material_Id",
                "Fecha_Id",
                "Centro_Almacen_Id",
                "Gpo_Cliente",
            ),
            db_schema="dbo",
            table_name="BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia",
            df=demanda_procesada,
            temp_table_name="BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia_NEW",
        )

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
        demanda_procesada.write_database(
            table_name=sg.temp_table_name,
            connection=conn,
            if_table_exists="append",
        )

        dwh_farinter_bi.execute_and_commit(
            sg.primary_key_table_sql_script(temp=True), connection=conn
        )

        dwh_farinter_bi.execute_and_commit(sg.swap_table_with_temp(), connection=conn)


@graph
def limpiar_demanda_graph(
    BI_SAP_Agr_SocAlmArtGpoCli_Demanda_Plan, BI_SAP_Agr_SocAlmArt_Stock_Plan
):
    df_demanda = get_BI_SAP_Agr_SocAlmArtGpoCli_Demanda_Plan_data(
        BI_SAP_Agr_SocAlmArtGpoCli_Demanda_Plan
    )
    df_stock = get_BI_SAP_Agr_SocAlmArt_Stock_Plan_data(BI_SAP_Agr_SocAlmArt_Stock_Plan)
    demanda_procesada_mddme = procesar_con_mddme_op(df_demanda, df_stock)
    return save_demanda_procesada(demanda_procesada_mddme)


BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia = AssetsDefinition.from_graph(
    graph_def=limpiar_demanda_graph,
    keys_by_input_name={
        "BI_SAP_Agr_SocAlmArtGpoCli_Demanda_Plan": AssetKey(
            ["BI_FARINTER", "dbo", "BI_SAP_Agr_SocAlmArtGpoCli_Demanda_Plan"]
        ),
        "BI_SAP_Agr_SocAlmArt_Stock_Plan": AssetKey(
            ["BI_FARINTER", "dbo", "BI_SAP_Agr_SocAlmArt_Stock_Plan"]
        ),
    },
    keys_by_output_name={
        "result": AssetKey(
            ["BI_FARINTER", "dbo", "BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia"]
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
            warnings.warn(
                "Running in local mode, using top 10000 rows and no loading to SQL Server"
            )

        @asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1

        mock_dwh_farinter_bi = ResourceDefinition.mock_resource()
        mock_dwh_farinter_dl = ResourceDefinition.mock_resource()

        result = materialize(
            assets=[mock_between_asset, BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia],
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dwh_farinter_bi": dwh_farinter_bi,
                "smb_resource_staging_dagster_dwh": smb_resource_staging_dagster_dwh,
                "polars_parquet_io_manager": PolarsParquetIOManager(),
            },
        )
        print(
            result.output_for_node(
                BI_SAP_Hecho_SocAlmArtGpoCli_Demanda_Limpia.node_def.name
            )
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
