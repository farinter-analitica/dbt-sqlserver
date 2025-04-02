from dagster import (
    AssetKey,
    asset,
    load_assets_from_current_module,
    load_asset_checks_from_current_module,
    AssetChecksDefinition,
    AssetExecutionContext,
    Field,
)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import tags_repo, env_str
from dagster_shared_gf.automation import automation_hourly_delta_12_cron
from datetime import timedelta, datetime, date
from typing import Sequence
import polars as pl
import re


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag | tags_repo.Monthly.tag,
    compute_kind="sqlserver",
    config_schema={"p_fecha_desde": Field(str, is_required=False, default_value="")},
    description="EXEC dbo.DL_paCargarKielsa_FacturaEncabezado condicional, por hora hoy/ayer, por dia los ultimos 7 dias, por mes, todo el mes anterior.",
    automation_condition=automation_hourly_delta_12_cron,
)
def DL_Kielsa_FacturaEncabezado(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    final_query = r"EXEC dbo.DL_paCargarKielsa_FacturaEncabezado"
    from_date: date = datetime.now().date() - timedelta(days=1)
    if context.op_execution_context.op_config.get(
        "p_fecha_desde"
    ) != "" and context.op_execution_context.op_config.get("p_fecha_desde"):
        from_date = datetime.fromisoformat(
            context.op_execution_context.op_config.get("p_fecha_desde")
        ).date()
    elif context.run_tags.get(tags_repo.Daily.key) is not None:
        from_date = datetime.now().date() - timedelta(days=7)
    elif context.run_tags.get(tags_repo.HourlyAdditional.key) is not None or (
        context.run_tags.get(tags_repo.AutomationHourly.key) is not None
        and datetime.now().hour not in [12, 4]
    ):  # Actualizar desde dia de ayer a las 12 y las 4 de la noche
        from_date = datetime.now().date() - timedelta(days=0)
    elif context.run_tags.get(tags_repo.AutomationHourly.key) is not None:
        from_date = datetime.now().date() - timedelta(days=1)
    elif context.run_tags.get(tags_repo.Monthly.key) is not None:
        from_date = (datetime.now().date().replace(day=1) - timedelta(days=1)).replace(
            day=1
        )

    if from_date:
        final_query = final_query + (f" @FechaDesdeSP='{from_date.isoformat()}'")

    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit=True) as conn:
        # print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection=conn)


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag | tags_repo.Monthly.tag,
    compute_kind="sqlserver",
    config_schema={"p_fecha_desde": Field(str, is_required=False, default_value="")},
    description="EXEC dbo.DL_paCargarKielsa_FacturasPosiciones condicional, por hora hoy/ayer, por dia los ultimos 7 dias, por mes, todo el mes anterior.",
    automation_condition=automation_hourly_delta_12_cron,
)
def DL_Kielsa_FacturasPosiciones(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    final_query = r"EXEC dbo.DL_paCargarKielsa_FacturasPosiciones"
    from_date: date = datetime.now().date() - timedelta(days=1)
    if context.op_execution_context.op_config.get(
        "p_fecha_desde"
    ) != "" and context.op_execution_context.op_config.get("p_fecha_desde"):
        from_date = datetime.fromisoformat(
            context.op_execution_context.op_config.get("p_fecha_desde")
        ).date()
    elif context.run_tags.get(tags_repo.Daily.key) is not None:
        from_date = datetime.now().date() - timedelta(days=7)
    elif context.run_tags.get(tags_repo.HourlyAdditional.key) is not None or (
        context.run_tags.get(tags_repo.AutomationHourly.key) is not None
        and datetime.now().hour not in [12, 4]
    ):  # Actualizar desde dia de ayer a las 12 y las 4 de la noche
        from_date = datetime.now().date() - timedelta(days=0)
    elif context.run_tags.get(tags_repo.AutomationHourly.key) is not None:
        from_date = datetime.now().date() - timedelta(days=1)
    elif context.run_tags.get(tags_repo.Monthly.key) is not None:
        from_date = (datetime.now().date().replace(day=1) - timedelta(days=1)).replace(
            day=1
        )

    if from_date:
        final_query = final_query + (f" @FechaDesdeSP='{from_date.isoformat()}'")

    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit=True) as conn:
        dwh_farinter_dl.execute_and_commit(final_query, connection=conn)


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag | tags_repo.Monthly.tag,
    compute_kind="sqlserver",
    config_schema={"p_fecha_desde": Field(str, is_required=False, default_value="")},
    description="EXEC dbo.DL_paCargarKielsa_FacturaPosicionDescuento condicional, por hora hoy/ayer, por dia los ultimos 7 dias, por mes, todo el mes anterior.",
    automation_condition=automation_hourly_delta_12_cron,
)
def DL_Kielsa_FacturaPosicionDescuento(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    final_query = r"EXEC dbo.DL_paCargarKielsa_FacturaPosicionDescuento"
    from_date: date = datetime.now().date() - timedelta(days=1)
    if context.op_execution_context.op_config.get(
        "p_fecha_desde"
    ) != "" and context.op_execution_context.op_config.get("p_fecha_desde"):
        from_date = datetime.fromisoformat(
            context.op_execution_context.op_config.get("p_fecha_desde")
        ).date()
    elif context.run_tags.get(tags_repo.Daily.key) is not None:
        from_date = datetime.now().date() - timedelta(days=7)
    elif context.run_tags.get(tags_repo.HourlyAdditional.key) is not None or (
        context.run_tags.get(tags_repo.AutomationHourly.key) is not None
        and datetime.now().hour not in [12, 4]
    ):  # Actualizar desde dia de ayer a las 12 y las 4 de la noche
        from_date = datetime.now().date() - timedelta(days=0)
    elif context.run_tags.get(tags_repo.AutomationHourly.key) is not None:
        from_date = datetime.now().date() - timedelta(days=1)
    elif context.run_tags.get(tags_repo.Monthly.key) is not None:
        from_date = (datetime.now().date().replace(day=1) - timedelta(days=1)).replace(
            day=1
        )

    if from_date:
        final_query = final_query + (f" @FechaDesdeSP='{from_date.isoformat()}'")

    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit=True) as conn:
        dwh_farinter_dl.execute_and_commit(final_query, connection=conn)


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag | tags_repo.Monthly.tag,
    compute_kind="sqlserver",
    config_schema={
        "p_actualizar_todo": Field(bool, is_required=False, default_value=False)
    },
    description="EXEC dbo.DL_paCargarKielsa_Monedero_Tarjetas_Replica condicional, por hora sin parametros, por dia actualizar todo.",
    automation_condition=automation_hourly_delta_12_cron,
)
def DL_Kielsa_Monedero_Tarjetas_Replica(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    final_query = r"EXEC dbo.DL_paCargarKielsa_Monedero_Tarjetas_Replica"
    actualizar_todo: int
    if context.op_execution_context.op_config.get("p_actualizar_todo"):
        actualizar_todo = 1
    elif (
        context.run_tags.get(tags_repo.Daily.key) is not None
        or context.run_tags.get(tags_repo.Monthly.key) is not None
    ):
        actualizar_todo = 1
    else:
        actualizar_todo = 0

    if actualizar_todo:
        final_query = final_query + (
            f" @p_IndicadorActualizarTodo='{str(actualizar_todo)}'"
        )

    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit=True) as conn:
        # print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection=conn)


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag | tags_repo.Monthly.tag,
    compute_kind="sqlserver",
    config_schema={
        "p_actualizar_todo": Field(bool, is_required=False, default_value=False)
    },
    description="EXEC dbo.DL_paCargarKielsa_Articulo condicional, por hora sin parametros, por dia actualizar todo.",
    automation_condition=automation_hourly_delta_12_cron,
)
def DL_Kielsa_Articulo(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    final_query = r"EXEC dbo.DL_paCargarKielsa_Articulo"
    actualizar_todo: int = 0
    if context.op_execution_context.op_config.get("p_actualizar_todo"):
        actualizar_todo = 1
    elif (
        context.run_tags.get(tags_repo.Daily.key) is not None
        or context.run_tags.get(tags_repo.Monthly.key) is not None
    ):
        actualizar_todo = 1
    elif (
        context.run_tags.get(tags_repo.AutomationHourly.key) is not None
        or context.run_tags.get(tags_repo.HourlyAdditional.key) is not None
    ):
        actualizar_todo = 0

    if actualizar_todo:
        final_query = final_query + (
            f" @IndicadorActualizarTodo='{str(actualizar_todo)}'"
        )

    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit=True) as conn:
        # print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection=conn)


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag | tags_repo.Monthly.tag,
    compute_kind="sqlserver",
    config_schema={
        "p_actualizar_todo": Field(bool, is_required=False, default_value=False)
    },
    description="EXEC dbo.DL_paCargarKielsa_Articulo_x_Bodega condicional, por hora sin parametros, por dia actualizar todo.",
    automation_condition=automation_hourly_delta_12_cron,
)
def DL_Kielsa_Articulo_x_Bodega(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    final_query = r"EXEC dbo.DL_paCargarKielsa_Articulo_x_Bodega"
    actualizar_todo: int = 0
    if context.op_execution_context.op_config.get("p_actualizar_todo"):
        actualizar_todo = 1
    elif (
        context.run_tags.get(tags_repo.Daily.key) is not None
        or context.run_tags.get(tags_repo.Monthly.key) is not None
    ):
        actualizar_todo = 1
    elif (
        context.run_tags.get(tags_repo.AutomationHourly.key) is not None
        or context.run_tags.get(tags_repo.HourlyAdditional.key) is not None
    ):
        actualizar_todo = 0

    if actualizar_todo:
        final_query = final_query + (
            f" @p_IndicadorActualizarTodo='{str(actualizar_todo)}'"
        )

    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit=True) as conn:
        dwh_farinter_dl.execute_and_commit(final_query, connection=conn)


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.Daily.tag | tags_repo.AutomationHourly.tag | tags_repo.Monthly.tag,
    compute_kind="sqlserver",
    config_schema={
        "p_actualizar_todo": Field(bool, is_required=False, default_value=False)
    },
    description="EXEC dbo.DL_paCargarKielsa_Articulo_x_Sucursal condicional, por hora sin parametros, por dia actualizar todo.",
    automation_condition=automation_hourly_delta_12_cron,
)
def DL_Kielsa_Articulo_x_Sucursal(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
) -> None:
    final_query = r"EXEC dbo.DL_paCargarKielsa_Articulo_x_Sucursal"
    actualizar_todo: int = 0
    if context.op_execution_context.op_config.get("p_actualizar_todo"):
        actualizar_todo = 1
    elif (
        context.run_tags.get(tags_repo.Daily.key) is not None
        or context.run_tags.get(tags_repo.Monthly.key) is not None
    ):
        actualizar_todo = 1
    elif (
        context.run_tags.get(tags_repo.AutomationHourly.key) is not None
        or context.run_tags.get(tags_repo.HourlyAdditional.key) is not None
    ):
        actualizar_todo = 0

    if actualizar_todo:
        final_query = final_query + (
            f" @p_IndicadorActualizarTodo='{str(actualizar_todo)}'"
        )

    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit=True) as conn:
        dwh_farinter_dl.execute_and_commit(final_query, connection=conn)


@asset(
    key_prefix=["BI_FARINTER", "dbo"],
    tags=tags_repo.Daily.tag | {"dagster/storage_kind": "sqlserver"},
    compute_kind="polars",
    config_schema={
        "p_actualizar_todo": Field(bool, is_required=False, default_value=False)
    },
    deps=[AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_PV_Alerta"])],
)
def BI_Dim_MecanicaCanje_Kielsa(
    context: AssetExecutionContext, dwh_farinter_bi: SQLServerResource
) -> None:
    table_name = "BI_Dim_MecanicaCanje_Kielsa"
    # Read data from SQL Server
    df: pl.DataFrame
    with dwh_farinter_bi.get_sqlalchemy_conn() as conn:
        # conn.execute(f"USE {database}; SELECT Division_Id, Division_Nombre FROM dbo.DL_Edit_Division_SAP")
        df = pl.read_database(
            """SELECT Emp_Id, Alerta_Id, Nombre FROM DL_FARINTER.dbo.DL_Kielsa_PV_Alerta
                              WHERE Nombre LIKE '%CANJE%'
                              """,
            connection=conn,
        )

    # Define regular expression patterns
    pattern_canje = r"(?i)\bCANJES?\b"  # Matches 'CANJE' or 'CANJES', case-insensitive
    pattern_xy = r"\d+\s*\+\s*\d+"  # Matches 'x+y' patterns, allowing spaces around '+'

    # Function to extract 'Tipo' from 'Nombre'
    def extract_tipo(nombre):
        # Search for 'CANJE' or 'CANJES'
        match_canje = re.search(pattern_canje, nombre, re.IGNORECASE)
        if match_canje:
            # Always use 'CANJE' (singular) for 'Tipo'
            canje_word = "CANJE"
            # Find all 'x+y' patterns in the string
            xy_patterns = re.findall(pattern_xy, nombre)
            if xy_patterns:
                # Remove spaces within 'x+y' patterns for consistency
                xy_patterns_cleaned = [
                    xy_pattern.replace(" ", "") for xy_pattern in xy_patterns
                ]
                # Create 'Tipo' entries by combining 'CANJE' with each 'x+y' pattern
                tipos = [f"{xy_pattern}" for xy_pattern in xy_patterns_cleaned]
                # Join multiple 'Tipo' entries with a comma
                tipo = f"{canje_word} {', '.join(set(tipos))}"
                return tipo
            else:
                # If no 'x+y' patterns found, return 'CANJE'
                return canje_word
        else:
            return None  # If 'CANJE' not found

    # Apply the function to extract 'Tipo'
    df = df.with_columns(
        pl.col("Nombre")
        .map_elements(extract_tipo, return_dtype=pl.String)
        .fill_null("CANJE")
        .alias("Mecanica_Tipo"),
        pl.concat_str(
            [
                pl.col("Emp_Id").cast(pl.String()),
                pl.lit("-"),
                pl.col("Alerta_Id").cast(pl.String()),
            ]
        ).alias("Mecanica_Id"),
        pl.col("Nombre").alias("Mecanica_Nombre"),
    ).drop("Alerta_Id", "Nombre")

    # Display the DataFrame
    with pl.Config() as config:
        config.set_tbl_cols(10)
        config.set_tbl_rows(10)
        context.log.debug(df.head())

    # Define a staging table name
    staging_table_name = f"{table_name}_temp_dagster"

    # Write the DataFrame to the staging table
    # Use 'replace' to create or replace the staging table
    with dwh_farinter_bi.get_sqlalchemy_conn() as conn:
        df.write_database(
            staging_table_name, connection=conn, if_table_exists="replace"
        )

    key_columns = ["Mecanica_Id"]
    key_columns_str = " AND ".join(
        [f"target.{col} = source.{col}" for col in key_columns]
    )
    non_key_columns = [col for col in df.columns if col not in key_columns]
    update_columns_str = ", ".join(
        [f"target.{col} = source.{col}" for col in non_key_columns]
    )
    insert_columns_str = ", ".join(df.columns)
    insert_values_str = ", ".join([f"source.{col}" for col in df.columns])
    except_target_str = ", ".join([f"target.{col}" for col in non_key_columns])
    except_source_str = ", ".join([f"source.{col}" for col in non_key_columns])
    # Build the MERGE SQL statement
    merge_sql = f"""
    MERGE INTO {table_name} AS target
    USING {staging_table_name} AS source
    ON ({key_columns_str})
    WHEN MATCHED 
    AND EXISTS (SELECT {except_target_str} EXCEPT SELECT {except_source_str}) 
    THEN
        UPDATE SET {update_columns_str}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns_str})
        VALUES ({insert_values_str});
    """

    context.log.debug(merge_sql)

    extra_sql = f"""
		if not exists(select * from dbo.BI_Dim_MecanicaCanje_Kielsa where Mecanica_Id = 'x')
		
            INSERT INTO {table_name} (
                Mecanica_Id,
                Mecanica_Nombre,
                Mecanica_Tipo,
                Emp_Id
            )
            SELECT 'x', 'No Aplica', 'No Aplica', 0
       ;

            INSERT INTO {table_name} (
                Mecanica_Id,
                Mecanica_Nombre,
                Mecanica_Tipo,
                Emp_Id
            )
            SELECT t.Mecanica_Id, t.Mecanica_Nombre, t.Mecanica_Tipo, t.Emp_Id
            FROM (
                SELECT '1-142' AS Mecanica_Id, 'Canje borrado en BD' AS Mecanica_Nombre, 'Canje borrado en BD' AS Mecanica_Tipo, 1 AS Emp_Id
                UNION ALL
                SELECT '1-165', 'Canje borrado en BD', 'Canje borrado en BD', 1
            ) t
            WHERE NOT EXISTS (
                SELECT 1
                FROM dbo.BI_Dim_MecanicaCanje_Kielsa e
                WHERE e.Mecanica_Id = t.Mecanica_Id
            );              
                      """

    # Execute the MERGE statement within a transaction
    with dwh_farinter_bi.get_sqlalchemy_conn() as conn:
        exec = dwh_farinter_bi.execute_and_commit
        exec(merge_sql, connection=conn)
        # Optionally, drop or truncate the staging table
        exec(f"DROP TABLE {staging_table_name}", connection=conn)
        exec(extra_sql, connection=conn)


all_assets = tuple(load_assets_from_current_module(group_name="ldcom_etl_dwh"))


all_asset_checks: Sequence[AssetChecksDefinition] = tuple(
    load_asset_checks_from_current_module()
)

if __name__ == "__main__":
    from unittest.mock import MagicMock, patch
    from dagster import materialize_to_memory

    def test_BI_Dim_MecanicaCanje_Kielsa():
        mock_data = pl.from_dict(
            {
                "Emp_Id": list(range(1, 26)),
                "Alerta_Id": list(range(1, 26)),
                "Nombre": [
                    "ALERTA CANJES 1+1 GRUPO1",
                    "ALERTA CANJES 1+1 GRUPO2",
                    "ALERTA CANJE 1+1 GRUPO3",
                    "ALERTA CANJES 2+1 GRUPO1",
                    "CANJES 3+1",
                    "CANJES 2+1",
                    "CANJE 1+1",
                    "CANJE 4+1",
                    "CANJES 20+10",
                    "CANJES 5+1",
                    "CANJE 2+1 LUVECK",
                    "CANJE 15+ 5 TAB NEWPORT",
                    "CANJE 1+1 CJA 15+15 TAB NEWPOR",
                    "CANJE FINLAY TABL 5+3 5+3.",
                    "CANJE  3+1 ACROMAX",
                    "CANJE 2+1 ZINEREO",
                    "CANJE  8+8 TAB  RAVEN",
                    "CANJE  1+1 ULTRADOCEPLEX LIQUI",
                    "CANJE 2+1 MEDIKEM",
                    "CANJE 2+1 MEFASA  CARDIO",
                    "CANJE 2+1 RODIM",
                    "CANJE  3+1  SOPHIA",
                    "CANJE 2+1 GLAXO",
                    "CANJE  2+1 RAVEN",
                    "CANJE 2+1 MARCA PROPIAS",
                ],
            }
        )
        with (
            patch(
                "polars.DataFrame.write_database", MagicMock(return_value=MagicMock())
            ) as mock_write_database,
            patch(
                "polars.read_database", MagicMock(return_value=mock_data)
            ) as mock_read_database,
        ):
            #      patch("dagster_shared_gf.resources.sql_server_resources.dwh_farinter_bi.get_sqlalchemy_conn", MagicMock()) as mock_get_conn:

            # # Mock connection and execute methods
            # mock_conn = mock_get_conn.return_value.__enter__.return_value
            # mock_conn.execute = MagicMock()

            materialize_to_memory(
                [BI_Dim_MecanicaCanje_Kielsa],
                resources={"dwh_farinter_bi": MagicMock()},
            )
            assert mock_write_database.call_count > 0
            assert mock_read_database.call_count > 0

    # prueba funcional
    def test_BI_Dim_MecanicaCanje_Kielsa_funcional():
        from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_bi

        materialize_to_memory(
            [BI_Dim_MecanicaCanje_Kielsa],
            resources={"dwh_farinter_bi": dwh_farinter_bi},
        )

    # run tests
    test_BI_Dim_MecanicaCanje_Kielsa() if 0 == 1 else print(
        "Skipping test_BI_Dim_MecanicaCanje_Kielsa"
    )
    test_BI_Dim_MecanicaCanje_Kielsa_funcional() if 1 == 1 and env_str in [
        "local",
        "dev",
    ] else print("Skipping test_BI_Dim_MecanicaCanje_Kielsa_funcional")
