from dagster import (
    asset,
    AssetKey,
    load_assets_from_current_module,
    load_asset_checks_from_current_module,
    build_last_update_freshness_checks,
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetsDefinition,
    multi_asset,
    AssetSpec,
    Output,
    AssetOut,
    Field
)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import (
    filter_assets_by_tags,
    get_all_instances_of_class,
)
from dagster_shared_gf.shared_variables import TagsRepositoryGF as tags_repo, env_str
from datetime import timedelta, datetime, date
from typing import Sequence, List, Mapping, Dict, Any


@asset(key_prefix= ["DL_FARINTER", "dbo"]
        , tags=tags_repo.Daily.tag | tags_repo.Hourly.tag | tags_repo.Monthly.tag
        , compute_kind="sqlserver"
        , config_schema={"p_fecha_desde":  Field(str, is_required=False, default_value="")
                        }
        , description="EXEC dbo.DL_paCargarKielsa_FacturaEncabezado condicional, por hora sin parametros, por dia los ultimos 7 dias, por mes, todo el mes anterior."
        )
def DL_Kielsa_FacturaEncabezado(context: AssetExecutionContext
                , dwh_farinter_dl: SQLServerResource
                ) -> None: 
    final_query = r"EXEC dbo.DL_paCargarKielsa_FacturaEncabezado"
    from_date: date = None
    if context.op_execution_context.op_config.get("p_fecha_desde") != "" and context.op_execution_context.op_config.get("p_fecha_desde") :
        from_date = datetime.fromisoformat(context.op_execution_context.op_config.get("p_fecha_desde")).date()
    elif context.job_def.tags.get(tags_repo.Daily.key) is not None:
        from_date = datetime.now().date() - timedelta(days=7)
    elif context.job_def.tags.get(tags_repo.Hourly.key) is not None:
        from_date = datetime.now().date() - timedelta(days=1)
    elif context.job_def.tags.get(tags_repo.Monthly.key) is not None:
        from_date = (datetime.now().date() - timedelta(months=1)).replace(day=1)
    
    if from_date:
        final_query = final_query + (f" @FechaDesdeSP='{from_date.isoformat()}'")

    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit = True) as conn:
        #print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection = conn)


@asset(key_prefix= ["DL_FARINTER", "dbo"]
        , tags=tags_repo.Daily.tag | tags_repo.Hourly.tag | tags_repo.Monthly.tag
        , compute_kind="sqlserver"
        , config_schema={"p_fecha_desde":  Field(str, is_required=False, default_value="")
                        }
        , description="EXEC dbo.DL_paCargarKielsa_FacturasPosiciones condicional, por hora sin parametros, por dia los ultimos 7 dias, por mes, todo el mes anterior."
        )
def DL_Kielsa_FacturasPosiciones(context: AssetExecutionContext
                , dwh_farinter_dl: SQLServerResource
                ) -> None: 
    final_query = r"EXEC dbo.DL_paCargarKielsa_FacturasPosiciones"
    from_date: date = None
    if context.op_execution_context.op_config.get("p_fecha_desde") != "" and context.op_execution_context.op_config.get("p_fecha_desde") :
        from_date = datetime.fromisoformat(context.op_execution_context.op_config.get("p_fecha_desde")).date()
    elif context.job_def.tags.get(tags_repo.Daily.key) is not None:
        from_date = datetime.now().date() - timedelta(days=7)
    elif context.job_def.tags.get(tags_repo.Hourly.key) is not None:
        from_date = datetime.now().date() - timedelta(days=1)
    elif context.job_def.tags.get(tags_repo.Monthly.key) is not None:
        from_date = (datetime.now().date() - timedelta(months=1)).replace(day=1)
    
    if from_date:
        final_query = final_query + (f" @FechaDesdeSP='{from_date.isoformat()}'")
    
    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit = True) as conn:
        #print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection = conn)


@asset(key_prefix= ["DL_FARINTER", "dbo"]
        , tags=tags_repo.Daily.tag | tags_repo.Hourly.tag | tags_repo.Monthly.tag
        , compute_kind="sqlserver"
        , config_schema={"p_actualizar_todo":  Field(bool, is_required=False, default_value=False)
                        }
        , description="EXEC dbo.DL_paCargarKielsa_Monedero_Tarjetas_Replica condicional, por hora sin parametros, por dia actualizar todo."
        )
def DL_Kielsa_Monedero_Tarjetas_Replica(context: AssetExecutionContext
                , dwh_farinter_dl: SQLServerResource
                ) -> None: 
    final_query = r"EXEC dbo.DL_paCargarKielsa_Monedero_Tarjetas_Replica"
    actualizar_todo: int = None
    if context.op_execution_context.op_config.get("p_actualizar_todo"):
        actualizar_todo = 1
    elif context.job_def.tags.get(tags_repo.Daily.key) is not None or context.job_def.tags.get(tags_repo.Monthly.key) is not None:
        actualizar_todo = 1
    elif context.job_def.tags.get(tags_repo.Hourly.key) is not None:
        actualizar_todo = 0
    
    if actualizar_todo:
        final_query = final_query + (f" @p_IndicadorActualizarTodo='{str(actualizar_todo)}'")
    
    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit = True) as conn:
        #print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection = conn)

@asset(key_prefix= ["DL_FARINTER", "dbo"]
        , tags=tags_repo.Daily.tag | tags_repo.Hourly.tag | tags_repo.Monthly.tag
        , compute_kind="sqlserver"
        , config_schema={"p_actualizar_todo":  Field(bool, is_required=False, default_value=False)
                        }
        , description="EXEC dbo.DL_paCargarKielsa_Articulo condicional, por hora sin parametros, por dia actualizar todo."
        )
def DL_Kielsa_Articulo(context: AssetExecutionContext
                , dwh_farinter_dl: SQLServerResource
                ) -> None: 
    final_query = r"EXEC dbo.DL_paCargarKielsa_Articulo"
    actualizar_todo: int = None
    if context.op_execution_context.op_config.get("p_actualizar_todo"):
        actualizar_todo = 1
    elif context.job_def.tags.get(tags_repo.Daily.key) is not None or context.job_def.tags.get(tags_repo.Monthly.key) is not None:
        actualizar_todo = 1
    elif context.job_def.tags.get(tags_repo.Hourly.key) is not None:
        actualizar_todo = 0
    
    if actualizar_todo:
        final_query = final_query + (f" @IndicadorActualizarTodo='{str(actualizar_todo)}'")
    
    context.log.info(final_query)

    with dwh_farinter_dl.get_connection("DL_FARINTER", autocommit = True) as conn:
        #print(final_query)
        dwh_farinter_dl.execute_and_commit(final_query, connection = conn)

all_assets = load_assets_from_current_module(group_name="ldcom_etl_dwh")

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
# print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
    lower_bound_delta=timedelta(hours=13),
    deadline_cron="0 10-16 * * 1-6",
)

all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
