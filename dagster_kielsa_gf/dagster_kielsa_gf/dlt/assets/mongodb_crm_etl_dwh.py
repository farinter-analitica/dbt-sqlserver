from datetime import datetime, timedelta
from itertools import chain
from typing import Sequence

import dlt
import dlt.extract
from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetKey,
    RunConfig,
    SourceAsset,
    asset_check,
    build_last_update_freshness_checks,
    instance_for_test,
    load_asset_checks_from_current_module,
)
from dlt.common import pendulum

from dagster_shared_gf.automation import automation_hourly_cron_prd
from dagster_shared_gf.dlt_shared.mongodb.custom_dagster_helpers import (
    DltIncrementalPartialConfig as IncConfig,
    ColConfigs,
    DltResourceCollectionConfig as DLTRColl,
    dlt_mongodb_asset_factory,
)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import (
    filter_assets_by_tags,
    get_for_current_env,
)
from dagster_shared_gf.shared_variables import default_timezone_teg, tags_repo
from dagster_shared_gf.dlt_shared.mongodb import mongodb


read_source_config_updated_at: ColConfigs = (
    DLTRColl(
        collection_name="crm_email",
    ),
    DLTRColl(
        collection_name="crm_sms",
    ),
    DLTRColl(
        collection_name="crm_incident",
    ),
    DLTRColl(
        collection_name="crm_call",
        columns_hints={
            "caller_id": {"data_type": "bigint"},
            "callee_id": {"data_type": "bigint"},
        },
    ),
    DLTRColl(
        collection_name="constantcontactcampaigns",
        table_new_name="constant_contact_campaigns",
    ),
    DLTRColl(
        collection_name="campaignsRecetas",
        columns_to_remove=("created_at",),
        incrementals=(IncConfig(cursor_path="updatedAt"),),
        automation_condition=automation_hourly_cron_prd,
    ),
    DLTRColl(
        collection_name="clientToCall",
        incrementals=(IncConfig(cursor_path="updatedAt"),),
        automation_condition=automation_hourly_cron_prd,
    ),
    DLTRColl(
        collection_name="crm_list",
        incrementals=(IncConfig(cursor_path="updatedAt"),),
    ),
)


collection_person = DLTRColl(
    collection_name="crm_person",
    columns_hints={
        "id": {"data_type": "bigint"},
        "treatment": {"data_type": "text"},
        "first_name": {"data_type": "text"},
        "middle_name": {"data_type": "text"},
        "last_name": {"data_type": "text"},
        "second_last_name": {"data_type": "text"},
        "email": {"data_type": "text"},
        "dni": {"data_type": "text"},
        "phone_number": {"data_type": "text"},
        "landline": {"data_type": "text"},
        "address": {"data_type": "text"},
        "gender": {"data_type": "text"},
        "code": {"data_type": "text"},
        "sap_company_name": {"data_type": "text"},
        "country_code": {"data_type": "text"},
        "country_name": {"data_type": "text"},
        "department_code": {"data_type": "text"},
        "department_name": {"data_type": "text"},
        "municipality_code": {"data_type": "text"},
        "town_code": {"data_type": "text"},
        "neighborhood_code": {"data_type": "text"},
        "class_name": {"data_type": "text"},
        "business_name": {"data_type": "text"},
        "category_client": {"data_type": "text"},
        "company_id": {"data_type": "bigint"},
        "notes": {"data_type": "text"},
        "sap_company_code": {"data_type": "text"},
        "sap_zone_code": {"data_type": "text"},
        "sap_zone_name": {"data_type": "text"},
        "_id": {"data_type": "text"},
        "birth_date": {"data_type": "date"},
        "created_at": {"data_type": "timestamp"},
        "updated_at": {"data_type": "timestamp"},
        "sap_username": {"data_type": "text"},
        "user_id": {"data_type": "bigint"},
        "sapusercode": {"data_type": "text"},
        "number": {"data_type": "text"},
        "sellers": {"data_type": "complex"},
        "seller_id": {"data_type": "text"},
        "custom_fields": {"data_type": "complex"},
        "debt_collectors": {"data_type": "complex"},
        "facebook_username": {"data_type": "text"},
        "facebook_id": {"data_type": "text"},
        "twitter_username": {"data_type": "text"},
        "twitter_id": {"data_type": "text"},
        "linkedin_username": {"data_type": "text"},
        "instagram_username": {"data_type": "text"},
        "fecha_ingreso": {"data_type": "timestamp"},
    },
    incrementals=(
        IncConfig(
            cursor_path="updated_at",
            initial_value=pendulum.now().subtract(months=1),
        ),
        IncConfig(cursor_path="created_at"),
    ),
    # limit=1000,
)

read_source_config_multi_column: ColConfigs = (
    DLTRColl(
        collection_name="crm_message",
        incrementals=(
            IncConfig(
                cursor_path="updated_at",
                initial_value=pendulum.now().subtract(months=1),
            ),
            IncConfig(cursor_path="created_at"),
        ),
    ),
    DLTRColl(
        collection_name="crm_campaign",
        # cursor_path="updatedAt",
        incrementals=(
            IncConfig(
                cursor_path="updatedAt",
                initial_value=pendulum.now().subtract(months=1),
            ),
            IncConfig(
                cursor_path="updated_at",
                initial_value=pendulum.now().subtract(months=1),
            ),
            IncConfig(cursor_path="created_at"),
        ),
    ),  # updatedAt
    DLTRColl(
        collection_name="campaignSchedule",
        incrementals=(
            IncConfig(
                cursor_path="EndDate",
                initial_value=pendulum.now().subtract(months=1),
            ),
            IncConfig(cursor_path="createdDate"),
        ),
    ),
    DLTRColl(
        collection_name="dataViewList",
        incrementals=(
            IncConfig(
                cursor_path="updatedAt",
                initial_value=pendulum.now().subtract(months=1),
            ),
            IncConfig(cursor_path="creationDate"),
        ),
    ),
    DLTRColl(
        collection_name="crmCall",
        table_new_name="crm_call",
        columns_hints={
            "clientNumber": {"data_type": "text"},
            "callerId": {"data_type": "bigint"},
            "clientId": {"data_type": "bigint", "name": "callee_id"},
        },
        incrementals=(
            IncConfig(
                cursor_path="UpdatedAt",
                initial_value=pendulum.now().subtract(months=1),
            ),
            IncConfig(cursor_path="createdAt"),
        ),
    ),
)


read_source_config_full_refresh: ColConfigs = (
    DLTRColl(collection_name="campaignActivity"),
)


# all_mongodb_source_configs: ColConfigs = (
#     *read_source_config_updated_at,
#     *read_source_config_multi_column,
#     *read_source_config_not_incremental,
# )

mongodb_crm_hn_updated_at = mongodb(
    connection_url=dlt.secrets["sources.mdb_crm_hn.connection_url"],
    database=dlt.secrets["sources.mdb_crm_hn.database"],
    collection_names=[c.collection_name for c in read_source_config_updated_at],
    write_disposition="merge",
    parallel=True,
)

mongodb_crm_hn_updated_at_assets = dlt_mongodb_asset_factory(
    dlt_source=mongodb_crm_hn_updated_at,
    dataset_name="mongo_db_crm_hn",
    collections_config=read_source_config_updated_at,
    group_name="dlt_mongo_db_crm_hn_etl_dwh",
    base_pipeline_name="mongodb_crm_hn_pipeline_updated_at",
)

mongodb_crm_hn_person = mongodb(
    connection_url=dlt.secrets["sources.mdb_crm_hn.connection_url"],
    database=dlt.secrets["sources.mdb_crm_hn.database"],
    collection_names=[collection_person.collection_name],
    write_disposition="merge",
    parallel=True,
)

mongodb_crm_hn_person_assets = dlt_mongodb_asset_factory(
    dlt_source=mongodb_crm_hn_person,
    dataset_name="mongo_db_crm_hn",
    collections_config=(collection_person,),
    group_name="dlt_mongo_db_crm_hn_etl_dwh",
    base_pipeline_name="mongodb_crm_hn_pipeline_person",
)


mongodb_crm_hn_multi = mongodb(
    connection_url=dlt.secrets["sources.mdb_crm_hn.connection_url"],
    database=dlt.secrets["sources.mdb_crm_hn.database"],
    collection_names=[c.collection_name for c in read_source_config_multi_column],
    write_disposition="merge",
    parallel=True,
)

mongodb_crm_hn_multi_assets = dlt_mongodb_asset_factory(
    dlt_source=mongodb_crm_hn_multi,
    dataset_name="mongo_db_crm_hn",
    collections_config=read_source_config_multi_column,
    group_name="dlt_mongo_db_crm_hn_etl_dwh",
    base_pipeline_name="mongodb_crm_hn_pipeline_multi",
)

mongodb_crm_hn_full_refresh = mongodb(
    connection_url=dlt.secrets["sources.mdb_crm_hn.connection_url"],
    database=dlt.secrets["sources.mdb_crm_hn.database"],
    collection_names=[c.collection_name for c in read_source_config_full_refresh],
    write_disposition="replace",
    parallel=True,
)

mongodb_crm_hn_full_refresh_assets = dlt_mongodb_asset_factory(
    dlt_source=mongodb_crm_hn_full_refresh,
    dataset_name="mongo_db_crm_hn",
    collections_config=read_source_config_full_refresh,
    group_name="dlt_mongo_db_crm_hn_etl_dwh",
    base_pipeline_name="mongodb_crm_hn_pipeline_full_refresh",
)

#Enlistar todos los assets
all_mongodb_hn_assets = (
    *mongodb_crm_hn_updated_at_assets,
    *mongodb_crm_hn_person_assets,
    *mongodb_crm_hn_multi_assets,
    *mongodb_crm_hn_full_refresh_assets,
)

all_mongodb_hn_source_assets = list(
    SourceAsset(
        key,
        group_name="mongodb_crm_hn_etl_dwh_sources",
        tags={"dagster/storage_kind": "mongodb"},
    )
    for key in set(
        chain.from_iterable(
            dlt_assets_.dependency_keys for dlt_assets_ in all_mongodb_hn_assets
        )
    )
    if key not in set(asset.key for asset in all_mongodb_hn_assets)
)


@asset_check(
    asset=AssetKey(
        ("DL_FARINTER", "mongodb_crm_hn", "campaigns_recetas", "updated_at")
    ),
    blocking=False,
)
def campaigns_recetas_check(dwh_farinter_dl: SQLServerResource) -> AssetCheckResult:
    dwh = dwh_farinter_dl
    last_date: datetime | None
    with dwh.get_sqlalchemy_conn() as conn:
        last_date_result = conn.execute(
            dwh.text(
                """SELECT TOP 1 created_at FROM mongodb_crm_hn.campaigns_recetas ORDER BY created_at DESC"""
            ),
        )
        last_date = last_date_result.scalar() if last_date_result.returns_rows else None
    last_date_str = (
        str(last_date.astimezone()) if isinstance(last_date, datetime) else "No data"
    )
    expected_date = get_for_current_env(
        {
            "dev": pendulum.now(default_timezone_teg).subtract(days=1),
            "prd": pendulum.now(default_timezone_teg).subtract(days=1)
            if pendulum.now(default_timezone_teg).hour < 5
            else pendulum.now(default_timezone_teg).replace(
                hour=4, minute=0, second=0, microsecond=0
            ),
        }
    )
    if (
        isinstance(last_date, (datetime, pendulum.DateTime))
        and last_date >= expected_date
    ):
        return AssetCheckResult(
            passed=True,
            description=f"Fecha minima esperada: {expected_date}\n Última fecha en campaigns_recetas: {last_date_str}",
        )
    else:
        return AssetCheckResult(
            passed=False,
            description=f"Fecha minima esperada: {expected_date}\n Última fecha en mongodb_crm_hn.campaigns_recetas: {last_date_str}",
        )


all_assets = all_mongodb_hn_assets

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(
        all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"
    ),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
# print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = (
    build_last_update_freshness_checks(
        assets=filter_assets_by_tags(
            all_assets,
            tags_to_match=tags_repo.Hourly.tag,
            filter_type="any_tag_matches",
        ),
        lower_bound_delta=timedelta(hours=13),
        deadline_cron="0 10-16 * * 1-6",
    )
)

all_assets = (*all_assets, *all_mongodb_hn_source_assets)
all_asset_checks: Sequence[AssetChecksDefinition] = (
    load_asset_checks_from_current_module()
)
all_asset_freshness_checks = (
    *all_assets_non_hourly_freshness_checks,
    *all_assets_hourly_freshness_checks,
)

if __name__ == "__main__":

    def test_all_assets_loaded():
        configured_total = 0
        configured_total += len(read_source_config_updated_at)
        configured_total += 1 #person
        configured_total += len(read_source_config_multi_column)
        configured_total += len(read_source_config_full_refresh)

        loaded_total = len(all_assets)
        assert (
            configured_total == loaded_total
        ), f"Expected {configured_total} assets, but loaded {loaded_total} assets"

    from dagster import build_resources, materialize

    from dagster_shared_gf.dlt_shared.dlt_resources import dlt_pipeline_dest_mssql_dwh
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl

    with instance_for_test() as instance:
        with build_resources(
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dlt_pipeline_dest_mssql_dwh": dlt_pipeline_dest_mssql_dwh,
            },
        ) as resources_init:
            asset_to_test = tuple(
                asset
                for asset in all_assets
                if asset.key
                in (
                    AssetKey(("DL_FARINTER", "mongo_db_crm_hn", "crm_campaign")),
                )
            )
            assert asset_to_test
            result = materialize(
                asset_to_test,
                instance=instance,
                resources=resources_init.original_resource_dict,
                # run_config=RunConfig(
                #     resources={
                #         "dlt_pipeline_dest_mssql_dwh": {
                #             "config": {
                #                 "write_disposition": "replace",
                #             }
                #         }
                #     }
                # ),
            )

            # check_result = campaigns_recetas_check(
            #     dwh_farinter_dl=resources_init.dwh_farinter_dl
            # )
            # print(check_result)

            print(f"Materialized:{
                [
                    mat.step_materialization_data
                    for mat in result.get_asset_materialization_events()
                ]
            }")
