from collections.abc import Sequence
from datetime import timedelta
from itertools import chain
from decimal import Decimal
import os

import dlt
from dagster import (
    AssetChecksDefinition,
    SourceAsset,
    build_last_update_freshness_checks,
    load_asset_checks_from_current_module,
)
from dagster_dlt import (
    DagsterDltResource,
)
from dlt.common import pendulum

from dagster_shared_gf.dlt_shared.dlt_resources import (
    dlt_pipeline_dest_mssql_dwh,
)
from dagster_shared_gf.dlt_shared.mongodb import mongodb
from dagster_shared_gf.dlt_shared.mongodb.custom_dagster_helpers import (
    DltIncrementalPartialConfig as IncConfig,
)
from dagster_shared_gf.dlt_shared.mongodb.custom_dagster_helpers import (
    DltResourceCollectionConfig as DLTRColl,
)
from dagster_shared_gf.dlt_shared.mongodb.custom_dagster_helpers import (
    dlt_mongodb_asset_factory,
)
from dagster_shared_gf.shared_functions import (
    filter_assets_by_tags,
    get_for_current_env,
)
from dagster_shared_gf.shared_variables import tags_repo
from dagster_shared_gf.automation import automation_daily_delta_2_cron
import dlt.normalize
import dlt.normalize.items_normalizers

# type mapping
type_mongodb_python_mapping = {
    "String": str,
    "Int32": int,
    "Double": Decimal,
    "Boolean": bool,
    "Date": pendulum.DateTime,
    "Document": dict,
    "Array": list,
    "Null": None,
    "Undefined": None,
}

# orders
orders_forced_schema = {
    "_id": {"String": 1},
    "address": {"Document": 0.997, "Null": 0.003},
    "amount": {"Double": 0.954, "Int32": 0.046},
    "appVersion": {"Double": 0.258, "Undefined": 0.742},
    "billNumberDGI": {"String": 0.691, "Undefined": 0.309},
    "billNumberLD": {"String": 0.691, "Undefined": 0.309},
    "billReference": {"String": 0.691, "Undefined": 0.309},
    "country": {"String": 0.691, "Undefined": 0.309},
    "createdAt": {"Date": 1},
    # "dataRoutes": {"Document": 0.896, "Array": 0.104},
    "deliveryManName": {"String": 0.691, "Undefined": 0.309},
    "deliveryManNumber": {"String": 0.691, "Undefined": 0.309},
    # "error": {"Document": 0.187, "String": 0.061, "Undefined": 0.752},
    "homeDelivery": {"Int32": 0.992, "Undefined": 0.008},
    "isCanje": {"Boolean": 0.093, "Undefined": 0.907},
    "isRecharge": {"Boolean": 0.258, "Undefined": 0.742},
    # "modificatedAt": {"String": 0.002, "Undefined": 0.998},
    "modificatedDateAt": {"Date": 0.002, "Undefined": 0.998},
    "orderForTomorrow": {"Boolean": 1},
    "orderNumber": {"Int32": 0.691, "String": 0.309},
    "origin": {"String": 1},
    "packagingOrder": {"Boolean": 0.806, "Undefined": 0.194},
    # "payData": {"Document": 0.752, "Null": 0.004, "Undefined": 0.244},
    "productPackage": {"Array": 0.875, "Null": 0.125},
    "resumeOrder": {"Document": 1},
    # "routeDate": {"String": 0.691, "Undefined": 0.309},
    "rtn": {"Document": 0.806, "Undefined": 0.194},
    "rtnActive": {"Boolean": 0.806, "Undefined": 0.194},
    "selectedAddress": {"String": 0.806, "Undefined": 0.194},
    "selectedCreditCard": {"String": 0.747, "Undefined": 0.253},
    "shoppingCart": {"Array": 1},
    "status": {"String": 1},
    "tax": {"Int32": 0.476, "Double": 0.215, "Undefined": 0.309},
    "total": {"Double": 0.651, "Int32": 0.04, "Undefined": 0.309},
    "typeOrder": {"String": 1},
    "typePayCard": {"String": 0.809, "Boolean": 0.183, "Undefined": 0.008},
    "typePayment": {"String": 1},
    "userId": {"String": 1},
    "userName": {"String": 1},
    "version": {"Double": 0.008, "Undefined": 0.992},
}

for key, value in orders_forced_schema.items():
    filtered_value = {k: v for k, v in value.items() if k != "Undefined"}
    max_value = max(filtered_value.values())
    max_type = [k for k, v in filtered_value.items() if v == max_value][0]
    orders_forced_schema[key] = type_mongodb_python_mapping[max_type]

collections_config = (
    DLTRColl(
        collection_name="orders",
        primary_key="_id",
        incrementals=(
            IncConfig(
                cursor_path="createdAt",
                initial_value=get_for_current_env(
                    {
                        "local": pendulum.now().subtract(days=60),
                        "dev": pendulum.now().subtract(years=2),
                    }
                ),
                lag=15,  # days
            ),
            # IncConfig(
            #     cursor_path="modificatedDateAt",
            #     initial_value=get_for_current_env(
            #         {
            #             "local": pendulum.now().subtract(days=30),
            #             "dev": pendulum.now().subtract(days=30),
            #         }
            #     ),
            #     lag=15,  # days
            # ),
        ),
        automation_condition=automation_daily_delta_2_cron,
        tags=tags_repo.Daily | tags_repo.AutomationOnly | tags_repo.UniquePeriod,
        # columns_hints={
        #     "dataRoutes" : {"nullable" : True, "data_type": "json"},
        #     "address" : {"nullable" : True},
        # },
        max_table_nesting=4,
        # force_columns_type={
        #     "dataRoutes": dict,
        # },
        # force_columns_type=orders_forced_schema, # usamos el esquema en la carpeta de esquemas para controlar esto mejor.
        columns_to_include=tuple(key for key in orders_forced_schema),
        export_schema_path=os.path.join(
            os.path.dirname(__file__), "mongodb_ecommerce_schemas", "orders", "export"
        ),
        schema_contract={"columns": "evolve", "data_type": "evolve"},
    ),
)

mongodb_ecommerce_hn = mongodb(
    connection_url=dlt.secrets["sources.mdb_ecommerce_hn.connection_url"],
    database=dlt.secrets["sources.mdb_ecommerce_hn.database"],
    collection_names=[c.collection_name for c in collections_config],
    write_disposition="replace",
    parallel=False,
)

created_mongodb_assets = dlt_mongodb_asset_factory(
    dlt_source=mongodb_ecommerce_hn,
    collections_config=collections_config,
    group_name="mongodb_ecommerce_hn_group",
    dataset_name="mdb_ecommerce_hn",
    base_pipeline_name="mongodb_ecommerce_hn",
)

all_mongodb_hn_assets = (*created_mongodb_assets,)


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
    print(all_assets)

    from dagster import (
        AssetKey,
        build_resources,
        instance_for_test,
        materialize,
        RunConfig,
    )

    from dagster_shared_gf.dlt_shared.dlt_resources import dlt_pipeline_dest_mssql_dwh
    from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl

    with instance_for_test() as instance:
        with build_resources(
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dlt_pipeline_dest_mssql_dwh": dlt_pipeline_dest_mssql_dwh,
                "dlt": DagsterDltResource(),
            },
        ) as resources_init:
            asset_to_test = tuple(
                asset
                for asset in all_assets
                if asset.key
                in (
                    AssetKey(("dlt_mongodb_orders",)),
                    AssetKey(("DL_FARINTER", "mdb_ecommerce_hn", "orders")),
                )
            )
            assert asset_to_test
            result = materialize(
                asset_to_test,
                instance=instance,
                resources=resources_init.original_resource_dict,
                run_config=RunConfig(
                    resources={
                        "dlt_pipeline_dest_mssql_dwh": {
                            "config": {
                                # "dev_mode": True,
                                "write_disposition": "replace",
                                # "refresh": "drop_resources",
                                "drop_pending_packages": True,
                            }
                        }
                    }
                ),
            )

            # check_result = campaigns_recetas_check(
            #     dwh_farinter_dl=resources_init.dwh_farinter_dl
            # )
            # print(check_result)

            print(
                f"Materialized:{
                    [
                        mat.step_materialization_data
                        for mat in result.get_asset_materialization_events()
                    ]
                }"
            )
