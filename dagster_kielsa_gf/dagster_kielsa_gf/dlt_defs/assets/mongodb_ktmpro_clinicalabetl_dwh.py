from itertools import chain

import dlt
from dagster import (
    AssetSpec,
)
from dagster_dlt import (
    DagsterDltResource,
)
from dlt.common import pendulum

from dagster_shared_gf.automation import automation_daily_delta_2_cron
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
    get_for_current_env,
)
from dagster_shared_gf.shared_variables import tags_repo

collections_config = (
    DLTRColl(
        collection_name="logtokens",
        primary_key="_id",
        incrementals=(
            IncConfig(
                cursor_path="createdAt",
                initial_value=get_for_current_env(
                    {
                        "local": pendulum.now().subtract(days=60),
                        "dev": pendulum.now().subtract(years=4),
                    }
                ),
                lag=7,  # days
            ),
        ),
        automation_condition=automation_daily_delta_2_cron,
        columns_to_include=("_id", "active", "createdAt", "userId"),
        tags=tags_repo.AutomationDaily | tags_repo.AutomationOnly,
    ),
)

mongodb_ktmpro_clinicalab = mongodb(
    connection_url=dlt.secrets["sources.mdb_ktmpro_clinicalab.connection_url"],
    database=dlt.secrets["sources.mdb_ktmpro_clinicalab.database"],
    collection_names=[c.collection_name for c in collections_config],
    write_disposition="replace",
    parallel=False,
)

created_mongodb_assets = dlt_mongodb_asset_factory(
    dlt_source=mongodb_ktmpro_clinicalab,
    collections_config=collections_config,
    group_name="mongodb_ktmpro_clinicalab_group",
    dataset_name="mdb_ktmpro_clinicalab",
    base_pipeline_name="mongodb_ktmpro_clinicalab",
)

all_mongodb_hn_assets = [  # El test y load_asset_from modules solo funciona con listas
    *created_mongodb_assets,
]


all_mongodb_hn_source_assets = list(
    AssetSpec(
        key,
        group_name="mongodb_ktmpro_clinicalab_etl_dwh_sources",
        tags={"dagster/storage_kind": "mongodb"},
    )
    for key in set(
        chain.from_iterable(
            dlt_assets_.dependency_keys for dlt_assets_ in all_mongodb_hn_assets
        )
    )
    if key not in set(asset.key for asset in all_mongodb_hn_assets)
)

if __name__ == "__main__":
    from dagster import (
        AssetKey,
        RunConfig,
        build_resources,
        instance_for_test,
        materialize,
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
                for asset in all_mongodb_hn_assets
                if asset.key
                in (
                    AssetKey(("dlt_mongodb",)),
                    AssetKey(("DL_FARINTER", "mdb_ktmpro_clinicalab", "logtokens")),
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
                                # "write_disposition": "replace",
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
