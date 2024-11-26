import os
import dlt
from dagster_embedded_elt.dlt import (
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
    get_for_current_env,
)
default_schema= dlt.Schema(
    name="mdb_ecommerce_hn"
)
mdb_ecommerce_hn = mongodb
print(mdb_ecommerce_hn)
mongodb_ecommerce_hn = mdb_ecommerce_hn(
    connection_url=dlt.secrets["sources.mdb_ecommerce_hn.connection_url"],
    database=dlt.secrets["sources.mdb_ecommerce_hn.database"],
    collection_names=["orders"],
    write_disposition="replace",
    parallel=True,
    #data_item_format="arrow",
    # limit=100,
    max_table_nesting=1,
    # incremental=dlt.sources.incremental(
    #     cursor_path="createdAt",
    #     initial_value=pendulum.now().subtract(days=30),
    # ),
    # filter_={"createdAt": {"$gte": get_for_current_env({"local": pendulum.now().subtract(days=7)})}},
)


mongodb_ecommerce_hn.resources["orders"].apply_hints(
    incremental=dlt.sources.incremental(
        cursor_path="createdAt",
        initial_value=pendulum.now().subtract(days=30),
    ),
    primary_key=["_id"],
)

mongodb_ecommerce_hn.resources["orders"].max_table_nesting = 0
# mongodb_ecommerce_hn.resources["orders"].apply_hints(
#     columns={
#         "data_routes": {"nullable": True, "data_type": "json"},
#         "error": {"nullable": True, "data_type": "json"},
#         "pay_data": {"nullable": True, "data_type": "json"},
#     }
# )

pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
    "mdb_ecommerce_hn_pipeline",
    "mdb_ecommerce_hn",
    import_schema_path=os.path.join(os.path.dirname(__file__), "schemas", "import"),
    export_schema_path=os.path.join(os.path.dirname(__file__), "schemas", "export"),
)
pipeline.drop_pending_packages()
# pipeline.drop()
pipeline.schemas.clear_storage()
# with pipeline.destination_client() as client:
#     client.drop_storage()
# pipeline.extract(mongodb_ecommerce_hn, refresh="drop_sources")
# pipeline.schemas.save_import_schema_if_not_exists(schema=mongodb_ecommerce_hn.schema)
# pipeline.normalize()
# # print(pipeline.schemas["mongodb"].to_pretty_json())
# pipeline.sync_schema()
# pipeline.load(raise_on_failed_jobs=True)
pipeline.run(mongodb_ecommerce_hn)


# if __name__ == "__main__":
#     pass
#     print(all_assets)

#     from dagster import (
#         AssetKey,
#         build_resources,
#         instance_for_test,
#         materialize,
#     )

#     from dagster_shared_gf.dlt_shared.dlt_resources import dlt_pipeline_dest_mssql_dwh
#     from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl

#     with instance_for_test() as instance:
#         with build_resources(
#             instance=instance,
#             resources={
#                 "dwh_farinter_dl": dwh_farinter_dl,
#                 "dlt_pipeline_dest_mssql_dwh": dlt_pipeline_dest_mssql_dwh,
#                 "dlt": DagsterDltResource(),
#             },
#         ) as resources_init:
#             asset_to_test = tuple(
#                 asset
#                 for asset in all_assets
#                 if asset.key in (AssetKey(("dlt_mongodb_orders",)),
#                 AssetKey(("DL_FARINTER", "mdb_ecommerce_hn", "orders")),)
#             )
#             assert asset_to_test
#             result = materialize(
#                 asset_to_test,
#                 instance=instance,
#                 resources=resources_init.original_resource_dict,
#                 # run_config=RunConfig(
#                 #     resources={
#                 #         "dlt_pipeline_dest_mssql_dwh": {
#                 #             "config": {
#                 #                 # "dev_mode": True,
#                 #                 "refresh": "drop_sources",
#                 #             }
#                 #         }
#                 #     }
#                 # ),
#             )

#             # check_result = campaigns_recetas_check(
#             #     dwh_farinter_dl=resources_init.dwh_farinter_dl
#             # )
#             # print(check_result)

#             print(f"Materialized:{
#                 [
#                     mat.step_materialization_data
#                     for mat in result.get_asset_materialization_events()
#                 ]
#             }")
