from dagster import Definitions, load_assets_from_modules
from dagster_dlt import DagsterDltResource
from dagster_kielsa_gf.dlt_defs.assets import mongodb_crm_etl_dwh, mongodb_ecommerce_etl_dwh
from dagster_shared_gf.dlt_shared import dlt_resources

# defs = Definitions(
#     assets=mongo_db_crm_etl_dwh.all_mongo_db_hn_assets + mongo_db_crm_etl_dwh.all_mongo_db_hn_source_assets,
#     resources={
#         "dlt": DagsterDltResource(),
#         "dlt_pipeline_dest_mssql_dwh": dlt_resources.dlt_pipeline_dest_mssql_dwh
#     },
# )

all_assets = (*mongodb_crm_etl_dwh.all_assets, *mongodb_ecommerce_etl_dwh.all_assets)
all_resources={
        "dlt": DagsterDltResource(),
        "dlt_pipeline_dest_mssql_dwh": dlt_resources.dlt_pipeline_dest_mssql_dwh
    }
all_asset_checks = (*mongodb_crm_etl_dwh.all_asset_checks, *mongodb_ecommerce_etl_dwh.all_asset_checks)