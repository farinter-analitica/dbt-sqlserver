from dagster import Definitions, load_assets_from_modules
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_kielsa_gf.dlt.assets import mongo_db_crm_etl_dwh
from dagster_shared_gf.dlt_shared import dlt_resources

# defs = Definitions(
#     assets=mongo_db_crm_etl_dwh.all_mongo_db_hn_assets + mongo_db_crm_etl_dwh.all_mongo_db_hn_source_assets,
#     resources={
#         "dlt": DagsterDltResource(),
#         "dlt_pipeline_dest_mssql": dlt_resources.dlt_pipeline_dest_mssql
#     },
# )

all_assets = mongo_db_crm_etl_dwh.all_assets
all_resources={
        "dlt": DagsterDltResource(),
        "dlt_pipeline_dest_mssql": dlt_resources.dlt_pipeline_dest_mssql
    }
all_asset_checks = mongo_db_crm_etl_dwh.all_asset_checks