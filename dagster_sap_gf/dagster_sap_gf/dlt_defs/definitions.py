from dagster_dlt import DagsterDltResource
from dagster_shared_gf.dlt_shared import dlt_resources
from dagster_sap_gf.dlt_defs import hontrack_assets as ha

# defs = Definitions(
#     assets=mongo_db_crm_etl_dwh.all_mongo_db_hn_assets + mongo_db_crm_etl_dwh.all_mongo_db_hn_source_assets,
#     resources={
#         "dlt": DagsterDltResource(),
#         "dlt_pipeline_dest_mssql_dwh": dlt_resources.dlt_pipeline_dest_mssql_dwh
#     },
# )

all_assets = (*ha.all_assets,)
all_resources = {
    "dlt": DagsterDltResource(),
    "dlt_pipeline_dest_mssql_dwh": dlt_resources.dlt_pipeline_dest_mssql_dwh,
}
all_asset_checks = ha.all_asset_checks
