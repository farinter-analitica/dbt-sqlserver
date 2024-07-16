from dagster import Definitions, load_assets_from_modules
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_kielsa_gf.dlt.assets import mongo_db_crm_etl_dwh

defs = Definitions(
    assets=mongo_db_crm_etl_dwh.all_mongo_db_hn_assets + mongo_db_crm_etl_dwh.all_mongo_db_hn_source_assets,
    resources={
        "dlt": DagsterDltResource(),
    },
)
