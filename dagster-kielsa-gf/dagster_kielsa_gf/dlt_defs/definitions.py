from dagster_dlt import DagsterDltResource
from dagster_kielsa_gf.dlt_defs import assets
from dagster_shared_gf.dlt_shared import dlt_resources
from dagster import load_definitions_from_package_module

all_resources = {
    "dlt": DagsterDltResource(),
    "dlt_pipeline_dest_mssql_dwh": dlt_resources.dlt_pipeline_dest_mssql_dwh,
}

defs = load_definitions_from_package_module(assets, resources=all_resources)
