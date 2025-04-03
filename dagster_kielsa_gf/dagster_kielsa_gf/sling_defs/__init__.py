from dagster import load_definitions_from_modules
from dagster_shared_gf.sling_shared.sling_resources import MySlingResource
from dagster_kielsa_gf.sling_defs import sling_nocodb_data_gf

defs = load_definitions_from_modules(
    [sling_nocodb_data_gf], resources={"sling": MySlingResource()}
)
