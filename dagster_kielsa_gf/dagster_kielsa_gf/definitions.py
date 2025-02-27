from dagster import (
    Definitions,
    build_sensor_for_freshness_checks,
)

import dagster_kielsa_gf.dlt_defs.definitions as dlt_defs
import dagster_kielsa_gf.gobernor.jobs_gobernor as gobernor_defs
from dagster_kielsa_gf import job_control_replicas, jobs
from dagster_kielsa_gf.assets import (
    analysis_services,
    cliente_general,
    dbt_dwh_kielsa,
    dbt_example,
    dbt_sources,
    examples,
    knime_asset_factory,
    ldcom_etl_dwh,
    ldcom_etl_dwh_sp,
    recetas_libros_etl_dwh,
    recomendaciones,
    smb_etl_dwh,
)
from dagster_kielsa_gf.schedules import all_schedules
from dagster_kielsa_gf.sensors import all_sensors
from dagster_shared_gf import (
    all_shared_resources,
    all_shared_sensors,
)
from dagster_shared_gf.shared_constants import (
    hourly_freshness_seconds_per_environ,
    running_default_sensor_status,
)
from dagster_shared_gf.shared_helpers import (
    create_freshness_checks_for_assets,
    get_unique_source_assets,
)

all_assets = (
    *examples.all_assets,
    *dbt_example.all_assets,
    *dbt_dwh_kielsa.all_assets,
    *ldcom_etl_dwh_sp.all_assets,
    *knime_asset_factory.all_assets,
    *recetas_libros_etl_dwh.all_assets,
    *analysis_services.all_assets,
    *ldcom_etl_dwh.all_assets,
    *smb_etl_dwh.all_assets,
    *cliente_general.all_assets,
    *recomendaciones.all_assets,
)
all_asset_checks = (
    *dbt_example.all_asset_checks,
    *dbt_dwh_kielsa.all_asset_checks,
    *ldcom_etl_dwh_sp.all_asset_checks,
    *knime_asset_factory.all_asset_checks,
    *recetas_libros_etl_dwh.all_asset_checks,
    *analysis_services.all_asset_checks,
    *ldcom_etl_dwh.all_asset_checks,
    *smb_etl_dwh.all_asset_checks,
    *recomendaciones.all_asset_checks,
)

dbt_sources_assets: list = get_unique_source_assets(
    all_assets, dbt_sources.source_assets
)

all_resources = all_shared_resources

all_asset_freshness_checks = create_freshness_checks_for_assets(all_assets)

all_assets_freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_asset_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=hourly_freshness_seconds_per_environ,  # 1 hour
    name="all_assets_freshness_checks_sensor",
)

defs = Definitions.merge(
    # dlt_defs.defs, #antes todos los subrepos
    Definitions(
        assets=(
            *all_assets,
            *dbt_sources_assets,
            *dlt_defs.all_assets,
        ),
        asset_checks=(*all_asset_checks, *dlt_defs.all_asset_checks),
        resources=all_resources | dlt_defs.all_resources,
        jobs=(*jobs.all_jobs, *job_control_replicas.all_jobs),
        schedules=all_schedules,
        sensors=(
            *all_sensors,
            *all_shared_sensors,
            all_assets_freshness_checks_sensor,
        ),
    ),
    gobernor_defs.defs,  # De ultimo ya que puede gobernar los demas subrepos
)

# @repository
# def dagster_kielsa_gf_repo():
#     return defs
