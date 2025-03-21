from dagster import (
    AssetSelection,
    Definitions,
    FilesystemIOManager,
    InMemoryIOManager,
    build_sensor_for_freshness_checks,
    DefaultSensorStatus,
)
from dagster import (
    AutomationConditionSensorDefinition as ACS,
)
from dagster_polars import PolarsParquetIOManager

from dagster_shared_gf import assets as assets_repo
from dagster_shared_gf.assets import dbt_sources
from dagster_shared_gf.jobs import all_jobs
from dagster_shared_gf.resources import (
    correo_e,
    dbt_resources,
    postgresql_resources,
    smb_resources,
    sql_server_resources,
)
from dagster_shared_gf.schedules import all_schedules
from dagster_shared_gf.shared_constants import (
    running_default_sensor_status,
    hourly_freshness_seconds_per_environ,
)
from dagster_shared_gf.shared_helpers import (
    get_unique_source_assets,
    create_freshness_checks_for_assets,
)
from dagster_shared_gf.shared_variables import tags_repo, Tags

all_assets = assets_repo.all_assets
dbt_sources_assets: list = get_unique_source_assets(
    assets_repo.all_assets, dbt_sources.source_assets
)

all_shared_resources = {
    "dwh_farinter": sql_server_resources.dwh_farinter,
    "dwh_farinter_adm": sql_server_resources.dwh_farinter_adm,
    "dwh_farinter_dl": sql_server_resources.dwh_farinter_dl,
    "dwh_farinter_dl_prd": sql_server_resources.dwh_farinter_dl_prd,
    "dwh_farinter_bi": sql_server_resources.dwh_farinter_bi,
    "dwh_farinter_prd_replicas_ldcom": sql_server_resources.dwh_farinter_prd_replicas_ldcom,
    "ldcom_hn_prd_sqlserver": sql_server_resources.ldcom_hn_prd_sqlserver,
    "ldcom_ni_prd_sqlserver": sql_server_resources.ldcom_ni_prd_sqlserver,
    "ldcom_cr_prd_sqlserver": sql_server_resources.ldcom_cr_prd_sqlserver,
    "ldcom_cr_arb_prd_sqlserver": sql_server_resources.ldcom_cr_arb_prd_sqlserver,
    "ldcom_gt_prd_sqlserver": sql_server_resources.ldcom_gt_prd_sqlserver,
    "ldcom_sv_prd_sqlserver": sql_server_resources.ldcom_sv_prd_sqlserver,
    "siteplus_sqlldsubs_sqlserver": sql_server_resources.siteplus_sqlldsubs_sqlserver,
    #
    "dbt_resource": dbt_resources.dbt_resource,
    #
    "db_analitica_etl": postgresql_resources.db_analitica_etl,
    #
    "smb_resource_analitica_nasgftgu02": smb_resources.smb_resource_analitica_nasgftgu02,
    #
    "smb_resource_staging_dagster_dwh": smb_resources.smb_resource_staging_dagster_dwh,
    #
    "enviador_correo_e_analitica_farinter": correo_e.enviador_correo_e_analitica_farinter,
    #
    "in_memory_io_manager": InMemoryIOManager(),
    "filesystem_io_manager": FilesystemIOManager(),
    "polars_parquet_io_manager": PolarsParquetIOManager(),
}


class ACSSensorFactory:
    def __init__(self, default_status=running_default_sensor_status):
        self.default_status = default_status
        self.custom_sensors = []
        self.base_selections = {}
        self.custom_selections = {}

        # Initialize standard selections
        self._initialize_base_selections()

    def _initialize_base_selections(self):
        """Initialize base selections without exclusions"""
        # Define base selections
        self.base_selections["hourly"] = AssetSelection.tag(
            key=tags_repo.Hourly.key, value=tags_repo.Hourly.value
        )

        self.base_selections["daily"] = AssetSelection.tag(
            key=tags_repo.Daily.key, value=tags_repo.Daily.value
        )

        self.base_selections["weekly"] = (
            AssetSelection.tag(key=tags_repo.Weekly.key, value=tags_repo.Weekly.value)
            | AssetSelection.tag(
                key=tags_repo.Weekly1.key, value=tags_repo.Weekly1.value
            )
            | AssetSelection.tag(
                key=tags_repo.Weekly7.key, value=tags_repo.Weekly7.value
            )
        )

        self.base_selections["monthly"] = (
            AssetSelection.tag(key=tags_repo.Monthly.key, value=tags_repo.Monthly.value)
            | AssetSelection.tag(
                key=tags_repo.MonthlyStart.key, value=tags_repo.MonthlyStart.value
            )
            | AssetSelection.tag(
                key=tags_repo.MonthlyEnd.key, value=tags_repo.MonthlyEnd.value
            )
        )

    def _get_final_selections(self):
        """
        Calculate final selections with proper exclusions, accounting for custom selections
        """
        # Start with a copy of base selections
        final_selections = {k: v for k, v in self.base_selections.items()}

        # Add custom selections
        for name, selection in self.custom_selections.items():
            final_selections[name] = selection

        # Apply exclusions in priority order
        priority_order = ["hourly", "daily", "weekly", "monthly"] + list(
            self.custom_selections.keys()
        )

        # First, exclude custom selections from standard selections
        for std_name in ["hourly", "daily", "weekly", "monthly"]:
            for custom_name, custom_selection in self.custom_selections.items():
                final_selections[std_name] = (
                    final_selections[std_name] - custom_selection
                )

        # Then apply the hierarchical exclusions
        for i, higher_priority in enumerate(priority_order):
            for lower_priority in priority_order[i + 1 :]:
                if (
                    lower_priority in final_selections
                    and higher_priority in final_selections
                ):
                    final_selections[lower_priority] = (
                        final_selections[lower_priority]
                        - final_selections[higher_priority]
                    )

        # Add the "remaining" selection that excludes everything else
        all_other_selections = AssetSelection.all()
        for name, selection in final_selections.items():
            all_other_selections = all_other_selections - selection

        final_selections["remaining"] = all_other_selections

        return final_selections

    def add_sensor(
        self,
        name: str,
        selection: AssetSelection,
        interval_seconds: int = 300,
        tags: dict[str, str] | Tags | None = None,
        run_tags: dict[str, str] | Tags | None = None,
        status: DefaultSensorStatus | None = None,
    ):
        """
        Add a custom sensor with specific parameters

        Args:
            name: Name of the sensor
            selection: The AssetSelection object
            interval_seconds: Minimum interval between sensor evaluations
            tags: Tags to apply to the sensor
            run_tags: Tags to apply to runs triggered by the sensor
            status: Default status of the sensor
        """
        # Store the selection
        self.custom_selections[name] = selection

        # Create and store the sensor (will be built with proper exclusions when get_sensors is called)
        sensor_config = {
            "name": name,
            "selection_name": name,
            "interval_seconds": interval_seconds,
            "tags": tags or {},
            "run_tags": run_tags or {},
            "status": status or self.default_status,
        }

        self.custom_sensors.append(sensor_config)
        return self

    def get_sensors(self):
        """
        Build and return all sensors with up-to-date selections
        """
        # Calculate final selections with proper exclusions
        final_selections = self._get_final_selections()

        # Build all sensors
        all_sensors = []

        # Build default sensors
        all_sensors.append(
            ACS(
                "automation_condition_sensor",
                target=final_selections["daily"],
                use_user_code_server=True,
                minimum_interval_seconds=60 * 5,
                tags=tags_repo.Daily,
                run_tags=tags_repo.Daily,
                default_status=self.default_status,
            )
        )

        all_sensors.append(
            ACS(
                "automation_condition_sensor_slow",
                target=final_selections["monthly"]
                | final_selections["weekly"]
                | final_selections["remaining"],
                use_user_code_server=True,
                minimum_interval_seconds=60 * 60,
                tags=tags_repo.Monthly | tags_repo.Weekly,
                run_tags=tags_repo.Monthly | tags_repo.Weekly,
                default_status=self.default_status,
            )
        )

        all_sensors.append(
            ACS(
                "automation_condition_sensor_hourly",
                target=final_selections["hourly"],
                use_user_code_server=True,
                minimum_interval_seconds=60,
                tags=tags_repo.Hourly,
                run_tags=tags_repo.Hourly,
                default_status=self.default_status,
            )
        )

        # Build custom sensors
        for sensor_config in self.custom_sensors:
            all_sensors.append(
                ACS(
                    sensor_config["name"],
                    target=final_selections[sensor_config["selection_name"]],
                    use_user_code_server=True,
                    minimum_interval_seconds=sensor_config["interval_seconds"],
                    tags=sensor_config["tags"],
                    run_tags=sensor_config["run_tags"],
                    default_status=sensor_config["status"],
                )
            )

        return all_sensors


all_freshness_checks = create_freshness_checks_for_assets(all_assets)

all_shared_assets_freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=hourly_freshness_seconds_per_environ,
    name="all_shared_assets_freshness_checks_sensor",
)

defs = Definitions(
    assets=(*assets_repo.all_assets, *dbt_sources_assets),
    jobs=(*all_jobs,),
    schedules=(*all_schedules,),
    asset_checks=(*assets_repo.all_asset_checks, *all_freshness_checks),
    sensors=(
        *ACSSensorFactory().get_sensors(),
        all_shared_assets_freshness_checks_sensor,
    ),
    resources=all_shared_resources,
)
