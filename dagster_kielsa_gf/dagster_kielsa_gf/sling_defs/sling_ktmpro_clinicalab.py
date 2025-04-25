pass
# sling tambien esta bugueado, pero las tablas sencillas es mas facil dlt que ya esta empezado
# se necesitaba sling para tablas complejas, pero se podria hacer la misma separacion en dlt.
# from datetime import datetime
# from pathlib import Path

# from dagster import (
#     AssetExecutionContext,
#     get_dagster_logger,
# )
# from dagster_sling import (
#     sling_assets,
# )
# from dagster_shared_gf.automation import automation_daily_delta_2_cron
# from dagster_shared_gf.shared_variables import tags_repo
# from dagster_shared_gf.sling_shared.sling_resources import (
#     MyDagsterSlingTranslator,
#     MySlingResource,
# )
# from dagster_shared_gf.load_env_run import load_env_vars, os
# import yaml


# logger = get_dagster_logger("ktmpro_clinicalab")

# if not os.environ.get("SLING_HOME_DIR") or not os.environ.get(
#     "DAGSTER_DWH_FARINTER_IP"
# ):
#     load_env_vars()


# PARENT_PATH = Path(__file__).parent
# REPLICATION_CONFIG_NAME = "sling_ktmpro_clinicalab.yaml"

# REPLICATION_CONFIG_PATH = PARENT_PATH / REPLICATION_CONFIG_NAME
# REPLICATION_CONFIG_DICT = {}
# with open(REPLICATION_CONFIG_PATH, "r") as file:
#     REPLICATION_CONFIG_DICT = yaml.safe_load(file)


# @sling_assets(
#     replication_config=REPLICATION_CONFIG_DICT,
#     dagster_sling_translator=MyDagsterSlingTranslator(
#         asset_database="DL_FARINTER",
#         schema_name="ktmpro_clinicalab",
#         tags=tags_repo.AutomationDaily,
#         automation_condition=automation_daily_delta_2_cron,
#         group_name="ktmpro_clinicalab",
#     ),
# )
# def ktmpro_clinicalab(context: AssetExecutionContext, sling: MySlingResource):
#     if sling.default_mode == "full-refresh":
#         REPLICATION_CONFIG_DICT["defaults"]["mode"] = "full-refresh"

#     yield from sling.replicate(context=context, stream=True)


# if __name__ == "__main__":
#     from dagster import instance_for_test, materialize, ResourceDefinition
#     from dagster_shared_gf.sling_shared.sling_resources import MySlingResource
#     from datetime import datetime
#     import warnings
#     import os

#     # Determine environment
#     env_str = os.environ.get("ENV", "local")

#     start_time = datetime.now()

#     with instance_for_test() as instance:
#         # Create mock or real MySlingResource based on environment
#         if env_str == "local" and 1==0:
#             warnings.warn("Running in local mode with test database connection")
#             sling_resource = ResourceDefinition.mock_resource()
#         else:
#             sling_resource = MySlingResource(default_mode="incremental") #default_mode="full-refresh") #

#         # Run the asset with the appropriate resources
#         result = materialize(
#             assets=[ktmpro_clinicalab],
#             instance=instance,
#             resources={
#                 "sling": sling_resource,
#             },
#         )

#         # Print the output of the operation
#         print(result.all_events)

#     end_time = datetime.now()
#     print(
#         f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
#     )
