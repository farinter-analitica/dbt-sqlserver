import re
import subprocess
from typing import Optional, Sequence, NamedTuple
from collections import deque

from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    AssetsDefinition,
    AutomationCondition,
    EnvVar,
    Failure,
    asset,
    load_asset_checks_from_current_module,
    get_dagster_logger,
    DagsterType,
    multi_asset,
)
from logging import Logger

from dagster_shared_gf.automation import (
    automation_hourly_delta_12_cron,
    automation_monthly_start_delta_1_cron,
)
from dagster_shared_gf.load_env_run import load_env_vars
from dagster_shared_gf.resources.postgresql_resources import (
    PostgreSQLResource,
    db_independent_analitica_etl as db_analitica_etl,
)
from dagster_shared_gf.shared_functions import (
    get_for_current_env,
    search_for_word_in_text,
)
from dagster_shared_gf.shared_variables import tags_repo, env_str, Tags


"""
#Add privileges to analitica for 
GRANT CONNECT ON DATABASE analitica TO dagster_instance_role;

GRANT USAGE ON SCHEMA knime TO dagster_instance_role;
GRANT SELECT ON ALL TABLES IN SCHEMA knime TO dagster_instance_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA knime GRANT SELECT ON TABLES TO dagster_instance_role;
"""


class Workflow(NamedTuple):
    knime_bin: str
    ambiente: str
    knime_workflow: str
    workflow_directory: str
    tags: Optional[dict]
    automation_condition: Optional[AutomationCondition]
    asset_keys: Optional[list[str]]


WorkflowsTuple = DagsterType(
    name="WorkflowsTuple",
    type_check_fn=lambda _, value: isinstance(value, tuple)
    and all(isinstance(x, Workflow) for x in value),
    typing_type=tuple[Workflow, ...],
)

AssetsTuple = DagsterType(
    name="AssetsTuple",
    type_check_fn=lambda _, value: isinstance(value, tuple)
    and all(isinstance(x, AssetsDefinition) for x in value),
    typing_type=tuple[AssetsDefinition, ...],
)


def filter_logs_std(logs):
    exclude_patterns = [
        re.compile(
            r".*ERROR StatusLogger Log4j2 could not find a logging implementation.*",
            re.IGNORECASE,
        ),
        re.compile(r".*Execution failed in Try-Catch block.*", re.IGNORECASE),
        re.compile(
            r"WARN.*Errors overwriting node settings with flow variables.*",
            re.IGNORECASE,
        ),
        re.compile(r".*Node.*No new variables defined.*", re.IGNORECASE),
        re.compile(r".*Node.*The node configuration changed.*", re.IGNORECASE),
        re.compile(r"WARN.*Component does not have input data.*", re.IGNORECASE),
        re.compile(r"WARN.*No grouping column included.*", re.IGNORECASE),
        re.compile(r"WARN.*Errors loading flow variables into node.*", re.IGNORECASE),
        re.compile(r"WARN.*No such variable.*", re.IGNORECASE),
        re.compile(
            r"WARN.*The table structures of active ports are not compatible.*",
            re.IGNORECASE,
        ),
        re.compile(r"WARN.*No aggregation column defined.*", re.IGNORECASE),
        re.compile(
            r"WARN.* The input table has fewer rows.*than the specified k.*",
            re.IGNORECASE,
        ),
        re.compile(r"WARN.*Node All.*partition.*", re.IGNORECASE),
        re.compile(r"WARN.*Node Multiple inputs are active.*", re.IGNORECASE),
        re.compile(r"ERROR.*elf_dynamic_array_reader.h\(64\).*", re.IGNORECASE),
        re.compile(r"ERROR.*file_io_posix.cc\(144\).*", re.IGNORECASE),
        re.compile(r"WARN.*Node No connection available..*", re.IGNORECASE),
        re.compile(r"WARN.*Node created an empty data table.*", re.IGNORECASE),
    ]

    filtered_lines = []
    for line in logs.split("\n"):
        line = line.strip()
        line = re.sub(r"\s+", " ", line)

        if not line:
            continue

        if not any(pattern.search(line) for pattern in exclude_patterns):
            filtered_lines.append(" ".join(line.split()))
    return "\n".join(filtered_lines)


# ##define place holder asset to use if not found f"knime_wf_{env_str.upper()}_DWHFP_SalidaExportarAExcel"
# @asset(  # name=f"knime_wf_{env_str.upper()}_DWHFP_SalidaExportarAExcel"
#     key=AssetKey(["knime_wf", env_str, "DWHFP_SalidaExportarAExcel"]),
#     group_name="knime_workflows",
#     description="Placeholder asset to use if not found in the target environment.",
# )
# def knime_wf_DWHFP_SalidaExportarAExcel() -> None:
#     return None


# Operation to fetch workflows from the database
def fetch_knime_workflows(
    dagster_logger: Logger, db_analitica_etl: PostgreSQLResource
) -> tuple[Workflow, ...]:
    query = f"""
    SELECT knime_bin, ambiente, knime_workflow, cron_text, workflow_directory, asset_keys 
    FROM knime.programacion_ejecucion WHERE activo = true AND ambiente = '{get_for_current_env({"dev": "DEV", "prd": "PRD"})}';"""
    results = db_analitica_etl.query(query)

    tags: dict[str, Tags | dict] = {
        "MDBCRM_ETL_LlamadasConsolidado": {
            **tags_repo.Hourly.tag,
            **tags_repo.Daily.tag,
            **tags_repo.Automation.tag,
        },
        "DWHFP_SalidaExportarAExcel": tags_repo.Monthly | tags_repo.AutomationOnly,
        "Knime_Workflows_Placeholder": tags_repo.Monthly,  # Para prevenir error en el job
    }
    automation_conditions = {
        "MDBCRM_ETL_LlamadasConsolidado": automation_hourly_delta_12_cron,
        "DWHFP_SalidaExportarAExcel": automation_monthly_start_delta_1_cron,
    }
    wf_ph = Workflow(
        knime_bin="",
        ambiente=get_for_current_env({"dev": "DEV", "prd": "PRD"}),
        knime_workflow="Knime_Workflows_Placeholder",
        # cron_text="",
        workflow_directory="",
        tags=tags.get("Knime_Workflows_Placeholder", None),
        automation_condition=None,
        asset_keys=None,
    )  # Para prevenir error en el job

    if not results:
        dagster_logger.error("No workflows found in the database.")
        return tuple(wf_ph for _ in range(1))
    else:
        dagster_logger.info(f"Found {len(results)} workflows in the database.")
        wf_tuple = tuple(
            Workflow(
                knime_bin=row[0],
                ambiente=row[1].lower(),
                knime_workflow=row[2],
                # cron_text=row[3],
                workflow_directory=row[4],
                tags=tags.get(row[2], tags_repo.Daily.tag),
                automation_condition=automation_conditions.get(row[2], None),
                asset_keys=row[5],
            )
            for row in results
        )
        wf_tuple += (wf_ph,)
        return wf_tuple


def execute_knime_workflow(
    knime_bin: str, workflow_directory: str, context: AssetExecutionContext
) -> None:
    command = [
        "sudo",
        "-S",
        "-u",
        "analitica@farinter.net",
        knime_bin,
        "-reset",
        "-nosave",
        "-nosplash",
        "-consoleLog",
        "--launcher.suppressErrors",
        "-application",
        "org.knime.product.KNIME_BATCH_APPLICATION",
        "-workflowDir=" + workflow_directory,
    ]

    password = EnvVar("DAGSTER_SECRET_ANALITICA_SU_PASSWORD").get_value()
    if not password:
        load_env_vars()

    if password:
        password = password + "\n"
    else:
        exception = Exception(
            "DAGSTER_SECRET_ANALITICA_SU_PASSWORD environment variable not found."
        )
        exception.__traceback__ = None
        raise exception

    try:
        context.log.info(f"Executing in {env_str} environment command: {command}")
        result = subprocess.check_output(
            command, input=password.encode("utf-8"), stderr=subprocess.STDOUT
        )
        context.log.info(filter_logs_std(result.decode("utf-8")))
    except subprocess.CalledProcessError as e:
        filtered_logs: str = filter_logs_std(e.output.decode("utf-8"))
        # check if it really contains error on the message
        if search_for_word_in_text(text=filtered_logs, word="ERROR"):
            context.log.error(f"Workflow execution failed: {filtered_logs}")
            e.__traceback__ = None
            raise Failure("Workflow execution failed.")
        else:
            context.log.warning(
                f"Workflow execution aparently succeeded even with this response: {filtered_logs}"
            )

    return


# Function to create a knime workflow asset
def create_knime_workflow_asset(
    wf: Workflow,
) -> AssetsDefinition:
    if wf.asset_keys is None or len(wf.asset_keys) == 1:

        @asset(
            key=AssetKey(
                [
                    "DL_FARINTER",
                    "dbo",
                    wf.asset_keys[0] if wf.asset_keys else wf.knime_workflow,
                ]
            ),
            description=f"Executes the {wf.knime_workflow} workflow in {wf.ambiente} target environment, dir: {wf.workflow_directory}",
            group_name="knime_workflows",
            automation_condition=wf.automation_condition,
            tags=wf.tags,
            kinds={"knime", "sql"},
        )
        def knime_workflow_asset(context: AssetExecutionContext) -> None:
            supported_envs = ("dev", "prd")
            if (
                wf.ambiente.lower() in supported_envs
                and wf.knime_workflow != "Knime_Workflows_Placeholder"
            ):
                execute_knime_workflow(
                    knime_bin=wf.knime_bin,
                    workflow_directory=wf.workflow_directory,
                    context=context,
                )
                context.log.info(
                    f"Executed {wf.knime_workflow} in {wf.ambiente} target environment"
                )
            else:
                context.log.warning(
                    f"Skipping workflow execution in {env_str} environment. Supported only in {supported_envs} environments."
                )
    elif wf.asset_keys is not None and len(wf.asset_keys) > 1:

        @multi_asset(
            specs=tuple(
                (
                    AssetSpec(
                        key=AssetKey(("DL_FARINTER", "dbo", asset_key)),
                        tags=wf.tags,  # check automation condition on load_assets_from_current_module
                        automation_condition=wf.automation_condition,
                        kinds={"knime", "sql"},
                        description=f"Executes the {wf.knime_workflow} workflow in {wf.ambiente} target environment, dir: {wf.workflow_directory}",
                        group_name="knime_workflows",
                    )
                    for asset_key in wf.asset_keys
                )
            ),
            description=f"Executes the {wf.knime_workflow} workflow in {wf.ambiente} target environment, dir: {wf.workflow_directory}",
            group_name="knime_workflows",
        )
        def knime_workflow_asset(context: AssetExecutionContext):
            supported_envs = ("dev", "prd")
            if (
                wf.ambiente.lower() in supported_envs
                and wf.knime_workflow != "Knime_Workflows_Placeholder"
            ):
                execute_knime_workflow(
                    knime_bin=wf.knime_bin,
                    workflow_directory=wf.workflow_directory,
                    context=context,
                )
                context.log.info(
                    f"Executed {wf.knime_workflow} in {wf.ambiente} target environment"
                )
            else:
                context.log.warning(
                    f"Skipping workflow execution in {env_str} environment. Supported only in {supported_envs} environments."
                )
            return tuple(None for _ in wf.asset_keys)  # type: ignore

    return knime_workflow_asset


# Dynamically create assets based on the fetched workflows
def create_knime_assets(
    dagster_logger: Logger, workflows
) -> tuple[AssetsDefinition, ...]:
    asset_definitions = deque()
    # print("Starting create_knime_assets")
    if len(workflows) > 0:
        for workflow in workflows:
            if workflow.ambiente.lower() not in get_for_current_env(
                {"dev": "dev", "prd": "prd"}
            ):
                continue

            workflow_asset = create_knime_workflow_asset(wf=workflow)
            asset_definitions.append(workflow_asset)
        dagster_logger.info(f"Created {len(asset_definitions)} knime assets.")
        return tuple(asset_definitions)
    else:
        dagster_logger.error("No workflows found in the database.")
        return tuple()


def knime_asset_creation_graph(
    dagster_logger, db_analitica_etl
) -> tuple[AssetsDefinition, ...]:
    fetched_workflows = fetch_knime_workflows(
        dagster_logger=dagster_logger, db_analitica_etl=db_analitica_etl
    )
    return create_knime_assets(
        dagster_logger=dagster_logger, workflows=fetched_workflows
    )


all_assets: tuple[AssetsDefinition, ...]
# Build the context with the resourcesc
resources = {"db_analitica_etl": db_analitica_etl}
# context=build_op_context(resources=resources)
dagster_logger_instance = get_dagster_logger(name="Independent")
all_assets = knime_asset_creation_graph(
    dagster_logger=dagster_logger_instance, db_analitica_etl=db_analitica_etl
)

# Check for placeholders
# if knime_wf_DWHFP_SalidaExportarAExcel.key not in [asset.key for asset in all_assets]:
#     all_assets.append(knime_wf_DWHFP_SalidaExportarAExcel)

all_asset_checks: Sequence[AssetChecksDefinition] = (
    load_asset_checks_from_current_module()
)
if __name__ == "__main__":
    for asset_def in all_assets:
        print(asset_def, asset_def.tags_by_key)
