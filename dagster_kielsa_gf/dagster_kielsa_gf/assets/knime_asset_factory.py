import re
import subprocess
from datetime import timedelta
from typing import Dict, List, Optional, Sequence

from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AutomationCondition,
    EnvVar,
    Failure,
    In,
    OpExecutionContext,
    Out,
    asset,
    build_last_update_freshness_checks,
    graph,
    load_asset_checks_from_current_module,
    op,
)

from dagster_shared_gf.automation import automation_hourly_cron_prd
from dagster_shared_gf.load_env_run import load_env_vars
from dagster_shared_gf.resources.postgresql_resources import db_analitica_etl
from dagster_shared_gf.shared_functions import (
    filter_assets_by_tags,
    get_for_current_env,
    search_for_word_in_text,
)
from dagster_shared_gf.shared_variables import TagsRepositoryGF, env_str

tags_repo = TagsRepositoryGF

"""
#Add privileges to analitica for 
GRANT CONNECT ON DATABASE analitica TO dagster_instance_role;

GRANT USAGE ON SCHEMA knime TO dagster_instance_role;
GRANT SELECT ON ALL TABLES IN SCHEMA knime TO dagster_instance_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA knime GRANT SELECT ON TABLES TO dagster_instance_role;
"""


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


# Operation to fetch workflows from the database
@op(required_resource_keys={"db_analitica_etl"})
def fetch_knime_workflows(context: OpExecutionContext) -> List[Dict[str, str | None]]:
    query = f"""
    SELECT knime_bin, ambiente, knime_workflow, cron_text, workflow_directory 
    FROM knime.programacion_ejecucion WHERE activo = true AND ambiente = '{get_for_current_env({"dev":"DEV", "prd":"PRD"})}';"""
    results = context.resources.db_analitica_etl.query(query)
    if not results:
        context.log.error("No workflows found in the database.")
        return [{}]
    else:
        context.log.info(f"Found {len(results)} workflows in the database.")
        return [
            {
                "knime_bin": row[0],
                "ambiente": row[1],
                "knime_workflow": row[2],
                "cron_text": row[3],
                "workflow_directory": row[4],
                "automation_condition": automation_hourly_cron_prd
                if row[2] in ("MDBCRM_ETL_LlamadasConsolidado",)
                else None,
            }
            for row in results
        ]


def execute_knime_workflow(
    knime_bin: str, workflow_directory: str, current_context: AssetExecutionContext
) -> None:
    if not current_context:
        current_context = AssetExecutionContext.get()

    supported_envs = ["dev", "prd"]
    if current_context.asset_key.path[1] in supported_envs:
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

        if not EnvVar("DAGSTER_SECRET_ANALITICA_SU_PASSWORD").get_value():
            load_env_vars()

        password = EnvVar("DAGSTER_SECRET_ANALITICA_SU_PASSWORD").get_value() + "\n"

        try:
            current_context.log.info(
                f"Executing in {env_str} environment command: {command}"
            )
            result = subprocess.check_output(
                command, input=password.encode("utf-8"), stderr=subprocess.STDOUT
            )
            current_context.log.info(filter_logs_std(result.decode("utf-8")))
        except subprocess.CalledProcessError as e:
            filtered_logs: str = filter_logs_std(e.output.decode("utf-8"))
            # check if it really contains error on the message
            if search_for_word_in_text(text=filtered_logs, word="ERROR"):
                current_context.log.error(f"Workflow execution failed: {filtered_logs}")
                e.__traceback__ = None
                raise Failure("Workflow execution failed.")
            else:
                current_context.log.warn(
                    f"Workflow execution aparently succeeded even with this response: {filtered_logs}"
                )

    else:
        current_context.log.info(
            f"Skipping workflow execution in {env_str} environment. Supported only in {supported_envs} environments."
        )
    return


# Function to create a knime workflow asset
def create_knime_workflow_asset(
    knime_bin: str,
    ambiente: str,
    knime_workflow: str,
    workflow_directory: str,
    automation_condition: Optional[AutomationCondition],
) -> AssetsDefinition:
    @asset(
        key=AssetKey(["knime_wf", ambiente, knime_workflow]),
        description=f"Executes the {knime_workflow} workflow in {ambiente} target environment, dir: {workflow_directory}",
        group_name="knime_workflows",
        automation_condition=automation_condition,
    )
    def knime_workflow_asset(context: OpExecutionContext) -> None:
        execute_knime_workflow(
            knime_bin=knime_bin,
            workflow_directory=workflow_directory,
            current_context=context,
        )
        context.log.info(f"Executed {knime_workflow} in {ambiente} target environment")

    return knime_workflow_asset


# Dynamically create assets based on the fetched workflows
@op(ins={"workflows": In(List[Dict[str, str | None]])}, out=Out(List[AssetsDefinition]))
def create_knime_assets(
    context: OpExecutionContext, workflows
) -> List[AssetsDefinition]:
    asset_definitions = set()
    # print("Starting create_knime_assets")
    if len(workflows[0]) > 0:
        for workflow in workflows:
            if workflow["ambiente"].lower() not in get_for_current_env({"dev": "dev", "prd": "prd"}):
                continue

            workflow_asset = create_knime_workflow_asset(
                knime_bin=workflow["knime_bin"],
                ambiente=workflow["ambiente"].lower(),
                knime_workflow=workflow["knime_workflow"],
                workflow_directory=workflow["workflow_directory"],
                automation_condition=workflow["automation_condition"],
            )
            asset_definitions.add(workflow_asset)
            # print(workflow_asset) #BI_CRM_Hecho_Llamadas
        context.log.info(f"Created {len(asset_definitions)} knime assets.")
        return list(asset_definitions)
    else:
        context.log.error("No workflows found in the database.")
        return list()


@graph()
def knime_asset_creation_graph():  # first = first_op()     second = second_op(start=first)     third = third_op(start_first=first, start_second=second)
    fetched_workflows = fetch_knime_workflows()
    return create_knime_assets(workflows=fetched_workflows)


##define place holder asset to use if not found f"knime_wf_{env_str.upper()}_DWHFP_SalidaExportarAExcel"
@asset(  # name=f"knime_wf_{env_str.upper()}_DWHFP_SalidaExportarAExcel"
    key=AssetKey(["knime_wf", env_str, "DWHFP_SalidaExportarAExcel"]),
    group_name="knime_workflows",
    description="Placeholder asset to use if not found in the target environment.",
)
def knime_wf_DWHFP_SalidaExportarAExcel() -> None:
    return None



# Build the context with the resources
# builded_context = build_op_context(resources={"db_analitica_etl":db_analitica_etl})
builded_resources = {"db_analitica_etl": db_analitica_etl}

all_assets = (
    knime_asset_creation_graph.to_job()
    .execute_in_process(resources=builded_resources)
    .output_value()
)

# Check for placeholders
if knime_wf_DWHFP_SalidaExportarAExcel.key not in [asset.key for asset in all_assets]:
    all_assets.append(knime_wf_DWHFP_SalidaExportarAExcel)

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(
        all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"
    ),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
# print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = (
    build_last_update_freshness_checks(
        assets=filter_assets_by_tags(
            all_assets,
            tags_to_match=tags_repo.Hourly.tag,
            filter_type="any_tag_matches",
        ),
        lower_bound_delta=timedelta(hours=13),
        deadline_cron="0 10-16 * * 1-6",
    )
)

all_asset_checks: Sequence[AssetChecksDefinition] = (
    load_asset_checks_from_current_module()
)
all_asset_freshness_checks = (
    all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
)
