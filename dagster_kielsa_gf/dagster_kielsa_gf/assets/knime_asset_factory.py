from dagster import asset, op, In, Out, graph, OpExecutionContext, build_op_context, AssetsDefinition, AssetExecutionContext, Failure, EnvVar, AssetKey
from dagster_shared_gf.resources.postgresql_resources import PostgreSQLResource
from dagster_shared_gf.shared_functions import get_for_current_env, search_for_word_in_text
from dagster_shared_gf.shared_variables import env_str
from dagster_shared_gf.load_env_run import load_env_vars
import subprocess
from typing import List, Dict
import re

def filter_logs_std(logs):
    exclude_patterns = [
        re.compile(r".*ERROR StatusLogger Log4j2 could not find a logging implementation.*", re.IGNORECASE),
        re.compile(r".*Execution failed in Try-Catch block.*", re.IGNORECASE),
        re.compile(r"WARN.*Errors overwriting node settings with flow variables.*", re.IGNORECASE),
        re.compile(r".*Node.*No new variables defined.*", re.IGNORECASE),
        re.compile(r".*Node.*The node configuration changed.*", re.IGNORECASE),
        re.compile(r"WARN.*Component does not have input data.*", re.IGNORECASE),
        re.compile(r"WARN.*No grouping column included.*", re.IGNORECASE),
        re.compile(r"WARN.*Errors loading flow variables into node.*", re.IGNORECASE),
        re.compile(r"WARN.*No such variable.*", re.IGNORECASE),
        re.compile(r"WARN.*The table structures of active ports are not compatible.*", re.IGNORECASE),
        re.compile(r"WARN.*No aggregation column defined.*", re.IGNORECASE),
        re.compile(r"WARN.* The input table has fewer rows.*than the specified k.*", re.IGNORECASE),
        re.compile(r"WARN.*Node All.*partition.*", re.IGNORECASE),
        re.compile(r"WARN.*Node Multiple inputs are active.*", re.IGNORECASE)
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
def fetch_knime_workflows(context: OpExecutionContext) -> List[Dict[str, str]]:
    query =f"""
    SELECT knime_bin, ambiente, knime_workflow, cron_text, workflow_directory 
    FROM knime.programacion_ejecucion WHERE activo = true AND ambiente = '{get_for_current_env({"dev":"dev", "prd":"prd"})}';"""
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
            }
            for row in results
        ]





def execute_knime_workflow(knime_bin: str, workflow_directory: str, current_context: AssetExecutionContext) -> None:
    if not current_context:
        current_context = AssetExecutionContext.get()
    
    supported_envs = ["dev", "prd"]
    if env_str in supported_envs:
        command = [
            "sudo", "-S", "-u", "analitica@farinter.net",
            knime_bin, "-reset", "-nosave", "-nosplash", "-consoleLog",
            "--launcher.suppressErrors",
            "-application", "org.knime.product.KNIME_BATCH_APPLICATION",
            "-workflowDir=" + workflow_directory
        ]
        
        if not EnvVar("DAGSTER_SECRET_ANALITICA_SU_PASSWORD").get_value():
            load_env_vars()
        
        password = EnvVar("DAGSTER_SECRET_ANALITICA_SU_PASSWORD").get_value() + '\n'
        
        try:
            current_context.log.info(f"Executing in {env_str} environment command: {command}")
            result = subprocess.check_output(command, input=password.encode('utf-8'), stderr=subprocess.STDOUT)
            current_context.log.info(filter_logs_std(result.decode('utf-8')))
        except subprocess.CalledProcessError as e:
            filtered_logs:str = filter_logs_std(e.output.decode('utf-8'))
            #check if it really contains error on the message
            if search_for_word_in_text(text=filtered_logs, word="ERROR"):
                current_context.log.error(f"Workflow execution failed: {filtered_logs}")
                e.__traceback__ = None
                raise Failure("Workflow execution failed.")
            else:
                current_context.log.warn(f"Workflow execution aparently succeeded even with this response: {filtered_logs}")

    else:
        current_context.log.info(f"Skipping workflow execution in {env_str} environment. Supported only in {supported_envs} environments.")
    return
    
# Function to create a knime workflow asset
def create_knime_workflow_asset(knime_bin: str, ambiente: str, knime_workflow: str, workflow_directory: str) -> AssetsDefinition:
    @asset(
        key=AssetKey(["knime_wf", ambiente, knime_workflow]),
        description=f"Executes the {knime_workflow} workflow in {ambiente} target environment, dir: {workflow_directory}",
        group_name="knime_workflows"
    )
    def knime_workflow_asset(context: OpExecutionContext) -> None:
        execute_knime_workflow(knime_bin=knime_bin, workflow_directory=workflow_directory, current_context=context)
        context.log.info(f"Executed {knime_workflow} in {ambiente} target environment")

    return knime_workflow_asset

# Dynamically create assets based on the fetched workflows
@op(ins={"workflows": In(List[Dict[str, str]])}, out=Out(List[AssetsDefinition]))
def create_knime_assets(context: OpExecutionContext, workflows) -> List[AssetsDefinition]:
    asset_definitions = []
    #print("Starting create_knime_assets")
    if len(workflows[0]) > 0:	
        for workflow in workflows:
            knime_bin = workflow["knime_bin"]
            ambiente = workflow["ambiente"].lower()
            knime_workflow = workflow["knime_workflow"]
            workflow_directory = workflow["workflow_directory"]

            if ambiente not in get_for_current_env({"dev":"dev", "prd":"prd"}):
                continue

            #If not used a funtion, the variables retain the latest value for all assets (referenced instead of copied.)
            asset_definitions.append(create_knime_workflow_asset(knime_bin=knime_bin
                                                                , ambiente=ambiente
                                                                , knime_workflow=knime_workflow
                                                                , workflow_directory=workflow_directory))
            #print(workflow_asset)
        context.log.info(f"Created {len(asset_definitions)} knime assets.")
        return asset_definitions
    else:
        context.log.error("No workflows found in the database.")
        return []

@graph()
def knime_asset_creation_graph():    # first = first_op()     second = second_op(start=first)     third = third_op(start_first=first, start_second=second)
    fetched_workflows = fetch_knime_workflows()
    return create_knime_assets(workflows=fetched_workflows)


##define place holder asset to use if not found f"knime_wf_{env_str.upper()}_DWHFP_SalidaExportarAExcel"
@asset(#name=f"knime_wf_{env_str.upper()}_DWHFP_SalidaExportarAExcel"
        key=AssetKey(["knime_wf",env_str,"DWHFP_SalidaExportarAExcel"]),
        group_name="knime_workflows",
        description="Placeholder asset to use if not found in the target environment.")
def knime_wf_DWHFP_SalidaExportarAExcel() -> None:
    return None


asset_definitions: list = []
knime_assets_definitions: list = []
# The main script to configure and run the job
if __name__ == "__main__":
    current_test = 2
    match current_test:
        case 1:
            from dagster_shared_gf.load_env_run import load_env_vars
            load_env_vars(joinpath_str=["..",".."])
            from dagster_shared_gf.resources.postgresql_resources import db_analitica_etl
            # Build the context with the resources
            #builded_context = build_op_context(resources={"db_analitica_etl":db_analitica_etl})
            builded_resources = {"db_analitica_etl":db_analitica_etl}
            # Fetch workflows and create assets
            asset_definitions = knime_asset_creation_graph.to_job().execute_in_process(resources=builded_resources).output_value()
            print([asset.key for asset in asset_definitions])
            assert len(asset_definitions) > 0, "No assets were created for the knnime workflows."
        case 2:
            logs = """
            INFO This is a normal log line.
            ERROR StatusLogger Log4j2 could not find a logging implementation.
            Execution failed in Try-Catch block.
            WARN Errors overwriting node settings with flow variables.
            Node\t No new variables defined.
            Node\t The node configuration changed.
            WARN Component does not have input data.
            WARN No grouping column included.
            WARN Errors loading flow variables into node.
            WARN No such variable.
            WARN The table structures of active ports are not compatible.
            WARN No aggregation column defined.
            WARN The input table has fewer rows 10 than the specified k.
            WARN Node All partition issues.
            WARN Node Multiple inputs are active.
            DEBUG A debug message.
            INFO Another info message.
            """
            expected_logs = """INFO This is a normal log line.\nDEBUG A debug message.\nINFO Another info message."""
            filtered_logs = filter_logs_std(logs)
            assert filtered_logs == expected_logs, f"Expected:\n{expected_logs}\nGot:\n{filtered_logs}"       
else:
    from dagster_shared_gf.resources.postgresql_resources import db_analitica_etl
    builded_resources = {"db_analitica_etl":db_analitica_etl}
    # Fetch workflows and create assets
    asset_definitions = knime_asset_creation_graph.to_job().execute_in_process(resources=builded_resources).output_value()
    # Check for placeholders
    if knime_wf_DWHFP_SalidaExportarAExcel.key not in [asset.key for asset in asset_definitions]:
        asset_definitions.append(knime_wf_DWHFP_SalidaExportarAExcel)
    knime_assets_definitions = asset_definitions


"""
#Add privileges to analitica for 
GRANT CONNECT ON DATABASE analitica TO dagster_instance_role;

GRANT USAGE ON SCHEMA knime TO dagster_instance_role;
GRANT SELECT ON ALL TABLES IN SCHEMA knime TO dagster_instance_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA knime GRANT SELECT ON TABLES TO dagster_instance_role;
"""

if __name__ == "__main__":
    test_string='this is a erROr string'
    if "ERROR" in test_string:
        print("it contains error simple")
    else:
        print("it does not contain error simple")

    if search_for_word_in_text(test_string,"ERROR"):
        print("it contains error")
    else:
        print("it does not contain error")
    #tests for search_for_word_in_text
    assert search_for_word_in_text(test_string,"ERROR") is not None
    assert search_for_word_in_text(test_string,"esrror") is None