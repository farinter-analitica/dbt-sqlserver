from dagster import asset, op, In, Out, graph, OpExecutionContext, build_op_context, AssetsDefinition, AssetExecutionContext
from dagster_shared_gf.resources.postgresql_resources import PostgreSQLResource
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.shared_variables import env_str
import subprocess
from typing import List, Dict


# Operation to fetch workflows from the database
@op(required_resource_keys={"db_analitica_etl"})
def fetch_knime_workflows(context: OpExecutionContext) -> List[Dict[str, str]]:
    query = "SELECT knime_bin, ambiente, knime_workflow, cron_text, workflow_directory FROM knime.programacion_ejecucion WHERE activo = true"
    results = context.resources.db_analitica_etl.query(query)
    if not results:
        context.log.error("No workflows found in the database.")
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

def execute_knime_workflow(knime_bin: str, workflow_directory: str, current_context: AssetExecutionContext | None) -> None:
    if not current_context:
        current_context = AssetExecutionContext.get()
    command = [
        knime_bin, "-reset", "-nosave", "-nosplash", "-consoleLog",
        "--launcher.suppressErrors",
        "-application", "org.knime.product.KNIME_BATCH_APPLICATION",
        "-workflowDir=" + workflow_directory
    ]
    supported_envs = ["dev", "prd"]
    if env_str in supported_envs:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            #raise Exception(f"Workflow execution failed: {stderr.decode('utf-8')}").
            current_context.log.error(f"Workflow execution failed: {stderr.decode('utf-8')}")

        #return stdout.decode('utf-8')
        current_context.log.info(stdout.decode('utf-8'))
    else:
        current_context.log.info(f"Skipping workflow execution in {env_str} environment. Supported only in {supported_envs} environments.")
    

# Dynamically create assets based on the fetched workflows
@op(ins={"workflows": In(List[Dict[str, str]])}, out=Out(List[AssetsDefinition]))
def create_knime_assets(context: OpExecutionContext, workflows) -> List[AssetsDefinition]:
    asset_definitions = []
    #print("Starting create_knime_assets")
    for workflow in workflows:
        knime_bin = workflow["knime_bin"]
        ambiente = workflow["ambiente"]
        knime_workflow = workflow["knime_workflow"]
        workflow_directory = workflow["workflow_directory"]

        if ambiente.lower() not in get_for_current_env({"dev":"dev", "prd":"prd"}):
            continue

        @asset(
            name=f"knime_wf_{ambiente}_{knime_workflow}",
            description=f"Executes the {knime_workflow} workflow in {ambiente} target environment",
            group_name="knime_workflows",
        )
        def knime_workflow_asset(context: AssetExecutionContext) -> None:
            execute_knime_workflow(knime_bin=knime_bin, workflow_directory=workflow_directory, current_context=context)
            #return Output(output, metadata={"workflow": knime_workflow, "environment": ambiente})
            context.log.info(f"Executed {knime_workflow} in {ambiente} target environment")

        asset_definitions.append(knime_workflow_asset)
        #print(workflow_asset)
    context.log.info(f"Created {len(asset_definitions)} knime assets.")
    return asset_definitions

@graph()
def knime_asset_creation_graph():    # first = first_op()     second = second_op(start=first)     third = third_op(start_first=first, start_second=second)
    fetched_workflows = fetch_knime_workflows()
    return create_knime_assets(workflows=fetched_workflows)


asset_definitions: list = []
# The main script to configure and run the job
if __name__ == "__main__":
    from dagster_shared_gf.load_env_run import load_env_vars
    load_env_vars(joinpath_str=["..",".."])
    from dagster_shared_gf.resources.postgresql_resources import db_analitica_etl
    # Build the context with the resources
    #builded_context = build_op_context(resources={"db_analitica_etl":db_analitica_etl})
    builded_resources = {"db_analitica_etl":db_analitica_etl}
    # Fetch workflows and create assets
    asset_definitions = knime_asset_creation_graph.to_job().execute_in_process(resources=builded_resources).output_value()
    print(asset_definitions)
else:
    from dagster_shared_gf.resources.postgresql_resources import db_analitica_etl
    builded_resources = {"db_analitica_etl":db_analitica_etl}
    # Fetch workflows and create assets
    asset_definitions = knime_asset_creation_graph.to_job().execute_in_process(resources=builded_resources).output_value()



"""
#Add privileges to analitica for 
GRANT CONNECT ON DATABASE analitica TO dagster_instance_role;

GRANT USAGE ON SCHEMA knime TO dagster_instance_role;
GRANT SELECT ON ALL TABLES IN SCHEMA knime TO dagster_instance_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA knime GRANT SELECT ON TABLES TO dagster_instance_role;
"""