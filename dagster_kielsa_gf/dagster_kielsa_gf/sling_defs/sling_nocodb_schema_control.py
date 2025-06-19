import os
import json
from pathlib import Path

from dagster import (
    op,
    Out,
    OpExecutionContext,
    Output,
    graph,
)
from dagster_shared_gf.resources.postgresql_resources import PostgreSQLResource


TIMESTAMP_FIELDS = ["fecha_creado", "fecha_actualizado", "created_at", "updated_at"]
CREATION_FIELDS = ["fecha_creado", "created_at"]
UPDATE_FIELDS = ["fecha_actualizado", "updated_at"]
CACHE_DIR = Path(".cache")


@op(
    out={"processed_tables": Out(list)},
    tags={"kind": "nocodb_maintenance"},
)
def create_timestamp_triggers(
    context: OpExecutionContext, db_nocodb_data_gf: PostgreSQLResource, schema_name: str
) -> Output:
    """
    Creates timestamp triggers for PostgreSQL tables in the specified schema.

    This op:
    1. Identifies tables that need timestamp triggers
    2. Creates functions and triggers for timestamp management
    3. Caches processed tables to avoid redundant operations
    """

    # Ensure cache directory exists
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"timestamp_triggers_{schema_name}.json"

    # Load cached tables if available
    cached_tables = set()
    if cache_file.exists():
        try:
            with open(cache_file, "r") as f:
                cached_data = json.load(f)
                cached_tables = set(cached_data.get("processed_tables", []))
                context.log.info(f"Loaded {len(cached_tables)} cached tables")
        except Exception as e:
            context.log.error(f"Error loading cache: {e}")

    # Get all tables in the schema
    query = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '{schema_name}' 
    AND table_type = 'BASE TABLE';
    """

    tables_result = db_nocodb_data_gf.query(query)
    if not tables_result:
        context.log.warning(f"No tables found in schema '{schema_name}'")
        return Output([], "processed_tables")

    tables = [row[0] for row in tables_result]
    context.log.info(f"Found {len(tables)} tables in schema '{schema_name}'")

    # Filter out already processed tables
    tables_to_process = [t for t in tables if t not in cached_tables]
    context.log.info(f"{len(tables_to_process)} tables need processing")

    processed_tables = list(cached_tables)

    # Process each table
    for table_name in tables_to_process:
        context.log.info(f"Processing table: {table_name}")
        is_processed = False

        # Check table columns
        columns_query = f"""
        SELECT column_name, data_type, column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        AND table_name = '{table_name}';
        """

        columns_result = db_nocodb_data_gf.query(columns_query)
        if not columns_result:
            context.log.warning(f"No columns found for table '{table_name}'")
            continue

        columns = {
            row[0]: {"type": row[1], "default": row[2]} for row in columns_result
        }

        # Check which timestamp fields exist
        existing_timestamp_fields = [f for f in TIMESTAMP_FIELDS if f in columns]

        if not existing_timestamp_fields:
            context.log.info(f"Table '{table_name}' has no timestamp fields to manage")
            continue

        # Set default values for creation timestamp fields
        for field in CREATION_FIELDS:
            if field in columns and not columns[field]["default"]:
                set_default_query = f"""
                ALTER TABLE "{schema_name}"."{table_name}" 
                ALTER COLUMN "{field}" SET DEFAULT CURRENT_TIMESTAMP;
                """
                context.log.info(
                    f"Setting default value for '{field}' in '{table_name}'"
                )
                db_nocodb_data_gf.execute_and_commit(set_default_query)

                set_fill_query = f"""
                UPDATE "{schema_name}"."{table_name}" 
                SET "{field}" = CURRENT_TIMESTAMP 
                WHERE "{field}" IS NULL;
                """
                context.log.info(
                    f"Filling default value for '{field}' in '{table_name}'"
                )
                db_nocodb_data_gf.execute_and_commit(set_fill_query)

        # Create or replace update trigger function if needed
        for field in UPDATE_FIELDS:
            if field in columns:
                function_name = f"update_{table_name}_{field}_trigger"
                trigger_name = f"trg_{table_name}_{field}_timestamp"

                # Check if trigger already exists
                trigger_query = f"""
                SELECT 1 FROM pg_trigger 
                WHERE tgname LIKE '%timestamp%' 
                AND tgrelid = '"{schema_name}"."{table_name}"'::regclass;
                """

                trigger_exists = db_nocodb_data_gf.query(trigger_query)

                if not trigger_exists:
                    # Create trigger function
                    function_query = f"""
                    CREATE OR REPLACE FUNCTION "{schema_name}"."{function_name}"()	
                    RETURNS trigger
                    LANGUAGE 'plpgsql'
                    AS $BODY$
                    BEGIN
                        NEW.{field} := CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $BODY$;
                    
                    ALTER FUNCTION "{schema_name}"."{function_name}"()
                    OWNER TO nocodb_admin;
                    """

                    context.log.info(
                        f"Creating trigger function for '{field}' in '{table_name}'"
                    )
                    db_nocodb_data_gf.execute_and_commit(function_query)

                    # Create trigger
                    trigger_query = f"""
                    CREATE TRIGGER "{trigger_name}"
                    BEFORE UPDATE 
                    ON "{schema_name}"."{table_name}"
                    FOR EACH ROW
                    EXECUTE FUNCTION "{schema_name}"."{function_name}"();
                    """

                    context.log.info(
                        f"Creating trigger for '{field}' in '{table_name}'"
                    )
                    db_nocodb_data_gf.execute_and_commit(trigger_query)
                else:
                    context.log.info(
                        f"Trigger for '{field}' already exists on '{table_name}'"
                    )
        is_processed = True

        processed_tables.append(table_name) if is_processed else None

    # Update cache with processed tables
    try:
        with open(cache_file, "w") as f:
            json.dump({"processed_tables": processed_tables}, f)
        context.log.info(f"Updated cache with {len(processed_tables)} processed tables")
    except Exception as e:
        context.log.error(f"Error updating cache: {e}")

    return Output(processed_tables, "processed_tables")


@graph(
    description="Graph for nocodb schema control operations",
    tags={"kind": "nocodb_schema_control"},
)
def nocodb_schema_control_graph():
    create_timestamp_triggers()


nocodb_schema_control_job = nocodb_schema_control_graph.to_job(
    name="nocodb_schema_control_job",
)

if __name__ == "__main__":
    from dagster import instance_for_test, ResourceDefinition
    from dagster_shared_gf.resources.postgresql_resources import db_nocodb_data_gf
    from datetime import datetime
    import warnings
    import os

    # Determine environment
    env_str = os.environ.get("ENV", "local")

    start_time = datetime.now()

    with instance_for_test() as instance:
        if env_str == "local" and 1 == 2:
            warnings.warn("Running in local mode with test database connection")

            # Create mock or real PostgreSQL resource based on environment
            # Mock PostgreSQL resource for testing
            db_nocodb_data_gf = ResourceDefinition.mock_resource()

        # Run the job with the appropriate resources
        result = nocodb_schema_control_job.execute_in_process(
            instance=instance,
            resources={
                "db_nocodb_data_gf": db_nocodb_data_gf,
            },
            run_config={
                "ops": {
                    create_timestamp_triggers.name: {
                        "inputs": {"schema_name": "kielsa"}
                    }
                }
            },
        )

        # Print the output of the operation
        print(
            result.output_for_node(
                create_timestamp_triggers.name, output_name="processed_tables"
            )
        )

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
