from dagster_shared_gf.config import get_dagster_config
import json
from pathlib import Path

import dagster as dg
from dagster_shared_gf.resources.postgresql_resources import PostgreSQLResource


TIMESTAMP_FIELDS = ["fecha_creado", "fecha_actualizado", "created_at", "updated_at"]
CREATION_FIELDS = ["fecha_creado", "created_at"]
UPDATE_FIELDS = ["fecha_actualizado", "updated_at"]
DEFAULT_TIMESTAMP_FIELDS = ["fecha_creado", "fecha_actualizado"]
CACHE_DIR = Path(".cache")


def create_timestamp_triggers(
    context: dg.OpExecutionContext,
    db_nocodb_data_gf: PostgreSQLResource,
    schema_name: str,
    create_timestamp_if_not_exists: bool = False,
) -> dg.Output:
    """
    Crea triggers de timestamp para las tablas PostgreSQL en el esquema especificado.

    Este op:
    1. Identifica tablas que requieren triggers de timestamp
    2. Crea funciones y triggers para el manejo de timestamps
    3. Cachea las tablas procesadas para evitar operaciones redundantes
    """

    # Asegura que el directorio de cache exista
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"timestamp_triggers_{schema_name}.json"

    # Carga las tablas cacheadas si existen
    cached_tables = set()
    if cache_file.exists():
        try:
            with open(cache_file, "r") as f:
                cached_data = json.load(f)
                cached_tables = set(cached_data.get("processed_tables", []))
                context.log.info(f"Loaded {len(cached_tables)} cached tables")
        except Exception as e:
            context.log.error(f"Error loading cache: {e}")

    # Obtiene todas las tablas del esquema
    query = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '{schema_name}' 
    AND table_type = 'BASE TABLE';
    """

    tables_result = db_nocodb_data_gf.query(query)
    if not tables_result:
        context.log.warning(f"No tables found in schema '{schema_name}'")
        return dg.Output([], "processed_tables")

    tables = [row[0] for row in tables_result]
    context.log.info(f"Found {len(tables)} tables in schema '{schema_name}'")

    # Filtra las tablas ya procesadas
    tables_to_process = [t for t in tables if t not in cached_tables]
    context.log.info(f"{len(tables_to_process)} tables need processing")

    processed_tables = list(cached_tables)

    # Procesa cada tabla
    for table_name in tables_to_process:
        context.log.info(f"Processing table: {table_name}")
        is_processed = False

        # Consulta las columnas de la tabla
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

        # Verifica qué campos de timestamp existen
        existing_timestamp_fields = [f for f in TIMESTAMP_FIELDS if f in columns]

        if not existing_timestamp_fields and not create_timestamp_if_not_exists:
            context.log.info(f"Table '{table_name}' has no timestamp fields to manage")
            continue
        elif create_timestamp_if_not_exists:
            context.log.info(
                f"Creating timestamp fields for table '{table_name}' as requested"
            )
            # Agrega los campos de timestamp faltantes
            for field in DEFAULT_TIMESTAMP_FIELDS:
                if field not in columns:
                    add_column_query = f"""
                    ALTER TABLE "{schema_name}"."{table_name}" 
                    ADD COLUMN "{field}" TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    """
                    context.log.info(f"Adding column '{field}' to '{table_name}'")
                    db_nocodb_data_gf.execute_and_commit(add_column_query)
            # Refresca las columnas después de agregar
            columns_query_refresh = f"""
            SELECT column_name, data_type, column_default
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
            AND table_name = '{table_name}';
            """
            columns_result_refresh = db_nocodb_data_gf.query(columns_query_refresh)
            if columns_result_refresh:
                columns = {
                    row[0]: {"type": row[1], "default": row[2]}
                    for row in columns_result_refresh
                }
            else:
                columns = {}
            existing_timestamp_fields = [f for f in TIMESTAMP_FIELDS if f in columns]

        # Asigna valores por defecto a los campos de timestamp
        for field in TIMESTAMP_FIELDS:
            if field in columns:
                # Si no tiene default, lo asigna
                if not columns[field]["default"]:
                    set_default_query = f"""
                    ALTER TABLE "{schema_name}"."{table_name}" 
                    ALTER COLUMN "{field}" SET DEFAULT CURRENT_TIMESTAMP;
                    """
                    context.log.info(
                        f"Setting default value for '{field}' in '{table_name}'"
                    )
                    db_nocodb_data_gf.execute_and_commit(set_default_query)
                # Siempre rellena los nulos
                set_fill_query = f"""
                UPDATE "{schema_name}"."{table_name}" 
                SET "{field}" = CURRENT_TIMESTAMP 
                WHERE "{field}" IS NULL;
                """
                context.log.info(
                    f"Filling default value for '{field}' in '{table_name}'"
                )
                db_nocodb_data_gf.execute_and_commit(set_fill_query)

        # Crea o reemplaza el trigger de actualización si el campo existe (siempre intenta crear el trigger si no existe)
        for field in UPDATE_FIELDS:
            if field in columns:
                function_name = f"update_{table_name}_{field}_trigger"[:63]
                trigger_name = f"trg_{table_name}_{field}_timestamp"[:63]

                # Check if trigger already exists
                trigger_query = f"""
                SELECT 1 FROM pg_trigger 
                WHERE (tgname= '{trigger_name}' OR tgname LIKE '%timestamp%' OR tgname LIKE '%{trigger_name}%')
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
                    trigger_query_create = f"""
                    CREATE TRIGGER "{trigger_name}"
                    BEFORE UPDATE 
                    ON "{schema_name}"."{table_name}"
                    FOR EACH ROW
                    EXECUTE FUNCTION "{schema_name}"."{function_name}"();
                    """

                    context.log.info(
                        f"Creating trigger for '{field}' in '{table_name}'"
                    )
                    db_nocodb_data_gf.execute_and_commit(trigger_query_create)
                else:
                    context.log.info(
                        f"Trigger for '{field}' already exists on '{table_name}'"
                    )
        is_processed = True

        processed_tables.append(table_name) if is_processed else None

    # Actualiza el cache con las tablas procesadas
    try:
        with open(cache_file, "w") as f:
            json.dump({"processed_tables": processed_tables}, f)
        context.log.info(f"Updated cache with {len(processed_tables)} processed tables")
    except Exception as e:
        context.log.error(f"Error updating cache: {e}")

    return dg.Output(processed_tables, "processed_tables")


if __name__ == "__main__":
    from dagster_shared_gf.resources.postgresql_resources import db_nocodb_data_gf
    from datetime import datetime
    import warnings

    # Determine environment
    cfg = get_dagster_config()
    env_str = cfg.instance_current_env or "local"

    start_time = datetime.now()

    with dg.instance_for_test() as instance:
        if env_str == "local" and 1 == 2:
            warnings.warn("Running in local mode with test database connection")

            # Create mock or real PostgreSQL resource based on environment
            # Mock PostgreSQL resource for testing
            db_nocodb_data_gf = dg.ResourceDefinition.mock_resource()

        # Run the job with the appropriate resources
        result = create_timestamp_triggers(
            context=dg.build_op_context(),
            db_nocodb_data_gf=db_nocodb_data_gf,  # type: ignore
            schema_name="kielsa",
        )

        # Print the output of the operation
        print(result.value)

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )
