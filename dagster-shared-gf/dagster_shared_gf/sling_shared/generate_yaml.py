#!/usr/bin/env python3
"""
Script para generar un archivo YAML con formato Sling a partir de un esquema PostgreSQL.
Inspecciona el esquema utilizando SQLAlchemy y produce un YAML con la siguiente
información por tabla:
  - columnas (nombre y tipo SQL Server convertido)
  - claves primarias
  - una declaración de creación de tabla SQL Server (table_ddl)
  - una declaración post_sql para crear la restricción de clave primaria

El YAML también contiene configuraciones globales predeterminadas y configuración de origen/destino según se especifique.
Ver:
  - https://docs.slingdata.io/concepts/replication/structure
  - https://docs.slingdata.io/concepts/replication/target-options

Uso:
  Proporcione un motor SQLAlchemy (o conexión) a la función `generate_sling_yaml`.
  Por ejemplo, ejecute el script directamente después de ajustar su cadena de conexión.
"""

from datetime import datetime
import os
import sys
import yaml
import inspect
from typing import Dict, List, Optional, Any, Union
import sqlalchemy as sql
from pathlib import Path
from textwrap import dedent
from dagster_shared_gf.shared_functions import normalize_table_identifier


def get_caller_directory() -> str:
    """
    Obtiene el directorio del script que llamó a esta función.
    Retorna el directorio de trabajo actual si no puede determinar el llamador.

    Returns:
        str: Ruta al directorio del script que realizó la llamada
    """
    frame = None
    try:
        # Obtener el frame del llamador
        frame = inspect.currentframe()
        if frame is None:
            return os.getcwd()

        caller_frame = frame.f_back
        if caller_frame is None:
            return os.getcwd()

        # Si se llama directamente desde la línea de comandos
        if (
            caller_frame.f_code.co_filename == "<stdin>"
            or caller_frame.f_code.co_filename == "<string>"
        ):
            return os.getcwd()

        # Obtener el nombre de archivo del llamador y su directorio
        caller_file = caller_frame.f_code.co_filename
        caller_dir = os.path.dirname(os.path.abspath(caller_file))
        return caller_dir
    except (AttributeError, ValueError):
        # Fallback al directorio de trabajo actual
        return os.getcwd()
    finally:
        # Limpiar referencias para evitar problemas de referencia circular
        if frame is not None:
            del frame


def is_file_cache_valid(
    filename: str,
    seconds_threshold: float = 3600,
    directory: Optional[str | Path] = None,
) -> bool:
    """
    Verifica si un archivo existe y si es más antiguo que un umbral de horas especificado.
    Compatible con múltiples plataformas.

    Args:
        filename: Nombre del archivo a verificar
        seconds_threshold: Umbral de horas para considerar un archivo como antiguo
        directory: Directorio donde buscar el archivo. Si es None, usa el directorio del llamador.

    Returns:
        bool: True si el archivo existe y no es más antiguo que seconds_threshold, False en caso contrario
    """
    if directory is None:
        directory = get_caller_directory()

    # Construir la ruta completa del archivo usando Path para compatibilidad multiplataforma
    file_path = Path(directory) / filename

    # Verificar si el archivo existe
    exists = file_path.exists()

    # Si el archivo no existe, no puede ser antiguo
    if not exists:
        return False

    # Obtener la hora actual
    now = datetime.now()

    # Obtener la hora de modificación del archivo
    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)

    # Calcular la diferencia en horas
    time_diff = now - mtime
    seconds_diff = time_diff.total_seconds()

    # Determinar si el archivo es más antiguo que el umbral
    is_old = seconds_diff > seconds_threshold

    return exists and not is_old


def generate_sling_yaml_from_source(
    engine: Union[sql.Engine, sql.Connection],
    source_schemas: Union[str, List[str]],
    output_filename: str,
    source: str,
    target: str,
    output_dir: Optional[Path | str] = None,
    defaults: Optional[Dict[str, Any]] = None,
    update_keys: List[str] = ["fecha_actualizado", "updated_at"],
    target_schema: str = "sling_data",
) -> str:
    """
    Inspecciona uno o más esquemas PostgreSQL (usando reflexión de SQLAlchemy)
    y genera un archivo YAML con formato Sling que incluye streams de todos ellos.

    Args:
        engine: Instancia de SQLAlchemy Engine o Connection.
        source_schemas: Un esquema o una lista de esquemas PostgreSQL a reflejar.
        output_filename: Nombre del archivo YAML a escribir.
        output_dir: Directorio donde guardar el archivo YAML. Si es None, usa el directorio del llamador.
        source: Nombre del origen de datos.
        target: Nombre del destino de datos.
        defaults: Configuración predeterminada. Si es None, usa la configuración por defecto.
        update_keys: Lista de nombres de columna candidatos para update_key, por orden de prioridad.

    Returns:
        str: Ruta al archivo YAML generado.

    Se omiten tablas sin claves primarias y sin ninguna columna de update_key.
    """
    # Asegurar que source_schemas sea una lista
    if isinstance(source_schemas, str):
        schemas = [source_schemas]
    else:
        schemas = source_schemas

    if defaults is None:
        defaults = {
            "mode": "incremental",
            "object": f"{target_schema}.{{stream_schema}}_{{stream_table}}",
            "target_options": {"column_casing": "snake", "adjust_column_type": True},
            "source_options": {"flatten": True},
        }

    if output_dir is None:
        # Si no se especifica un directorio, usar el directorio del llamador
        output_dir = get_caller_directory()

    # Asegurar que el directorio de salida exista
    os.makedirs(output_dir, exist_ok=True)

    # Ruta completa al archivo de salida
    output_path = os.path.join(output_dir, output_filename)

    streams: Dict[str, Dict[str, Any]] = {}
    manual_streams: Dict[str, Dict[str, Any]] = {}
    for schema in schemas:
        metadata = sql.MetaData()
        # Reflejar tablas del esquema especificado
        metadata.reflect(bind=engine, schema=schema)

        # Iterar sobre cada tabla reflejada
        for table_name, table in metadata.tables.items():
            # Extraer solo el nombre de la tabla sin el esquema
            table_name_only = table.name
            default_table = True if "-" in table_name_only else False

            # Construir una clave de stream usando el esquema y nombre de tabla (por ejemplo, "kielsa.mytable")
            full_table_name = f"""\"{schema}\".\"{table_name_only}\""""

            # Obtener la lista de columnas de clave primaria
            pk_columns: List[str] = [str(col.name) for col in table.primary_key]

            # Verificar si existe la columna update_key en la tabla
            column_names = [col.name for col in table.columns]
            final_update_key = update_keys[0]
            has_update_key = False
            for update_key in update_keys:
                final_update_key = update_key
                has_update_key = update_key in column_names
                if has_update_key:
                    break

            if not pk_columns and not has_update_key:
                continue  # Omitir tablas sin claves primarias y fecha de actualización

            # Buscar todas las unique constraints/indexes y separarlas
            unique_fields_multi = []  # Lista de listas para múltiples unique compuestas
            unique_idx_columns = []

            # Revisar indexes únicos
            for idx in table.indexes:
                if idx.unique:
                    if len(idx.columns) > 1:
                        # Unique compuesta - agregar como lista
                        unique_combo = [str(col.name) for col in idx.columns]
                        if unique_combo not in unique_fields_multi:
                            unique_fields_multi.append(unique_combo)
                    elif len(idx.columns) == 1:
                        # Unique simple - la agregamos a la lista
                        col_name = str(list(idx.columns)[0].name)
                        if col_name not in unique_idx_columns:
                            unique_idx_columns.append(col_name)

            # Revisar constraints únicos
            for cst in table.constraints:
                if isinstance(cst, sql.UniqueConstraint):
                    if cst.columns == pk_columns:
                        continue  # Omitir si es igual a primary key

                    if len(cst.columns) > 1:
                        # Unique compuesta - agregar como lista
                        unique_combo = [str(col.name) for col in cst.columns]
                        if unique_combo not in unique_fields_multi:
                            unique_fields_multi.append(unique_combo)
                    elif len(cst.columns) == 1:
                        # Unique simple - la agregamos a la lista
                        col_name = str(list(cst.columns)[0].name)
                        if col_name not in unique_idx_columns:
                            unique_idx_columns.append(col_name)

            # Si hay unique compuesta, guardarla en manual_streams
            if unique_fields_multi:
                manual_streams[full_table_name] = {
                    "object": f"{target_schema}.{normalize_table_identifier(schema)}_{normalize_table_identifier(table_name_only)}",
                    "target_options": {
                        "table_keys": {
                            "unique": unique_fields_multi,
                        },
                    },
                    "note": "Unique compuesta detectada. Generar manualmente.",
                }

            types_mapping: Dict[str, tuple] = {
                "TIME": ("VARCHAR(20)", "::varchar(20)"),
                "BOOLEAN": ("INTEGER", "::integer"),
                "VARCHAR": ("VARCHAR(100)", "::varchar(100)"),
            }

            stream_entry: Dict[str, Any] = {}
            if has_update_key:
                # Si existe la columna fecha_actualizado, usarla como update_key
                stream_entry["update_key"] = final_update_key
                stream_entry["sql"] = dedent(
                    f"""
                    select 
                        {
                        ", ".join(
                            [
                                f'"{col.name}"{types_mapping.get(str(col.type), ("", ""))[1]}'
                                for col in table.columns
                            ]
                        )
                    }
                    from {full_table_name} 
                    where \"{
                        final_update_key
                    }\" > coalesce({{incremental_value}}::timestamp, '2001-01-01'::timestamp) - INTERVAL '1 day'
                    """
                )
            if default_table or not has_update_key:
                stream_entry["disabled"] = True

            # Construir la entrada de stream por tabla
            stream_entry["object"] = (
                f"{target_schema}.{normalize_table_identifier(schema)}_{normalize_table_identifier(table_name_only)}"
            )
            stream_entry["primary_key"] = pk_columns
            stream_entry["target_options"] = {
                "table_keys": {
                    "primary": pk_columns,
                    "unique": unique_idx_columns,
                },
                "use_bulk": False,
            }
            stream_entry["columns"] = [
                {
                    str(col.name): str(
                        types_mapping.get(str(col.type), (col.type, ""))[0]
                    )
                }
                for col in table.columns
            ]
            streams[full_table_name] = stream_entry

    # Agregar placeholder si esta vacio para que siempre pueda ejecutarse el asset
    if not streams:
        streams["placeholder"] = {
            "disabled": False,
            "primary_key": ["id"],
            "columns": [{"id": "INTEGER"}],
        }

    # Construir la estructura de configuración YAML final
    config: Dict[str, Any] = {
        "source": source,
        "target": target,
        "defaults": defaults,
        "streams": streams,
        "env": {"SLING_LOADED_AT_COLUMN": True},
    }

    # Escribir la configuración YAML al archivo principal
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, sort_keys=False)

    # Si hay streams manuales, escribirlos en un archivo aparte
    if manual_streams:
        manual_path = os.path.join(
            output_dir, output_filename.replace(".yaml", "_manual.yaml")
        )
        manual_config = {
            "source": source,
            "target": target,
            "defaults": defaults,
            "streams": manual_streams,
            "env": {"SLING_LOADED_AT_COLUMN": True},
        }
        with open(manual_path, "w", encoding="utf-8") as f:
            yaml.dump(manual_config, f, sort_keys=False)

    return output_path


# Ejemplo de uso:
if __name__ == "__main__":
    # Verificar si el script se está ejecutando directamente con argumentos
    if len(sys.argv) > 1:
        # Si se proporcionan argumentos, usarlos

        # Formato esperado: python labs_experimental.py "postgresql://user:pass@host:port/db" schema [output_filename]
        connection_string = sys.argv[1]
        schema_name = sys.argv[2]
        output_file = sys.argv[3] if len(sys.argv) > 3 else "sling.yaml"

        # Crear el motor de conexión
        engine = sql.create_engine(connection_string)
        generate_sling_yaml_from_source(
            engine=engine,
            source_schemas=schema_name,
            output_filename=output_file,
            output_dir=".",
            source="NOCODB_DATA_GF",
            target="DAGSTER_DWH_FARINTER",
        )
    else:
        # Comportamiento predeterminado cuando se ejecuta sin argumentos
        try:
            # Intentar importar los recursos de dagster_shared_gf
            from dagster_shared_gf.resources.postgresql_resources import (
                db_nocodb_data_gf,
            )

            # Obtener el motor de conexión
            engine = db_nocodb_data_gf.get_engine()

            # Especificar el esquema a inspeccionar (por ejemplo, "kielsa")
            target_schema = "kielsa"

            # Generar el archivo YAML de Sling
            generate_sling_yaml_from_source(
                engine=engine,
                source_schemas=target_schema,
                output_filename="sling.yaml",
                output_dir=".",
                source="NOCODB_DATA_GF",
                target="DAGSTER_DWH_FARINTER",
            )
        except ImportError:
            print(
                "Error: Al ejecutar sin argumentos, el script requiere que dagster_shared_gf esté instalado."
            )
            print(
                'Uso: python labs_experimental.py "cadena_de_conexion" esquema [nombre_archivo_salida]'
            )
            sys.exit(1)
