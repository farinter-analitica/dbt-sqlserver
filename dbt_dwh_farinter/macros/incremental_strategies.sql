{#
Incremental strategy: farinter_merge
------------------------------------

Resumen
- Extiende la estrategia de merge para SQL Server con:
    - Exclusión de columnas en update/compare
    - Soft delete opcional
    - Soft delete filtrado por columna(s) para evitar borrados accidentales cuando faltan orígenes

Parámetros de config (model):
- incremental_strategy: "farinter_merge"
- unique_key: string | list[string]
- merge_exclude_columns: list[string]
    - Columnas a excluir del SET en update
- merge_check_diff_exclude_columns: list[string]
    - Columnas a excluir del bloque EXISTS/EXCEPT (comparación de cambios)
- merge_update_columns: list[string]
    - Si se define, limita explícitamente las columnas a actualizar
- merge_insert_only: bool (default false)
    - Si true, no hará updates, solo inserts
- merge_soft_delete_column: string | none
    - Si se define, filas "not matched by source" se marcan con 1 (soft delete)
- merge_soft_delete_filter_columns: list[string] (opcional)
    - Lista de columnas por las que se acota el soft delete. Ej: ['Emp_Id']
    - Evita marcar como borradas filas fuera del subconjunto procesado en el batch actual

Comportamiento de soft delete filtrado
- when not matched by source then update set <soft_delete_column> = 1
- Si se configuran "merge_soft_delete_filter_columns", agrega un AND adicional que requiere que los valores de esas columnas existan en la fuente temporal del merge.
- Esto permite:
    - Ejecutar cargas parciales por "Emp_Id" (u otra llave de partición) sin borrar datos de otras particiones que no se procesaron en el batch actual.

Ejemplo de uso en un modelo:
{{
    config(
        materialized='incremental',
        incremental_strategy='farinter_merge',
        unique_key=['Proveedor_Id','Emp_Id'],
        merge_exclude_columns=['Fecha_Carga'],
        merge_check_diff_exclude_columns=['Fecha_Carga','Fecha_Actualizado'],
        merge_soft_delete_column='Indicador_Borrado',
        merge_soft_delete_filter_columns=['Emp_Id']
    )
}}

Notas:
- Esta macro asume SQL Server y utiliza MERGE con una fuente temporal creada por dbt.
- Para cargas incrementales sensibles a borrados, prefiera no filtrar el CTE fuente por timestamp y acotar el soft delete con merge_soft_delete_filter_columns.
- Si se filtra por timestamp, asegúrese de procesar completamente la partición (p.ej. todas las empresas) para evitar falsos borrados.

Referencia oficial: https://docs.getdbt.com/docs/build/incremental-strategy
#}

{% macro get_incremental_farinter_merge_sql(arg_dict) %}
  {% do return(custom_get_farinter_merge_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["unique_key"], arg_dict["dest_columns"], arg_dict["incremental_predicates"])) %}
{% endmacro %}

{% macro custom_get_farinter_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns',[]) -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns',[]) -%}
    {%- set merge_check_diff_exclude_columns = config.get('merge_check_diff_exclude_columns',[]) -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set if_exists_diff_columns = get_merge_update_columns(merge_update_columns, merge_check_diff_exclude_columns, dest_columns) -%}
    {%- set merge_insert_only = config.get('merge_insert_only', false) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {% if not merge_insert_only and (update_columns | length == 0 or if_exists_diff_columns | length == 0) %}
        {%- do exceptions.raise_compiler_error("incremental mode: No hay columnas para update (todas estan excluidas o son llaves), verifica o especifica merge_insert_only = true.") -%}
    {% endif %}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {% endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{"(" ~ predicates | join(") and (") ~ ")"}}

    {% if unique_key and not merge_insert_only %}
    when matched 
      and exists (SELECT {% for column_name in if_exists_diff_columns -%}
            DBT_INTERNAL_DEST.{{ column_name }} 
            {%- if not loop.last and column_name not in merge_check_diff_exclude_columns %}, {%- endif %}
        {%- endfor %}
        EXCEPT
        SELECT {% for column_name in if_exists_diff_columns -%}
            DBT_INTERNAL_SOURCE.{{ column_name }} 
            {%- if not loop.last and column_name not in merge_check_diff_exclude_columns %}, {%- endif %}
        {%- endfor %}
        )
    then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched by target then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

    {% set merge_soft_delete_column = config.get('merge_soft_delete_column', none) %}
    {% set merge_soft_delete_filter_columns = config.get('merge_soft_delete_filter_columns', []) %}
    {% if merge_soft_delete_column is not none %}
    when not matched by source
        {%- if merge_soft_delete_filter_columns | length > 0 %}
        and (
            {%- for col in merge_soft_delete_filter_columns -%}
                DBT_INTERNAL_DEST.{{ col }} in (
                    select distinct {{ col }} from {{ source }}
                ){% if not loop.last %} and {% endif %}
            {%- endfor -%}
        )
        {%- endif %}
    then update
        set {{ merge_soft_delete_column }} = 1
    {% endif %}
    ;

{% endmacro %}
