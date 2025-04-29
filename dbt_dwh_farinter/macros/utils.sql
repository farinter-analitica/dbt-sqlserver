{%- macro dwh_farinter_get_type(value) %}
{%- if value is boolean %}
    boolean
{%- elif value is number %}
    number
{%- elif value is string %}
    string
{%- elif value is sequence %}
    sequence
{%- elif value is mapping %}
    mapping
{%- elif value is iterable and not value is string %}
    iterable
{%- else %}
    unknown
{%- endif %}
{%- endmacro %}

{%- macro dwh_farinter_get_default_dummy_value(column) %}
{%- if column is not none and column.data_type is not none and column.name is not none %}
    {%- set re = modules.re %}
    {%- if column.is_string() or re.search( '.*char.*', column.data_type|lower, re.IGNORECASE) or re.search( 'text', column.data_type|lower,re.IGNORECASE)  %}
        ISNULL(CAST('X' AS {{column.data_type}}),'')
    {%- elif column.is_number() or column.is_float() 
        or re.search( 'int',column.data_type|lower, re.IGNORECASE) 
        or re.search( 'decimal',column.data_type|lower, re.IGNORECASE) 
        or re.search( 'numeric',column.data_type|lower, re.IGNORECASE) 
        or re.search( 'money',column.data_type|lower, re.IGNORECASE) %}
        ISNULL(CAST(0 AS {{column.data_type}}),'')
    {%- elif column.data_type|lower in ["timestamp", "timestamp_ntz", "timestamp_ltz", "date", "datetime", "datetime2", "datetimeoffset"] and 
            "fecha_carga" not in column.name|lower and "fecha_actualizado" not in column.name|lower %}
        ISNULL(CAST('19000101' AS {{column.data_type}}),'19000101')
    {%- elif column.data_type|lower in ["timestamp", "timestamp_ntz", "timestamp_ltz", "date", "datetime", "datetime2", "datetimeoffset"] and 
            ("fecha_carga" in column.name|lower or "fecha_actualizado" in column.name|lower) %}
        ISNULL(CAST(current_timestamp AS {{column.data_type}}),'19000101')
    {%- elif column.data_type|lower == "boolean"  or column.data_type|lower == "bit"  %}
        ISNULL(CAST(0 AS {{column.data_type}}),'')
    {%- else %}
        CAST(NULL AS {{column.data_type}})
    {%- endif %}
{%- else %}
    {%- do log("Column is undefined or missing required attributes: " ~ column, info=true) %}
    NULL
{%- endif %}
{%- endmacro %}



{%- macro dwh_farinter_create_dummy_data(unique_key, is_incremental=is_incremental(), show_info=False, custom_column_values={}, loop_column_list={}) -%}
    {%- set predicates = [] %}
    {%- set all_columns_list_base_type = adapter.get_columns_in_relation(this) %}
    {#%- set merge_update_columns = config.get('merge_update_columns') -%#}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {#%- set merge_check_diff_exclude_columns = config.get('merge_check_diff_exclude_columns') -%#}
    {# set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns=all_columns_list_base_type) #}
    {%- if merge_exclude_columns -%}
        {%- set update_columns = [] -%}
        {%- for column in all_columns_list_base_type -%}
        {% if column.column | lower not in merge_exclude_columns | map("lower") | list %}
            {%- do update_columns.append(column) -%}
        {% endif %}
        {%- endfor -%}
    {%- else -%}
        {%- set update_columns = all_columns_list_base_type -%}
    {%- endif -%}
    {%- set unique_key = config.require('unique_key') -%}
    {%- if unique_key %}
        {%- do log("Unique key is defined: " ~ unique_key, info=show_info) %}
        {%- if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {%- do log("Unique key is a sequence", info=show_info) %}
            {%- for key in unique_key %}
                {%- set this_key_match = "dbtSOURCE." ~ key ~ " = dbtTARGET." ~ key %}
                {%- do predicates.append(this_key_match) %}
                {%- do log("Adding key match: " ~ this_key_match, info=show_info) %}
            {%- endfor %}
        {%- else %}
            {%- set unique_key_match = "dbtSOURCE." ~ unique_key ~ " = dbtTARGET." ~ unique_key %}
            {%- do predicates.append(unique_key_match) %}
            {%- do log("Unique key is a single key: " ~ unique_key_match, info=show_info) %}
        {%- endif %}
    {%- else %}
        {%- do predicates.append('FALSE') %}
        {%- do log("Unique key is not defined, adding FALSE predicate", info=show_info) %}
    {%- endif %}
        ;
    {%- set update_columns = update_columns if update_columns else all_columns_list_base_type %}
    {%- if all_columns_list_base_type %}
        {%- do log("All columns in relation: " ~ all_columns_list_base_type, info=show_info) %}
        merge into {{ this }} dbtTARGET
        using (
            {%- if loop_column_list|length > 0 %}
                {% set loop_column = loop_column_list.keys()|list|first %}
                {% set loop_values = loop_column_list[loop_column] %}
                select * from (
                    {% for value in loop_values %}
                        select 
                        {%- for column in all_columns_list_base_type %}
                            {%- if not loop.first %},{%- endif -%}
                            {%- if column.name == loop_column %}
                                CAST({{value}} AS {{column.data_type}}) as {{ column.name }}
                            {%- elif column.name in custom_column_values %}
                                CAST({{custom_column_values[column.name]}} AS {{column.data_type}}) as {{ column.name }}
                            {%- else %}
                                {{ dwh_farinter_get_default_dummy_value(column) }} as {{ column.name }}
                            {%- endif %}
                        {%- endfor %}
                        {% if not loop.last %}UNION ALL{% endif %}
                    {% endfor %}
                ) t
            {%- else %}
                select
                {%- for column in all_columns_list_base_type %}
                    {%- if not loop.first %},{%- endif -%}
                    {%- if column.name in custom_column_values %}
                        CAST({{custom_column_values[column.name]}} AS {{column.data_type}}) as {{ column.name }}
                    {%- else %}
                        {{ dwh_farinter_get_default_dummy_value(column) }} as {{ column.name }}
                    {%- endif %}
                {%- endfor %}
            {%- endif %}
        ) dbtSOURCE

        on {{ predicates | join(" and ") }}
        when matched and 
            exists (SELECT {%- for column in update_columns %}
                dbtTARGET.{{ column.name }} 
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
            EXCEPT
            SELECT {%- for column in update_columns %}
                dbtSOURCE.{{ column.name }} 
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
            ) 
        then update set
            {%- for column in update_columns %}
                {%- if not loop.first %},{%- endif %}
                dbtTARGET.{{ column.name }} = dbtSOURCE.{{ column.name }}
            {%- endfor %}
        when not matched then
        insert (
            {%- for column in all_columns_list_base_type %}
                {%- if not loop.first %},{%- endif %}
                {{ column.name }}
            {%- endfor %}
        )
        values (
            {%- for column in all_columns_list_base_type %}
                {%- if not loop.first %},{%- endif %}
                dbtSOURCE.{{ column.name }}
            {%- endfor %}
        )
        ;
    {%- else %}
        {%- do log("No columns found in relation", info=show_info) %}
    {%- endif %}
{%- endmacro %}





{%- macro dwh_farinter_union_all_dummy_data(unique_key, is_incremental=is_incremental(), show_info=False) -%}
{# no funciona bien para cambios de esquema ya que get_columns_in_relation lee la tabla en la bd #}
{%- if not is_incremental %}
UNION ALL
    {%- set predicates = [] %}
    {%- do log("Entering dwh_farinter_create_dummy_data macro", info=show_info) %}
    {%- set all_columns_list_base_type = adapter.get_columns_in_relation(this) %}
    {%- if all_columns_list_base_type %}
        {%- do log("All columns in relation: " ~ all_columns_list_base_type, info=show_info) %}
        select
        {%- for column in all_columns_list_base_type -%}
            {%- if not loop.first %},{%- endif -%}
            {{ dwh_farinter_get_default_dummy_value(column) }} as {{ column.name }}
        {%- endfor -%}  
    {%- else %}
        {%- do log("No columns found in relation", info=show_info) %}
    {%- endif %}
{%- else %}
    {%- do log("Incremental mode is enabled, skipping dummy data creation", info=show_info) %}
{%- endif %}
{%- endmacro %}

{% macro run_query_and_return(query='',show_info=False) %}
    {% do log("Entering run_query_and_return macro", info=show_info) %}
    {%- if execute  %}
        {%- if query == '' -%}
            {% do log("Query not specified.", info=show_info) %}
            {% do return([{}]) %}
        {%- endif %}

        {%- set query_to_run %}
            {{query}};
        {%- endset %}

        {% set results = [{}] %}
            {% do log("Running query: " ~ query_to_run, info=show_info)         %}
            {%- set query_results = run_query(query_to_run) %}
            {%- if query_results|length > 0 %} 
                {%- set results = query_results %}
            {%- endif %}
    {%- endif %}

    {% do return(results) %}

{% endmacro %}


{% macro run_single_value_query_on_relation_and_return(relation=this,query='',relation_not_found_value='NULL',show_info=False) %}
    {% if not execute %}
        {% do return(relation_not_found_value) %}
    {% endif %}
    {% do log("Entering run_single_value_query_on_relation_and_return macro from " + this|string, info=show_info) %}
    {%- if query == '' -%}
        {% do log("Query not specified.", info=show_info) %}
        {% do return(relation_not_found_value) %}
    {%- endif %}
    {%- set query_check %}
            USE [{{ relation.database }}];
            SELECT TOP 1 1 FROM sys.tables WHERE name = '{{ relation.identifier }}';
    {%- endset %}	
    {%- if execute  %}
        {% do log("Running query: "  ~ query_check, info=show_info)         %}
        {%- set results = run_query(query_check) %}
        {% if results|length > 0 %}
        {# Execute only on runtime Return the first column #}
            {% print(results) %}
            {%- set results_list = results.columns[0] | default([0]) %}
            {% do log("Something found", info=show_info) %}
        {%- else %}
            {% do log("Empty result", info=show_info) %}
            {%- set results_list = [0] %}
        {%- endif %}
    {%- else %}
        {%- set results_list = [0] %}
        {% do log("Not executed", info=show_info) %}
    {%- endif %}
    {%- set value = results_list[0]  %}
    {% if value|int != 1 %}
        {% do log("Relation not found", info=show_info) %}
        {% do return(relation_not_found_value) %}
    {% endif %}

    {%- set query_to_run %}
        USE [{{ relation.database }}];
        {{query}};
    {%- endset %}
    {%- if execute  %}
        {% do log("Running query: " ~ query_to_run, info=show_info)         %}
        {%- set results = run_query(query_to_run) %}
        {%- if results|length > 0 %}
        {# Execute only on runtime Return the first column #}
            {%- set results_list = results.columns[0] %}
            {% do log("Something found" ~ results_list, info=show_info) %}
        {%- else %}
            {%- set results_list = [relation_not_found_value] %}
        {%- endif %}
        {%- set value = results_list[0]  %}
    {%- else %}
        {%- set value = relation_not_found_value %}
    {%- endif %}

    {% do return(value) %}

{% endmacro %}

{% macro run_execute_query_on_relation_without_return(relation=this,query='', show_info=False) %}
    {% do log("Entering run_execute_query_on_relation_without_return macro", info=show_info) %}
    {%- if query == '' -%}
        {% do log("Query not specified.", info=show_info) %}
    {%- endif %}
    {%- set query_check %}
        USE [{{ relation.database }}];
        SELECT TOP 1 1 FROM sys.tables WHERE name = '{{ relation.identifier }}';
    {%- endset %}	
    {% do log("Running query: "  ~ query_check, info=show_info)         %}
    {%- if execute  %}
    {%- set results = run_query(query_check) %}
        {% if results|length > 0 %}
        {# Execute only on runtime Return the first column #}
            {%- set results_list = results.columns[0] | default([0]) %}
            {% do log("Something found", info=show_info) %}
        {%- else %}
            {% do log("Empty result", info=show_info) %}
            {%- set results_list = [0] %}
        {%- endif %}
    {%- else %}
        {%- set results_list = [0] %}
        {% do log("Not executed", info=show_info) %}
    {%- endif %}
    {%- set value = results_list[0]  %}
    {% if value|int != 1 %}
        {% do log("Relation not found", info=show_info) %}
        {% do return %}
    {% else %}

        {%- set query_to_run %}
            USE [{{ relation.database }}];
            {{query}}
        {%- endset %}
        {% if execute  %}
            {% do log("Running query: " ~ query_to_run, info=show_info) %}
            {%- do run_query(query_to_run) %}
            {% do log("Query executed", info=show_info) %}
        {% else %}
            {% do log("Not executed", info=show_info) %}
        {% endif %}

    {% endif %}

{% endmacro %}

{% macro check_linked_server(server_name) %}
    {% set check_connection_query %}
    BEGIN TRY
        -- Attempt to query the linked server with a simple test query
        DECLARE @result INT
        SET @result = (
            SELECT 1 
            FROM OPENQUERY([{{ server_name }}], 'SELECT 1 AS test')
        )
        SELECT 1 AS is_available
    END TRY
    BEGIN CATCH
        -- If an error occurs, the server is not accessible
        SELECT 0 AS is_available
    END CATCH
    {% endset %}
    
    {% set connection_check = run_query_and_return(check_connection_query) %}
    {% if connection_check and connection_check[0]['is_available'] == 1 %}
        {{ return(true) }}
    {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}
