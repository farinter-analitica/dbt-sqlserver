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
    {%- set sql_header = config.get('sql_header', none) -%}
    {% if update_columns | length == 0 or if_exists_diff_columns | length == 0 %}
        {%- do exceptions.raise_compiler_error("incremental mode: No columns to update, check merge config and columns excluded, at least one needed.") -%}
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

    {% if unique_key %}
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

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})
    ;

{% endmacro %}

