{% macro check_for_cte(sql) %}
    {% if execute %}  {# Ensure this runs only at execution time #}
        {% set cleaned_sql = sql | lower | trim | replace("\n", " ") %}
        {% set cte_pos = cleaned_sql.find("with ") %}  {# Find position of first "WITH " #}
        {% if cte_pos == 0 %}
            {{ return(True) }}  {# CTE found at start #}
        {% endif %}
        {% set first_from_pos = cleaned_sql.find(" from ") %}  {# Find position of first " FROM " #}
        {% if cte_pos != -1 and (cte_pos < first_from_pos) %}  {# Ignore "WITH " after "FROM " #}
            {{ return(True) }}
        {% else %}
            {{ return(False) }}  {# No CTE found #}
        {% endif %}
    {% else %}
        {{ return(False) }}  {# Return False during parsing #}
    {% endif %}
{% endmacro %}

{% macro sqlserver__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {% if check_for_cte(select_sql) %}
        {# If there's a CTE, we can't wrap it in another subquery, so just modify all selects #}
        {{ select_sql | replace('select ', 'select top 0 ') }}
    {% else -%}
        select * from (
        {{ select_sql }}
    ) dbt_sbq_tmp
    where 1 = 0
    {%- endif -%}

{% endmacro %}

{% macro sqlserver__get_columns_in_query(select_sql) %}
    {% set query_label = apply_label() %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        {{ get_empty_subquery_sql(select_sql) }}
        {{ query_label }}
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro sqlserver__alter_column_type(relation, column_name, new_column_type) %}

    {%- set tmp_column = column_name + "__dbt_alter" -%}
    {% set alter_column_type %}
        alter {{ relation.type }} {{ relation }} add "{{ tmp_column }}" {{ new_column_type }};
    {%- endset %}

    {% set update_column %}
        update {{ relation }} set "{{ tmp_column }}" = "{{ column_name }}";
    {%- endset %}

    {% set drop_column %}
        alter {{ relation.type }} {{ relation }} drop column "{{ column_name }}";
    {%- endset %}

    {% set rename_column %}
        exec sp_rename '{{ relation | replace('"', '') }}.{{ tmp_column }}', '{{ column_name }}', 'column'
    {%- endset %}

    {% do run_query(alter_column_type) %}
    {% do run_query(update_column) %}
    {% do run_query(drop_column) %}
    {% do run_query(rename_column) %}

{% endmacro %}
