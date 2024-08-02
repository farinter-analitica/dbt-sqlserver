{% macro dwh_farinter_remove_incremental_temp_table(relation = this) -%}
    {%- set relation_name = relation.identifier ~ '__dbt_tmp' -%}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation_name ~ '"' -%}
    
    -- Ensure database selection is correct for your SQL dialect
    USE [{{ relation.database }}];

    -- Drop the temporary table if it exists
    IF EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES WITH (NOLOCK)
        WHERE TABLE_SCHEMA = '{{ relation.schema }}'
        AND TABLE_NAME = '{{ relation_name }}'
    )
    DROP TABLE {{ full_relation }};

    {%- set relation_name = relation.identifier ~ '__dbt_tmp_temp_view' -%}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation_name ~ '"' -%}
    -- Drop the temporary view if it exists
    IF EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.VIEWS WITH (NOLOCK)
        WHERE TABLE_SCHEMA = '{{ relation.schema }}'
        AND TABLE_NAME = '{{ relation_name }}'
    )
    DROP VIEW {{ full_relation }};

    {%- set relation_name = relation.identifier ~ '_temp_view' -%}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation_name ~ '"' -%}
    -- Drop the temporary view if it exists
    IF EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.VIEWS WITH (NOLOCK)
        WHERE TABLE_SCHEMA = '{{ relation.schema }}'
        AND TABLE_NAME = '{{ relation_name }}'
    )
    DROP VIEW {{ full_relation }};

    {%- set relation_name = relation.identifier ~ '__dbt_temp_temp_view' -%}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation_name ~ '"' -%}
    -- Drop the temporary view if it exists
    IF EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.VIEWS WITH (NOLOCK)
        WHERE TABLE_SCHEMA = '{{ relation.schema }}'
        AND TABLE_NAME = '{{ relation_name }}'
    )
    DROP VIEW {{ full_relation }};

{% endmacro %}
