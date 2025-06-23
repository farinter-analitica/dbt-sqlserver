{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set schema = (custom_schema_name | trim) if custom_schema_name is not none else target.schema -%}
    {{ schema }}
{%- endmacro %}