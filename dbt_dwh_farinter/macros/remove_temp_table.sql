{% macro dwh_farinter_remove_incremental_temp_table(relation) -%}
  {%- set relation_name = relation.identifier ~ '__dbt_tmp' -%}
  {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation_name ~ '"' -%}
  use [{{ relation.database }}];
  if EXISTS (
        SELECT *
        FROM sys.tables {{ information_schema_hints() }}
        WHERE name = '{{relation_name}}'
        AND object_id=object_id('{{relation_name}}')
    )
  DROP TABLE {{full_relation}}
{% endmacro %}
