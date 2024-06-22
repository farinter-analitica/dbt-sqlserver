{% macro dwh_farinter_create_clustered_columnstore_index(relation) -%}
  {%- set cci_name = (relation.schema ~ '_' ~ relation.identifier ~ '_cci') | replace(".", "") | replace(" ", "") -%}
  {%- set relation_name = relation.schema ~ '_' ~ relation.identifier -%}
  {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation.identifier ~ '"' -%}
  use [{{ relation.database }}];
  if not EXISTS (
        SELECT *
        FROM sys.indexes {{ information_schema_hints() }}
        WHERE name = '{{cci_name}}'
        AND object_id=object_id('{{relation_name}}')
    )
  CREATE CLUSTERED COLUMNSTORE INDEX {{cci_name}}
    ON {{full_relation}}
{% endmacro %}
