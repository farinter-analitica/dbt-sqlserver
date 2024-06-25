### Best Practices Discover
- Do not use call statement/query to execute changes on database, when debugging, it will execute instead of just compiling, and the change could be done multiple times per each instance of macro.


### Example of Query Execution Mixed with SQL (This will be executed before the model logic even if the macro is post_hook)
```jinja
{%- set existing_clustered_index_query %}
-- Query to fetch clustered index name if it exists
SELECT name
FROM sys.indexes
WHERE object_id = OBJECT_ID('{{ relation_name }}')
AND type_desc LIKE 'CLUSTERED%' AND is_primary_key <> 1;

{%- endset %}

-- Fetch existing clustered index name if it exists
{%- set result = run_query(existing_clustered_index_query) %}
{%- if execute %}
{# Execute only on runtime Return the first column #}
    {%- set results_list = result.columns[0].values() %}
{%- else %}
    {%- set results_list = [] %}
{%- endif %}
{%- set value = results_list[0]  %}
{%- set existing_clustered_index = "'" ~	 value ~ "'" if value and value|length > 0 else "NULL" %}
{# Asign the value to the SQL variable #}
SET @existing_clustered_index = {{ existing_clustered_index }};
{%- if existing_clustered_index != "NULL" %}
    {%- do log("Existing clustered index found. Index will not be created.", error=True) %}
{%- endif %}
```
