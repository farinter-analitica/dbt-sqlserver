{% macro dwh_farinter_hash_column(columns, output_length=15, input_length=8000) -%} 
{%- set hash_algorithm_str = 'SHA2_256' -%}
{%- set column_collation_str = ' COLLATE DATABASE_DEFAULT' -%}

{%- set modified_columns = [] -%}
{%- for column in columns -%}
    {%- do modified_columns.append(column ~ column_collation_str) -%}
{%- endfor -%}
{#--example: CONVERT(varchar(18),HASHBYTES('SHA2_256', CAST(CONCAT(A.[KTOPL] COLLATE DATABASE_DEFAULT ,'-', A.[SAKNR] COLLATE DATABASE_DEFAULT ) AS VARCHAR(MAX))),2)#}
{%- set columns_str = modified_columns | join(', ') -%}
CONVERT(varchar({{ output_length }}),HASHBYTES('{{ hash_algorithm_str }}', CAST(CONCAT({{ columns_str }}) AS VARCHAR({{ input_length }}))),2)
{%- endmacro %}
