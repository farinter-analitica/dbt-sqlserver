{% macro dwh_farinter_hash_column(columns=[], output_length=32, input_length=8000, table_alias="") -%} 
    {%- set hash_algorithm_str = 'SHA2_256' -%}
    {%- set column_collation_str = ' COLLATE DATABASE_DEFAULT' -%}
    {%- set modified_columns = [] -%}
    {%- if columns is none or columns|length == 0 -%}
        {%- do log("Columns is undefined or wrong, received: (" ~ columns|toJson ~ ") ", error=true) -%}
    {%- endif -%}
    {%- for column in columns -%}
        {# filter empty columns #}
        {%- if column is not none and column|length > 0 -%}
            {%- if table_alias|length > 0 -%}
                {%- do modified_columns.append(table_alias ~ '."' ~ column ~ '"') -%}
            {%- else -%}
                {%- do modified_columns.append('"' ~ column ~ '"') -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {#--example: CONVERT(varchar(18),HASHBYTES('SHA2_256', CAST(CONCAT(A.[KTOPL] COLLATE DATABASE_DEFAULT ,'-', A.[SAKNR] COLLATE DATABASE_DEFAULT ) AS VARCHAR(MAX))),2)#}
    {%- set columns_str = modified_columns | join(", '-', ") -%}
    CONVERT(varchar({{ output_length }}),HASHBYTES('{{ hash_algorithm_str }}', CAST(CONCAT({{ columns_str }},'') {{column_collation_str}}  AS VARCHAR({{ input_length }}))),2)
{%- endmacro %}

{% macro dwh_farinter_concat_key_columns(columns=[], input_length=0, table_alias="") -%} 
    {%- set column_collation_str = ' COLLATE DATABASE_DEFAULT' -%}
    {%- set modified_columns = [] -%}
    {%- if columns is none or columns|length == 0 -%}
        {%- do log("Columns is undefined or wrong, received: (" ~ columns|toJson ~ ").", error=true) -%}
    {%- endif -%}
    {%- if input_length is none or input_length == 0 -%}
        {%- do log("It's mandatory to specify input_length because key columns best practices.", error=true) -%}
    {%- endif -%}
    {%- for column in columns -%}
        {# filter empty columns #}
        {%- if column is not none and column|length > 0 -%}
            {%- if table_alias|length > 0 -%}
                {%- do modified_columns.append(table_alias ~ '."' ~ column ~ '"') -%}
            {%- else -%}
                {%- do modified_columns.append('"' ~ column ~ '"') -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set ouput_length = input_length + modified_columns|length-1 -%}
    {%- set columns_str = modified_columns | join(", '-', ") -%}
    CAST(CONCAT({{ columns_str }},'') {{column_collation_str}}  AS VARCHAR({{ ouput_length }}))
{%- endmacro %}
