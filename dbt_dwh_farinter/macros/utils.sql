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
        ISNULL(CAST('1900-01-01' AS {{column.data_type}}),'')
    {%- elif column.data_type|lower in ["timestamp", "timestamp_ntz", "timestamp_ltz", "date", "datetime", "datetime2", "datetimeoffset"] and 
            ("fecha_carga" in column.name|lower or "fecha_actualizado" in column.name|lower) %}
        ISNULL(CAST(current_timestamp AS {{column.data_type}}),'')
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



{%- macro dwh_farinter_create_dummy_data(unique_key, is_incremental=is_incremental(), show_info=False) %}
{%- if not is_incremental %}
    {%- set predicates = [] %}
    {%- do log("Entering dwh_farinter_create_dummy_data macro", info=show_info) %}

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
    {%- set all_columns = adapter.get_columns_in_relation(this) %}
    {%- if all_columns %}
        {%- do log("All columns in relation: " ~ all_columns, info=show_info) %}
        merge into {{ this }} dbtTARGET
        using (select
            {%- for column in all_columns %}
                {%- if not loop.first %},{%- endif -%}
                {{ dwh_farinter_get_default_dummy_value(column) }} as {{ column.name }}
            {%- endfor %}
        ) dbtSOURCE
        on {{ predicates | join(" and ") }}
        when matched and 
            exists (SELECT {%- for column in all_columns %}
                dbtTARGET.{{ column.name }} 
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
            EXCEPT
            SELECT {%- for column in all_columns %}
                dbtSOURCE.{{ column.name }} 
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
            ) 
        then update set
            {%- for column in all_columns %}
                {%- if not loop.first %},{%- endif %}
                dbtTARGET.{{ column.name }} = dbtSOURCE.{{ column.name }}
            {%- endfor %}
        when not matched then
        insert (
            {%- for column in all_columns %}
                {%- if not loop.first %},{%- endif %}
                {{ column.name }}
            {%- endfor %}
        )
        values (
            {%- for column in all_columns %}
                {%- if not loop.first %},{%- endif %}
                dbtSOURCE.{{ column.name }}
            {%- endfor %}
        )
        ;
    {%- else %}
        {%- do log("No columns found in relation", info=show_info) %}
    {%- endif %}
{%- else %}
    {%- do log("Incremental mode is enabled, skipping dummy data creation", info=show_info) %}
{%- endif %}
{%- endmacro %}





{%- macro dwh_farinter_union_all_dummy_data(unique_key, is_incremental=is_incremental(), show_info=False) -%}
{# no funciona bien para cambios de esquema ya que get_columns_in_relation lee la tabla en la bd #}
{%- if not is_incremental %}
UNION ALL
    {%- set predicates = [] %}
    {%- do log("Entering dwh_farinter_create_dummy_data macro", info=show_info) %}
    {%- set all_columns = adapter.get_columns_in_relation(this) %}
    {%- if all_columns %}
        {%- do log("All columns in relation: " ~ all_columns, info=show_info) %}
        select
        {%- for column in all_columns -%}
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
