{% macro dwh_farinter_create_clustered_columnstore_index(relation=this, is_incremental=is_incremental(), if_another_exists_drop_it=False, show_info=False, use_dynamic_name=True) %}
    {%- if is_incremental %}
        -- Incremental run, skipping clustered columnstore index creation.
        {% do log("Incremental run. Skipping clustered columnstore index creation.", info=show_info) %}
        {{ return(";") }}
    {%- endif %}

    {%- set index_name = 'CCI_' ~ relation.identifier | replace(".", "") | replace(" ", "") ~ '_dbt' -%}
    {%- if use_dynamic_name %}
        {% set index_name = index_name ~ "_" ~ local_md5(columns|join('-') ~ run_started_at.strftime("%Y-%m-%dT%H:%M:%S.%f"))  -%}
    {%- endif %}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation.identifier ~ '"' -%}
    {%- set relation_name = relation.schema ~ '.' ~ relation.identifier -%}

    USE [{{ relation.database }}];

    -- Declare variables to store intermediate results
    DECLARE @existing_index NVARCHAR(128);
    DECLARE @existing_clustered_index NVARCHAR(128);

    {%- if if_another_exists_drop_it %}
        {% do log("Dropping any existing clustered columnstore index.", info=show_info) %}

        -- Fetch existing clustered columnstore index name if it exists
        SELECT TOP 1 @existing_index = name
        FROM sys.indexes
        WHERE object_id = OBJECT_ID('{{ relation_name }}')
        AND type_desc = 'CLUSTERED COLUMNSTORE';

        -- Drop existing clustered columnstore index if found
        IF @existing_index IS NOT NULL
        BEGIN
            EXEC('DROP INDEX ' + @existing_index + ' ON {{ full_relation }}');
        END;
    {%- endif %}

    {%- do log("Creating new Clustered Columnstore Index.", info=show_info) -%}

    -- Exit if existing clustered index is found --better to throw an error here
    --IF @existing_clustered_index IS NULL
    BEGIN
        -- Drop specific CLUSTERED COLUMNSTORE INDEX if it exists
        IF EXISTS (
            SELECT * 
            FROM sys.indexes 
            WHERE object_id = OBJECT_ID('{{ relation_name }}')
            AND name = '{{ index_name }}'
        )
        BEGIN
            DROP INDEX {{ index_name }} ON {{ full_relation }}
        END;
        -- Add the new CLUSTERED COLUMNSTORE INDEX
        
        CREATE CLUSTERED COLUMNSTORE INDEX {{ index_name }} ON {{ full_relation }};
    END
    
{% endmacro %}



{%- macro dwh_farinter_create_index(relation=this
    , columns=[]
    , create_clustered=false
    , create_unique=false
    , is_incremental=is_incremental()
    , show_info=false
    , use_dynamic_name=false
    , dynamic_part=run_started_at.strftime("%Y%m%dT%H%M%S%f")
    , included_columns=[]) 
    %}
    {%- if columns is none or columns | length == 0 %}
        -- No columns specified for index, skipping index creation.
        {%- do log("No columns specified for index. Skipping index creation.", info=show_info) %}
        {{ return(";") }}
    {%- endif %}

    {%- if is_incremental %}
        -- Incremental run, skipping index creation.
        {%- do log("Incremental run. Skipping index creation.", info=show_info) %}
        {{ return(";;") }}
    {%- endif %}

    {%- set index_name = 'IX_' ~ relation.identifier | replace(".", "") | replace(" ", "") ~ '_dbt' %}
    {%- if use_dynamic_name %}
        {# {%- set index_name = index_name ~ "_" ~  v_current_time ~ v_random_number  %} #}
        {%- set final_index_name = index_name ~ "_" ~ local_md5((columns + included_columns)|join('_'))[:16] ~ "_"  ~ dynamic_part %}
    {%- else %}
        {%- set final_index_name = index_name ~ "_" ~ local_md5((columns + included_columns)|join('_'))[:16] %}
    {%- endif %}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation.identifier ~ '"' %}
    {%- set relation_name = relation.schema ~ '.' ~ relation.identifier %}

    USE [{{ relation.database }}];

    {%- do log("Creating new index.", info=show_info) %}

    BEGIN
        -- Drop specific index constraint if it exists
        IF EXISTS (
            SELECT * 
            FROM sys.indexes 
            WHERE object_id = OBJECT_ID('{{ relation_name }}')
            AND name = '{{ final_index_name }}'
        )
        BEGIN
            DROP INDEX {{ final_index_name }} ON {{ full_relation }};
        END;
        -- Add the new index constraint

        CREATE
        {%- if create_unique %} UNIQUE {% else %} {% endif -%}
        {%- if create_clustered %} CLUSTERED {% else %} NONCLUSTERED {% endif -%} 
        INDEX {{ final_index_name }} 
        ON {{ full_relation }} 
        ({{ columns | join(', ') }})
        {% if included_columns|length>0 -%} INCLUDE({{ included_columns | join(', ') }}) {%- endif %} 
        ;
        
    END



{%- endmacro %}
