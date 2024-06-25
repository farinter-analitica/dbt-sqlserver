{% macro dwh_farinter_create_clustered_columnstore_index(relation=this, is_incremental=is_incremental(), if_another_exists_drop_it=False, show_info=False, use_random_name=True) %}
    {%- if is_incremental %}
        -- Incremental run, skipping clustered columnstore index creation.
        {% do log("Incremental run. Skipping clustered columnstore index creation.", info=show_info) %}
        {{ return(";") }}
    {%- endif %}

    {%- set index_name = 'CCI_' ~ relation.identifier | replace(".", "") | replace(" ", "") ~ '_dbt' -%}
    {%- if use_random_name %}
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

    {%- if create_clustered -%}
        {% do log("Checking for existing clustered index.", info=show_info) %}

        {% set existing_clustered_index_query -%}
        -- Query to fetch clustered index name if it exists
        SELECT name
        FROM sys.indexes
        WHERE object_id = OBJECT_ID('{{ relation_name }}')
        AND type_desc LIKE 'CLUSTERED%' AND type_desc <> 'CLUSTERED COLUMNSTORE';

        {%- endset -%}

        -- Fetch existing clustered index name if it exists
        {%- set result = run_query(existing_clustered_index_query) -%}
        {%- if execute %}
        {# Return the first column #}
          {%- set results_list = result.columns[0].values() -%}
        {%- else -%}
          {%- set results_list = [] -%}
        {%- endif -%}
        {%- set value = results_list[0]  -%}
        {%- set existing_clustered_index = "'" ~	 value ~ "'" if value and value|length > 0 else "NULL" %}
        {# Asign the value to the SQL variable #}
        SET @existing_clustered_index = {{ existing_clustered_index }}
        {% if existing_clustered_index != "NULL" -%}
            {%- do log("Existing clustered index found. Clustered Columnstore Index will not be created.", error=True) -%}
        {%- endif -%}
    {%- endif -%}

    {%- do log("Creating new Clustered Columnstore Index if not exists.", info=show_info) -%}

    -- Exit if existing clustered index is found
    IF @existing_clustered_index IS NULL
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
