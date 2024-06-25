{%- macro dwh_farinter_create_primary_key(relation=this, columns=None, create_clustered=False, is_incremental=is_incremental()
    , if_another_exists_drop_it=False
    , show_info=False, use_random_name=True, random_part=run_started_at.strftime("%Y%m%dT%H%M%S%f")) %}
    {%- if columns is none or columns | length == 0 %}
        -- No columns specified for primary key, skipping primary key creation.
        {%- do log("No columns specified for primary key. Skipping primary key creation.", info=show_info) %}
        {{ return(";") }}
    {%- endif %}

    {%- if is_incremental %}
        -- Incremental run, skipping primary key creation.
        {%- do log("Incremental run. Skipping primary key creation.", info=show_info) %}
        {{ return(";;") }}
    {%- endif %}
    {#
        none of this work, it doesnt really updates the time constantly, probably because of macros not re-compiling for the model
        , so when dbt creates the backup table with the primary key it will cause an error
        {%- set v_current_time = modules.datetime.datetime.now().strftime("%Y%m%dT%H%M%S%f") %}
        {%- set v_current_time = run_started_at.strftime("%Y%m%dT%H%M%S%f") %}
        {%- set v_random_number = (range(1,100)|random) %}
        {%- set result = run_query('SELECT CURRENT_TIMESTAMP') %}
        {%- if execute %} {%- set v_current_time = result.columns[0].values()[0] %} {%- else %} {%- set v_current_time = run_started_at.strftime("%Y%m%dT%H%M%S%f") %} {%- endif %}
    #}
    {%- set primary_key_name = 'PK_' ~ relation.identifier | replace(".", "") | replace(" ", "") ~ '_dbt' %}
    {%- if use_random_name %}
        {# {%- set primary_key_name = primary_key_name ~ "_" ~  v_current_time ~ v_random_number  %} #}
        {%- set new_primary_key_name = primary_key_name ~ "_" ~ random_part %}
    {%- else %}
        {%- set new_primary_key_name = primary_key_name %}
    {%- endif %}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation.identifier ~ '"' %}
    {%- set relation_name = relation.schema ~ '.' ~ relation.identifier %}

    USE [{{ relation.database }}];

    -- Declare variables to store intermediate results
    DECLARE @existing_primary_key NVARCHAR(128);
    DECLARE @existing_clustered_index NVARCHAR(128);

    {%- set existing_clustered_index = "NULL" %}

    {%- if if_another_exists_drop_it %}
        {%- do log("Dropping any existing primary key.", info=show_info) %}

        -- Fetch existing primary key name if it exists
        SELECT TOP 1 @existing_primary_key = name
        FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID('{{ relation_name }}')
        AND type = 'PK'
        AND name <> '{{ new_primary_key_name }}';

        -- Drop existing primary key if found
        IF @existing_primary_key IS NOT NULL
        BEGIN
            EXEC('ALTER TABLE {{ full_relation }} DROP CONSTRAINT ' + @existing_primary_key);
        END;
    {%- endif %}
    {%- if create_clustered %}
        {%- do log("Checking for existing clustered index.", info=show_info) %}

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
    {%- endif %}

    {%- do log("Creating new primary key if not exists.", info=show_info) %}

    -- Exit if existing clustered index is found
    IF @existing_clustered_index IS NULL
    BEGIN
        -- Drop specific primary key constraint if it exists
        IF EXISTS (
            SELECT * 
            FROM sys.key_constraints 
            WHERE parent_object_id = OBJECT_ID('{{ relation_name }}')
            AND name = '{{ new_primary_key_name }}'
        )
        BEGIN
            ALTER TABLE {{ full_relation }} DROP CONSTRAINT {{ new_primary_key_name }};
        END;
        -- Add the new primary key constraint
        {# to fix bug when table is materialized and backup table have the same primary key #}
        IF EXISTS (
            SELECT name
            FROM sys.key_constraints 
            WHERE name = '{{ new_primary_key_name }}'
        )  
        BEGIN
            ALTER TABLE {{ full_relation }} 
            ADD CONSTRAINT {{ new_primary_key_name ~ range(1, 100)|random }} 
            PRIMARY KEY {%- if create_clustered %} CLUSTERED {%- else %} NONCLUSTERED {%- endif %} ({{ columns | join(', ') }});
        END
        ELSE
        BEGIN
            ALTER TABLE {{ full_relation }} 
            ADD CONSTRAINT {{ new_primary_key_name }} 
            PRIMARY KEY {%- if create_clustered %} CLUSTERED {%- else %} NONCLUSTERED {%- endif %} ({{ columns | join(', ') }});
        END
    END
{%- endmacro %}
