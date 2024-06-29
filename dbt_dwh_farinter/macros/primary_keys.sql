{%- macro dwh_farinter_create_primary_key(relation=this
    , columns=[]
    , create_clustered=false
    , is_incremental=is_incremental()
    , if_another_exists_drop_it=true
    , show_info=false
    , use_random_name=true
    , dynamic_part=run_started_at.strftime("%Y%m%dT%H%M%S%f")) 
    %}
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
        none of this work, it doesnt really updates the time constantly (only at parse), probably because of macros not re-compiling for the model
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
        {%- set final_primary_key_name = primary_key_name ~ "_" ~ dynamic_part %}
    {%- else %}
        {%- set final_primary_key_name = primary_key_name %}
    {%- endif %}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation.identifier ~ '"' %}
    {%- set relation_name = relation.schema ~ '.' ~ relation.identifier %}

    USE [{{ relation.database }}];

    -- Declare variables to store intermediate results
    DECLARE @existing_primary_key NVARCHAR(128);
    DECLARE @existing_clustered_index NVARCHAR(128);

    {%- if if_another_exists_drop_it %}
        {%- do log("Dropping any existing primary key.", info=show_info) %}

        -- Fetch existing primary key name if it exists
        SELECT TOP 1 @existing_primary_key = name
        FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID('{{ relation_name }}')
        AND type = 'PK'
        AND name <> '{{ final_primary_key_name }}';

        -- Drop existing primary key if found
        IF @existing_primary_key IS NOT NULL
        BEGIN
            EXEC('ALTER TABLE {{ full_relation }} DROP CONSTRAINT ' + @existing_primary_key);
        END;
    {%- endif %}
    -- Exit if existing clustered index is found (better to throw an error here)
    {#{%- if create_clustered %}
        {%- do log("Checking for existing clustered index.", info=show_info) %}
        -- Query to fetch clustered index name if it exists
        SELECT @existing_clustered_index = name
        FROM sys.indexes
        WHERE object_id = OBJECT_ID('{{ relation_name }}')
        AND type_desc LIKE 'CLUSTERED%' AND is_primary_key <> 1;

    {%- endif %}#}

    {%- do log("Creating new primary key.", info=show_info) %}

    -- Exit if existing clustered index is found (better to throw an error here)
    --IF @existing_clustered_index IS NULL
    BEGIN
        -- Drop specific primary key constraint if it exists
        IF EXISTS (
            SELECT * 
            FROM sys.key_constraints 
            WHERE parent_object_id = OBJECT_ID('{{ relation_name }}')
            AND name = '{{ final_primary_key_name }}'
        )
        BEGIN
            ALTER TABLE {{ full_relation }} DROP CONSTRAINT {{ final_primary_key_name }};
        END;
        -- Add the new primary key constraint
        {# to fix bug when table is materialized and backup table have the same primary key #}
        DECLARE @primary_key_conflicting_table_name NVARCHAR(128);
        SELECT @primary_key_conflicting_table_name = OBJECT_NAME(k.parent_object_id)
        FROM sys.key_constraints k
        INNER JOIN sys.schemas sc ON sc.schema_id = k.schema_id
        WHERE k.name = '{{final_primary_key_name}}' AND sc.name = '{{relation.schema}}';

        -- Check if the conflicting table name ends with '__dbt_backup'
        IF @primary_key_conflicting_table_name LIKE '%__dbt_backup'
        BEGIN
            DECLARE @primary_key_constraint_to_rename NVARCHAR(256);
            SET @primary_key_constraint_to_rename = '{{relation.schema}}' + '.' + @primary_key_conflicting_table_name + '.' + '{{ final_primary_key_name }}';
            -- Rename the conflicting primary key constraint

            EXEC sp_rename @primary_key_constraint_to_rename,'{{ final_primary_key_name }}{{ range(1,100)|random}}', 'OBJECT';
        END;

        ALTER TABLE {{ full_relation }} 
        ADD CONSTRAINT {{ final_primary_key_name }} 
        PRIMARY KEY {%- if create_clustered %} CLUSTERED {%- else %} NONCLUSTERED {%- endif %} ({{ columns | join(', ') }});
        
    END



{%- endmacro %}
