{% macro dwh_farinter_create_primary_key(relation, columns=None, create_clustered=False, is_incremental=False, if_another_exists_delete=False, show_info=False) %}
    {%- if columns is none or columns | length == 0 %}
        -- No columns specified for primary key, skipping primary key creation.
        {% do log("No columns specified for primary key. Skipping primary key creation.", info=show_info) %}
        {{ return(";") }}
    {%- endif %}

    {%- if is_incremental %}
        -- Incremental run, skipping primary key creation.
        {% do log("Incremental run. Skipping primary key creation.", info=show_info) %}
        {{ return(";;") }}
        
    {%- endif %}

    {%- set primary_key_name = 'PK_' ~ relation.identifier | replace(".", "") | replace(" ", "") ~ '_dbt' -%}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation.identifier ~ '"' -%}
    {%- set relation_name = relation.schema ~ '.' ~ relation.identifier -%}

    USE [{{ relation.database }}];
    SELECT '';
    {%- if if_another_exists_delete %}
        {% do log("Checking for existing primary keys to drop.", info=show_info) %}

        {%- call statement('existing_primary_key_name', fetch_result=True) -%}

            select top 1 name
            FROM sys.key_constraints
            WHERE parent_object_id = OBJECT_ID('{{ relation_name }}')
            AND type = 'PK'

        {%- endcall -%}
        {%- set existing_primary_key = load_result('existing_primary_key_name')['data'] -%}
        {%- if existing_primary_key -%}
            
            {% do log("Existing primary key found. Dropping existing primary key.", info=show_info) %}
            -- Existing primary key {{existing_primary_key[0][0]}} found. Dropping existing primary key.
                ALTER TABLE {{ full_relation }} 
                DROP CONSTRAINT {{existing_primary_key[0][0]}};
        {%- endif %}
    {%- endif %}

    {% do log("Checking for existing clustered index.", info=show_info) %}
    {%- call statement('existing_clustered_index_name', fetch_result=True) -%}

        SELECT name
        FROM sys.indexes
        WHERE object_id = OBJECT_ID('{{ relation_name }}')
        AND type_desc LIKE 'CLUSTERED%' AND is_primary_key <> 1;

    {%- endcall -%}
    {%- set existing_clustered_index = load_result('existing_clustered_index_name')['data'] -%}

    {%- if existing_clustered_index and create_clustered -%}
        {% do log(existing_clustered_index) %}
        {% do log("Existing clustered index found. Index will not be created.", info=True) %}
        -- Existing clustered index found. Index will not be created.
        {{ return(";;;") }}
    {%- else -%}
        -- Dropping existing primary key constraint with the specific name if exists
        IF EXISTS (
            SELECT * 
            FROM sys.key_constraints 
            WHERE name = '{{ primary_key_name }}'
        )
        BEGIN
            ALTER TABLE {{ full_relation }} 
            DROP CONSTRAINT {{ primary_key_name }};
        END;

        -- Adding the new primary key constraint
        ALTER TABLE {{ full_relation }} 
        ADD CONSTRAINT {{ primary_key_name }} 
        PRIMARY KEY 
        {% if create_clustered -%}
            CLUSTERED
        {% else -%}
            NONCLUSTERED
        {%- endif -%}
        ({{ columns | join(', ') }});
    {%- endif %}



{% endmacro %}
