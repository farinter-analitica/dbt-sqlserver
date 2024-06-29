{%- macro dwh_farinter_create_foreign_key(
    relation=this,
    columns=[],
    referenced_table="",
    referenced_columns=[],
    on_delete='NO ACTION',
    on_update='NO ACTION',
    is_incremental=is_incremental(),
    show_info=false,
    use_dynamic_name=false,
    dynamic_part=run_started_at.strftime("%Y%m%dT%H%M%S%f")
    )
%}

{%- if on_delete not in ('NO ACTION', 'CASCADE', 'SET NULL', 'SET DEFAULT') or on_update not in ('NO ACTION', 'CASCADE', 'SET NULL', 'SET DEFAULT') %}
    {%- do log("on_delete or on_update not in ('NO ACTION', 'CASCADE', 'SET NULL', 'SET DEFAULT')", error=true) %}
    {{ return("") }}
{%- endif %}

{%- if columns is none or columns | length == 0 or referenced_columns is none or referenced_columns | length == 0 %}
    -- No columns or referenced columns specified for foreign key constraint, skipping creation.
    {%- do log("No columns or referenced columns specified for foreign key constraint. Skipping creation.", info=show_info) %}
    {{ return(";") }}
{%- endif %}

{%- if is_incremental %}
    -- Incremental run, skipping foreign key constraint creation.
    {%- do log("Incremental run. Skipping foreign key constraint creation.", info=show_info) %}
    {{ return(";;") }}
{%- endif %}

{%- if columns | length == 0 != referenced_columns | length  %}

    {%- do log("Different number of columns and referenced columns specified for foreign key constraint. ", error=true) %}
    {{ return(";;;") }}
{%- endif %}

{%- set constraint_name = 'FK_' ~ relation.identifier | replace(".", "") | replace(" ", "") ~ '_dbt' %}
{%- set hashstr_md5 = local_md5((columns + referenced_columns)|join('_'))[:16] %}
{%- if use_dynamic_name %}
    {%- set final_constraint_name = constraint_name ~ "_" ~ hashstr_md5 ~ "_"  ~ dynamic_part %}
{%- else %}
    {%- set final_constraint_name = constraint_name ~ "_" ~ hashstr_md5 %}
{%- endif %}
{%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation.identifier ~ '"' %}
{%- set referenced_table = ref(referenced_table) %}
{%- set referenced_full_relation = '"' ~ referenced_table.schema ~ '"."' ~ referenced_table.identifier ~ '"' %}
{%- set relation_name = relation.schema ~ '.' ~ relation.identifier %}
{%- set referenced_relation_name = referenced_table.schema ~ '.' ~ referenced_table.identifier %}

USE [{{ relation.database }}];

{%- do log("Creating new foreign key constraint.", info=show_info) %}

BEGIN
    -- Drop specific foreign key constraint if it exists
    IF EXISTS (
        SELECT * 
        FROM sys.foreign_keys 
        WHERE parent_object_id = OBJECT_ID('{{ relation_name }}')
        AND name = '{{ final_constraint_name }}'
    )
    BEGIN
        ALTER TABLE {{ full_relation }} DROP CONSTRAINT {{ final_constraint_name }};
    END;
    -- Add the new foreign key constraint
    {# to fix bug when table is materialized and backup table have the same primary key #}
    {%- set primary_key_conflicting_table_name_var = "@conflicting_foreign_table_name" ~ hashstr_md5 %}
    {%- set foreign_key_constraint_to_rename_var = "@foreign_key_constraint_to_rename" ~ hashstr_md5 %}
    DECLARE {{primary_key_conflicting_table_name_var}} NVARCHAR(128);
    SELECT {{primary_key_conflicting_table_name_var}} = OBJECT_NAME(fk.parent_object_id)
    FROM sys.foreign_keys fk
    INNER JOIN sys.schemas sc ON sc.schema_id = fk.schema_id
    WHERE fk.name = '{{final_constraint_name}}' AND sc.name = '{{relation.schema}}';

    -- Check if the conflicting table name ends with '__dbt_backup'
    IF {{primary_key_conflicting_table_name_var}} LIKE '%__dbt_backup'
    BEGIN
        DECLARE {{foreign_key_constraint_to_rename_var}} NVARCHAR(256);
        SET {{foreign_key_constraint_to_rename_var}} = '{{relation.schema}}.{{ final_constraint_name }}';
        -- Rename the conflicting primary key constraint

        EXEC sp_rename {{foreign_key_constraint_to_rename_var}},'{{final_constraint_name ~ range(1,100)|random}}', 'OBJECT';
    END;

    ALTER TABLE {{ full_relation }}
    ADD CONSTRAINT {{ final_constraint_name }} 
    FOREIGN KEY ({{ columns | join(', ') }})
    REFERENCES {{ referenced_full_relation }} ({{ referenced_columns | join(', ') }})
    ON DELETE {{ on_delete }}
    ON UPDATE {{ on_update }};
    
END

{%- endmacro %}
