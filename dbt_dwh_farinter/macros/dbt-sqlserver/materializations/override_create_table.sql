
{% macro sqlserver__create_table_as(temporary, relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    {% set tmp_relation = relation.incorporate(
    path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
    type='view')-%}
    {% do run_query(fabric__get_drop_sql(tmp_relation)) %}

    {% set contract_config = config.get('contract') %}
    {% set unique_keys = config.get('unique_key') %}
    {% do run_query(fabric__create_view_as(tmp_relation, sql)) %}
    {#{{ fabric__create_view_as(tmp_relation, sql) }}#}

    {% if contract_config.enforced%}
        {% set listColumns %}
            {% for column in model['columns']  %}
                {{ "["~column~"]" }}{{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}
    {% else %}
        {% set listColumns %}
            {% for column in adapter.get_columns_in_relation(tmp_relation)  %}
                [{{column.name}}] {{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}
    {% endif %}
    

    {% set tmp_sql %}
            SELECT {{listColumns}} FROM [{{tmp_relation.database}}].[{{tmp_relation.schema}}].[{{tmp_relation.identifier}}]
    {% endset %}

    {% if contract_config.enforced %}

        CREATE TABLE [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}]
        {{ fabric__table_columns_and_constraints(relation) }}
        {{ get_assert_columns_equivalent(sql)  }}
        {%if config.get('on_clause_filegroup') %} ON {{ config.get('on_clause_filegroup') }} {%endif%}

        INSERT INTO [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}]
        ({{listColumns}}) {{tmp_sql}};

    {%- else %}
        EXEC('SELECT * INTO [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}] FROM [{{tmp_relation.database}}].[{{tmp_relation.schema}}].[{{tmp_relation.identifier}}] WHERE 1=0;');

        {% if config.get('on_clause_filegroup') %}
            {{dwh_farinter_create_index(relation=relation,columns=unique_keys,is_incremental=0, create_clustered=true)}};
            {{dwh_farinter_create_index(relation=relation,columns=unique_keys,is_incremental=0, just_drop_index=true)}};
        {% endif %}

        INSERT INTO [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}]
        SELECT * FROM [{{tmp_relation.database}}].[{{tmp_relation.schema}}].[{{tmp_relation.identifier}}];;
    {% endif %}

    {{fabric__get_drop_sql(tmp_relation)}}

    {#{% do run_query(fabric__get_drop_sql(tmp_relation)) %}#}

{% endmacro %}
