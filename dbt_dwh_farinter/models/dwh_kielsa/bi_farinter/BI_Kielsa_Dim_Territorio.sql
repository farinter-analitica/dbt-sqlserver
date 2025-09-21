{%- set unique_key_list = [
    "Emp_Id",
    "Departamento_Id",
    "Municipio_Id",
    "Ciudad_Id"
] -%}

{{
    config(
        as_columnstore=true,
        tags=["automation/eager", "automation_only"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ",
                create_clustered=false,
                is_incremental=is_incremental(),
                if_another_exists_drop_it=true)
            }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
    )
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this,
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

WITH UB AS (
    SELECT
        DEP.Emp_Id,
        DEP.Departamento_Id,
        DEP.Departamento_Nombre,
        MUN.Municipio_Id,
        MUN.Municipio_Nombre,
        CIU.Ciudad_Id,
        CIU.Ciudad_Nombre,
        -- Timestamps: take the latest across available levels
        COALESCE(CIU.Fecha_Actualizado, MUN.Fecha_Actualizado, DEP.Fecha_Actualizado) AS Fecha_Actualizado,
        COALESCE(CIU.Fecha_Carga, MUN.Fecha_Carga, DEP.Fecha_Carga) AS Fecha_Carga
    FROM {{ ref('DL_Kielsa_Departamento') }} AS DEP
    LEFT JOIN {{ ref('DL_Kielsa_Municipio') }} AS MUN
        ON
            DEP.Departamento_Id = MUN.Departamento_Id
            AND DEP.Emp_Id = MUN.Emp_Id
    LEFT JOIN {{ ref('DL_Kielsa_Ciudad') }} AS CIU
        ON
            MUN.Municipio_Id = CIU.Municipio_Id
            AND MUN.Departamento_Id = CIU.Departamento_Id
            AND MUN.Emp_Id = CIU.Emp_Id
    {%- if is_incremental() %}
        WHERE
            DEP.Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
            OR MUN.Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
            OR CIU.Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
    {%- endif %}
)

SELECT
    ISNULL(Emp_Id, 0) AS Emp_Id,
    ISNULL(Departamento_Id, 0) AS Departamento_Id,
    ISNULL(TRIM(UPPER(Departamento_Nombre)), 'N.D.') AS Departamento_Nombre,
    ISNULL(Municipio_Id, 0) AS Municipio_Id,
    ISNULL(TRIM(UPPER(Municipio_Nombre)), 'N.D.') AS Municipio_Nombre,
    ISNULL(Ciudad_Id, 0) AS Ciudad_Id,
    ISNULL(TRIM(UPPER(Ciudad_Nombre)), 'N.D.') AS Ciudad_Nombre,
    {{ dwh_farinter_concat_key_columns(
        columns=['Emp_Id','Departamento_Id','Municipio_Id','Ciudad_Id'],
        input_length=120) }}
        AS [EmpDepMunCiu_Id],
    Fecha_Carga,
    Fecha_Actualizado
FROM UB
