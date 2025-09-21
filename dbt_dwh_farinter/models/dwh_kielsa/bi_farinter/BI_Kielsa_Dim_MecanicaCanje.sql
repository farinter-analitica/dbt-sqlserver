{%- set unique_key_list = ["MecanicaCanje_Id", "Emp_Id"] -%}

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

WITH Dim_MecanicaCanje AS (
    SELECT --noqa: ST06
        ISNULL(MecanicaCanje_Id, 0) AS [MecanicaCanje_Id],
        ISNULL(Emp_Id, 0) AS [Emp_Id],
        ISNULL(MecanicaCanje_Nombre, 'N.D.') AS [MecanicaCanje_Nombre],
        ISNULL(MecanicaCanje_Tipo, 'N.D.') AS [MecanicaCanje_Tipo],
        EmpMecanicaCanje_Id,
        Fecha_Actualizado AS Fecha_Carga,
        Fecha_Actualizado
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_MecanicaCanje') }}
    {%- if is_incremental() %}
        WHERE Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
    {%- endif %}
)

SELECT
    MecanicaCanje_Id,
    Emp_Id,
    MecanicaCanje_Nombre,
    MecanicaCanje_Tipo,
    EmpMecanicaCanje_Id,
    Fecha_Carga,
    Fecha_Actualizado
FROM Dim_MecanicaCanje;
