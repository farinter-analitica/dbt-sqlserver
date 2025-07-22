{%- set unique_key_list = ["Fecha_Id", "Suc_Id", "Emp_Id"] -%}

{{ 
    config(
        as_columnstore=true,
        tags=["periodo/diario", "automation/periodo_por_hora"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        merge_insert_only=true,
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, 
                is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
    ) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Id)), 112), '19000101')  from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '20180101' %}
{%- endif %}

-- Genera los registros base que permiten unir las métricas de incentivos
-- con las reglas configuradas según rol, usuario/vendedor y sucursal(es)

WITH
FacturasAgrupada AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_factura_encabezado_emp_suc_fch') }}
    WHERE Fecha_Id >= '{{ last_date }}'
),

Vertebra AS (
    SELECT
        Fecha_Id,
        Suc_Id,
        Emp_Id
    FROM FacturasAgrupada
),

Final AS (
    SELECT DISTINCT *
    FROM Vertebra
)

SELECT * FROM Final
