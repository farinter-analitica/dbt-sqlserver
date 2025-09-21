{%- set unique_key_list = ["Fecha_Id", "Vendedor_Id", "Articulo_Id", "Suc_Id", "Emp_Id"] -%}

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
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", 
                create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
    ) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Id)), 112), '19000101') as fecha_a from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '20180101' %}
{%- endif %}

-- Genera los registros base que permiten unir las métricas de incentivos
-- con las reglas configuradas según rol, usuario/vendedor y sucursal(es)

WITH ComisionAgrupadaVenArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_comision_emp_suc_art_ven_fch') }}
    WHERE Fecha_Id >= '{{ last_date }}'
),

RegaliaAgrupadaVenArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_regalia_emp_suc_art_ven_fch') }}
    WHERE Fecha_Id >= '{{ last_date }}'
),

FacturasAgrupadaVenArt AS (
    SELECT * FROM {{ ref('dlv_kielsa_stg_factura_articulo_emp_suc_ven_fch') }}
    WHERE Fecha_Id >= '{{ last_date }}'
),

Vertebra AS (
    SELECT
        Fecha_Id,
        Vendedor_Id,
        Articulo_Id,
        Suc_Id,
        Emp_Id
    FROM ComisionAgrupadaVenArt
    UNION ALL
    SELECT
        Fecha_Id,
        Vendedor_Id,
        Articulo_Id,
        Suc_Id,
        Emp_Id
    FROM RegaliaAgrupadaVenArt
    UNION ALL
    SELECT
        Fecha_Id,
        Vendedor_Id,
        Articulo_Id,
        Suc_Id,
        Emp_Id
    FROM FacturasAgrupadaVenArt
),

Final AS (
    SELECT DISTINCT *
    FROM Vertebra
)

SELECT * FROM Final
