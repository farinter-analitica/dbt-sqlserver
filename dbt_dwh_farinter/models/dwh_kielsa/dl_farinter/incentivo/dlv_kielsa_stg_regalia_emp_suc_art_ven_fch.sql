{%- set unique_key_list = ["Fecha_Id", "Vendedor_Id","Articulo_Id","Suc_Id","Emp_Id"] -%}

{{ 
    config(
        as_columnstore=true,
        tags=["periodo/diario", "automation/periodo_por_hora"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        ]
    ) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '20180101' %}
{%- endif %}

-- Staging: Regalias agrupadas por Emp, Suc, Art, Ven, Fecha
WITH BaseComision AS (
    SELECT
        ISNULL(RD.Emp_Id, 0) AS Emp_Id,
        ISNULL(RD.Suc_Id, 0) AS Suc_Id,
        ISNULL(RD.Articulo_Id, 'X') AS Articulo_Id,
        ISNULL(RD.Vendedor_Id, 0) AS Vendedor_Id,
        ISNULL(CAST(RD.Detalle_Fecha AS DATE), '19000101') AS Fecha_Id,
        SUM(RD.Cantidad_Original) AS Cantidad_Original,
        SUM(RD.Cantidad_Padre) AS Cantidad_Padre,
        MAX(RD.Fecha_Actualizado) AS Fecha_Actualizado
    FROM {{ ref('BI_Kielsa_Hecho_Regalia_Detalle') }} AS RD
    WHERE RD.Detalle_Fecha >= '{{ last_date }}'
    GROUP BY CAST(RD.Detalle_Fecha AS DATE), RD.Articulo_Id, RD.Suc_Id, RD.Emp_Id, RD.Vendedor_Id
)

SELECT * FROM BaseComision
