{%- set unique_key_list = ["Fecha_Id", "Articulo_Id", "Suc_Id", "Emp_Id"] -%}

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
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '20180101' %}
{%- endif %}

-- Staging: Comisiones agrupadas por Emp, Suc, Art, Fecha
WITH BaseComision AS (
    SELECT
        ISNULL(CB.Emp_Id, 0) AS Emp_Id,
        ISNULL(CB.Suc_Id, 0) AS Suc_Id,
        ISNULL(CB.Articulo_Id, '') AS Articulo_Id,
        COUNT(DISTINCT CB.Vendedor_Id) AS Cantidad_Vendedores,
        ISNULL(CAST(CB.Comision_Fecha AS DATE), '19000101') AS Fecha_Id,
        SUM(CB.Comision_Total) AS Comision_Total,
        SUM(CB.Comision_CantArticulo) AS Comision_CantArticulo,
        SUM(CB.Cantidad_Padre) AS Cantidad_Padre,
        MAX(CB.Fecha_Actualizado) AS Fecha_Actualizado
    FROM {{ ref('BI_Kielsa_Hecho_Comision') }} AS CB
    WHERE CB.Comision_Fecha >= '{{ last_date }}'
    GROUP BY CAST(CB.Comision_Fecha AS DATE), CB.Articulo_Id, CB.Suc_Id, CB.Emp_Id
)

SELECT * FROM BaseComision
