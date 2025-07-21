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
    {%- set last_date = (modules.datetime.datetime.now() - 
        modules.datetime.timedelta(days=366)).replace(month=1, day=1).strftime('%Y%m%d') %}
{%- endif %}

-- Staging: Facturas de aseguradoras agrupadas por Emp, Suc, Vendedor, Fecha
WITH BaseFacturaAsegurada AS (
    SELECT
        ISNULL(FP.Emp_Id, 0) AS Emp_Id,
        ISNULL(FP.Suc_Id, 0) AS Suc_Id,
        ISNULL(FP.Articulo_Id, 'X') AS Articulo_Id,
        ISNULL(CAST(FP.Factura_Fecha AS DATE), '19000101') AS Fecha_Id,
        SUM(FP.Cantidad_Padre) AS Cantidad_Padre,
        SUM(FP.Detalle_Cantidad) AS Cantidad_Original,
        SUM(FP.Valor_Utilidad) AS Valor_Utilidad,
        SUM(FP.Descuento_Proveedor) AS Valor_Descuento_Proveedor,
        SUM(FP.Valor_Neto) AS Valor_Venta_Neta,
        MAX(FP.Fecha_Actualizado) AS Fecha_Actualizado
    FROM {{ ref('BI_Kielsa_Hecho_FacturaPosicion') }} AS FP
    WHERE
        FP.Factura_Fecha >= '{{ last_date }}'
    GROUP BY CAST(FP.Factura_Fecha AS DATE), FP.Articulo_Id, FP.Suc_Id, FP.Emp_Id
)

SELECT * FROM BaseFacturaAsegurada
