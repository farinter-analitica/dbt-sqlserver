{%- set unique_key_list = ["Fecha_Id", "Vendedor_Id", "Suc_Id", "Emp_Id"] -%}

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

-- Staging: Facturas de aseguradoras agrupadas por Emp, Suc, Vendedor, Fecha
WITH BaseFacturaAsegurada AS (
    SELECT
        ISNULL(FE.Emp_Id, 0) AS Emp_Id,
        ISNULL(FE.Suc_Id, 0) AS Suc_Id,
        ISNULL(FE.Vendedor_Id, 0) AS Vendedor_Id,
        COUNT(CASE WHEN TC.TipoCliente_Nombre LIKE '%ASEGURADO%' THEN C.Cliente_Id END) AS Cantidad_Clientes_Asegurados,
        ISNULL(CAST(FE.Factura_Fecha AS DATE), '19000101') AS Fecha_Id,
        SUM(CASE WHEN TC.TipoCliente_Nombre LIKE '%ASEGURADO%' THEN 1 ELSE 0 END) AS Cantidad_Facturas_Aseguradas,
        MAX(FE.Fecha_Actualizado) AS Fecha_Actualizado
    FROM {{ ref('BI_Kielsa_Hecho_FacturaEncabezado') }} AS FE
    INNER JOIN {{ ref('BI_Kielsa_Dim_Cliente') }} AS C
        ON
            FE.Emp_Id = C.Emp_Id
            AND FE.Cliente_Id = C.Cliente_Id
    INNER JOIN {{ ref('BI_Kielsa_Dim_TipoCliente') }} AS TC
        ON
            FE.Emp_Id = TC.Emp_Id
            AND C.Tipo_Cliente_Id = TC.TipoCliente_Id
    WHERE
        FE.Factura_Fecha >= '{{ last_date }}'
    GROUP BY CAST(FE.Factura_Fecha AS DATE), FE.Vendedor_Id, FE.Suc_Id, FE.Emp_Id
)

SELECT * FROM BaseFacturaAsegurada
