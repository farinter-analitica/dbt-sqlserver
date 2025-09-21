{% set unique_key_list = ["Factura_Fecha","Emp_Id","Suc_Id","Articulo_Id","Factura_Id","TipoDocumento_Id"] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}

SELECT --TOP 100
    -- Key columns for grouping and matching
    ISNULL(FP.Factura_Fecha, '19000101') AS Factura_Fecha, -- Fecha_Id,
    ISNULL(FP.Emp_Id, 0) AS Emp_Id,
    ISNULL(FP.Pais_Id, 0) AS Pais_Id,
    ISNULL(FP.Suc_Id, 0) AS Suc_Id,
    ISNULL(CAST(CONCAT(FP.Emp_Id, '-', FP.Suc_Id) AS VARCHAR(50)), 'X') AS EmpSuc_Id, -- Sucursal_Id,
    ISNULL(FP.Articulo_Id, 0) AS Articulo_Id,
    ISNULL(CAST(CONCAT(FP.Emp_Id, '-', FP.Articulo_Id) AS VARCHAR(50)), 'X') AS EmpArticulo_Id, -- Articulo_Id,
    ISNULL(CONCAT(FP.Emp_Id, '-', MAX(ART.Articulo_Codigo_Padre)), 'X') AS EmpArticuloPadre_Id, -- ArticuloPadre_Id,
    ISNULL(FP.Factura_Id, 0) AS Factura_Id,
    ISNULL(MAX(FP.EmpSucDocCajFac_Id), 0) AS EmpSucDocCajFac_Id,
    ISNULL(FP.TipoDoc_Id, 0) AS TipoDocumento_Id, -- Documento_Id,
    -- Non-key columns
    ISNULL(MAX(ART.Articulo_Codigo_Padre), 'X') AS ArticuloPadre_Id,
    MIN(FP.AnioMes_Id) AS AnioMes_Id,
    MIN(FP.Hora_Id) AS Hora_Id,
    YEAR(MAX(FP.Factura_Fecha)) AS Anio_Id,
    MONTH(MAX(FP.Factura_Fecha)) AS Mes_Id,
    MAX(FP.Monedero_Id) AS Monedero_Id,
    MAX(FP.Cliente_Id) AS Cliente_Id,
    MAX(FP.Vendedor_Id) AS Empleado_Id,
    MAX(FP.SubDoc_Id) AS SubDocumento_Id,
    MAX(FP.CanalVenta_Id) AS CanalVenta_Id,
    MAX(FP.Factura_Estado) AS FacturaEstado_Id,
    -- Aggregated measures
    SUM(FP.Detalle_Cantidad) AS Cantidad,
    SUM(FP.Cantidad_Padre) AS Cantidad_Padre,
    SUM(FP.Valor_Bruto) AS Valor_Bruto, -- Venta_Bruta,
    SUM(FP.Valor_Neto) AS Valor_Neto, -- Venta_Neta,
    SUM(FP.Valor_Utilidad) AS Valor_Utilidad, -- Utilidad,
    SUM(FP.Valor_Costo) AS Valor_Costo,  -- Costo,
    SUM(FP.Valor_Descuento) AS Valor_Descuento,  -- Descuento
    MAX(FP.Fecha_Actualizado) AS Fecha_Actualizado
FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FP -- {{ ref('BI_Kielsa_Hecho_FacturaPosicion') }}
LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo ART
    ON
        FP.Emp_Id = ART.Emp_Id
        AND FP.Articulo_Id = ART.Articulo_Id
LEFT JOIN (
    SELECT DISTINCT
        FP.Emp_Id,
        FP.Factura_Id,
        FP.Suc_Id,
        FP.TipoDoc_Id,
        FP.Caja_Id
    FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FP -- {{ ref('BI_Kielsa_Hecho_FacturaPosicion') }}
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo ART -- {{ ref('BI_Kielsa_Dim_Articulo') }}
        ON
            FP.Emp_Id = ART.Emp_Id
            AND FP.Articulo_Id = ART.Articulo_Id
    INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Cliente CLI -- {{ ref('BI_Kielsa_Dim_Cliente') }}
        ON
            FP.Emp_Id = CLI.Emp_Id
            AND FP.Cliente_Id = CLI.Cliente_Id
    WHERE
        FP.Emp_Id = 1
        {% if is_incremental() %}
            --Se hace aqui por si solo una posición se editó.
            AND FP.Fecha_Actualizado >= '{{ last_date }}'
        {% endif %} 
        AND (
            ART.Categoria_Id = '6'
            OR CLI.Tipo_Cliente LIKE '%CLINICA%PLAN%'
            OR CLI.Tipo_Cliente LIKE '%CLINICA%LAB%'
        )
) SubFP
    ON
        FP.Factura_Id = SubFP.Factura_Id
        AND FP.Emp_Id = SubFP.Emp_Id
        AND FP.Suc_Id = SubFP.Suc_Id
        AND FP.TipoDoc_Id = SubFP.TipoDoc_Id
        AND FP.Caja_Id = SubFP.Caja_Id
{% if is_incremental() %}
    WHERE
        FP.Factura_Fecha >= DATEADD(MONTH, -2, '{{ last_date }}')
        AND FP.Emp_Id = 1
        AND SubFP.Factura_Id IS NOT NULL
{% else %}
WHERE FP.Factura_Fecha >= DATEADD(YEAR, -3, GETDATE()) -- @FechaInicio
    AND FP.Emp_Id = 1
    AND  SubFP.Factura_Id IS NOT NULL
{% endif %}
GROUP BY
    FP.Factura_Fecha,
    FP.Emp_Id,
    FP.Pais_Id,
    FP.Suc_Id,
    FP.Articulo_Id,
    FP.Factura_Id,
    FP.AnioMes_Id,
    FP.TipoDoc_Id
