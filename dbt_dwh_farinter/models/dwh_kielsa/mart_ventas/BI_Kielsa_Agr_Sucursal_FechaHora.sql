{% set unique_key_list = ["Factura_Fecha", "Hora_Id", "Emp_Id","Suc_Id"] %}
{{- 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
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
            if_another_exists_drop_it=true) }}",
        ]
	) 
}}
{%- if is_incremental() -%}
	{%- set v_fecha_inicio = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  
            from  """ ~ this,
            relation_not_found_value='19000101'|string)|string %}
	{%- set v_fecha_fin = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,max(Fecha_Actualizado), 112), '19000101')  
            from  """ ~ ref('BI_Kielsa_Hecho_FacturaEncabezado'),
            relation_not_found_value='19000101'|string)|string %}
    {%- set v_anio_mes_inicio =  v_fecha_inicio[:6]  %}
{%- else -%}
	{%- set v_fecha_inicio = (modules.datetime.datetime.now()-
        modules.datetime.timedelta(days=365*3)).replace(month=1, day=1).strftime('%Y%m%d') %}
    {%- set v_fecha_fin = modules.datetime.datetime.now().strftime('%Y%m%d') %}
    {%- set v_anio_mes_inicio =  v_fecha_inicio[:6]  %}
{%- endif -%}

/*

--TODO: Agregar Factura_ORIGEN EX y PU es_express
--1. Pesos de cada dia de la semana por sucursal, valor y peso
DECLARE @Inicio AS DATE = GETDATE()
DECLARE @SemanasPonderacion AS INT = 12
DECLARE @DiasPonderacion AS INT = @SemanasPonderacion*7 --Historia para ponderar
DROP TABLE IF EXISTS #Temp
;*/
WITH ResumenPosiciones
AS
(
    SELECT 
        FP.Factura_Fecha, FP.Emp_Id, FP.Suc_Id, FP.TipoDoc_id, FP.Caja_Id, FP.Factura_Id,
        MAX(FP.Hora_Id) as Hora_Id,
        ISNULL(SUM(FP.Cantidad_Padre),0.0)*1.0 AS Sum_Cantidad_Padre,
        ISNULL(COUNT(DISTINCT FP.Articulo_Id)*1.0,1.0) AS Sum_Cantidad_Articulos,
        ISNULL(SUM(FP.Valor_Bruto),0.0)*1.0 AS Sum_Valor_Bruto,
        ISNULL(SUM(FP.Valor_Neto),0.0)*1.0 AS Sum_Valor_Neto,
        ISNULL(SUM(FP.Valor_Costo),0.0)*1.0 AS Sum_Valor_Costo,
        ISNULL(SUM(FP.Valor_Descuento),0.0)*1.0 AS Sum_Valor_Descuento,
        ISNULL(SUM(FP.Valor_Descuento_Financiero),0.0)*1.0 AS Sum_Valor_Descuento_Financiero,
        ISNULL(SUM(FP.Valor_Acum_Monedero),0.0)*1.0 AS Sum_Valor_Acum_Monedero,
        ISNULL(SUM(FP.Valor_Descuento_Cupon),0.0)*1.0 AS Sum_Valor_Descuento_Cupon,
        ISNULL(SUM(FP.Descuento_Proveedor),0.0)*1.0 AS Sum_Valor_Descuento_Proveedor,
        ISNULL(SUM(FP.Valor_Descuento_Tercera_Edad),0.0)*1.0 AS Sum_Valor_Descuento_Tercera_Edad,
        ISNULL(COUNT(DISTINCT FP.EmpSucDocCajFac_Id),0.0)*1.0 AS Sum_Conteo_Transacciones,
        ISNULL(MAX(CASE WHEN A.DeptoArt_Nombre LIKE '%FARMA%'
                THEN 1 ELSE 0 END),0.0)*1.0 AS contiene_farma,
        ISNULL(AVG(FP.Detalle_Precio_Unitario),0.0)*1.0 AS precio_unitario_promedio
    FROM BI_FARINTER.dbo.BI_Kielsa_Hecho_FacturaPosicion FP -- {{ ref ('BI_Kielsa_Hecho_FacturaPosicion') }} FP 
    INNER JOIN {{ ref('BI_Kielsa_Dim_Articulo') }} A
    ON FP.Articulo_Id = A.Articulo_Id
    AND FP.Emp_Id = A.Emp_Id
    WHERE FP.Factura_Fecha >= '{{ v_fecha_inicio }}' 
    AND FP.Factura_Fecha < '{{ v_fecha_fin }}' 
    AND FP.AnioMes_Id >= {{ v_anio_mes_inicio }}
    GROUP BY FP.Factura_Fecha, FP.Emp_Id, FP.Suc_Id, FP.TipoDoc_id, FP.Caja_Id, FP.Factura_Id
),
ResumenFinal AS (
    SELECT 
        FE.Factura_Fecha,
        FE.Hora_Id,
        FE.Emp_Id,
        FE.Suc_Id,
        ISNULL(MIN(MIN(FE.Factura_Hora)) 
            OVER(PARTITION BY 
                FE.Factura_Fecha,
                FE.Emp_Id,
                FE.Suc_Id),'00:00') AS Min_Hora_Dia,
        ISNULL(MAX(MAX(FE.Factura_Hora))
            OVER(PARTITION BY 
                FE.Factura_Fecha,
                FE.Emp_Id,
                FE.Suc_Id),'23:59') AS Max_Hora_Dia,
        ISNULL(MIN(FE.Factura_Hora),'00:00') AS Min_Hora,
        ISNULL(MAX(FE.Factura_Hora),'23:59') AS Max_Hora,
        ISNULL(SUM(FP.Sum_Cantidad_Padre),0.0)*1.0 AS Sum_Cantidad_Padre,
        ISNULL(SUM(FP.Sum_Cantidad_Articulos),0.0)*1.0 AS Sum_Cantidad_Articulos,
        ISNULL(SUM(FP.Sum_Valor_Bruto),0.0)*1.0 AS Sum_Valor_Bruto,
        ISNULL(SUM(FP.Sum_Valor_Neto),0.0)*1.0 AS Sum_Valor_Neto,
        ISNULL(SUM(FP.Sum_Valor_Costo),0.0)*1.0 AS Sum_Valor_Costo,
        ISNULL(SUM(FP.Sum_Valor_Descuento),0.0)*1.0 AS Sum_Valor_Descuento,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Financiero),0.0)*1.0 AS Sum_Valor_Descuento_Financiero,
        ISNULL(SUM(FP.Sum_Valor_Acum_Monedero),0.0)*1.0 AS Sum_Valor_Acum_Monedero,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Cupon),0.0)*1.0 AS Sum_Valor_Descuento_Cupon,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Proveedor),0.0)*1.0 AS Sum_Valor_Descuento_Proveedor,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Tercera_Edad),0.0)*1.0 AS Sum_Valor_Descuento_Tercera_Edad,
        ISNULL(COUNT(DISTINCT FE.EmpSucDocCajFac_Id),0.0)*1.0 AS Sum_Conteo_Transacciones,
        ISNULL(SUM(FP.precio_unitario_promedio),0.0)*1.0 AS Sum_Valor_Precio_Unitario_Promedio,
        ISNULL(AVG(FP.precio_unitario_promedio),0.0)*1.0 AS Valor_Precio_Unitario_Promedio,
        ISNULL(SUM(CASE WHEN TC.TipoCliente_Nombre LIKE '%TER%EDAD%' 
                OR TC.TipoCliente_Nombre LIKE '%CUART%EDAD%'
                OR M.Tipo_Plan LIKE '%TER%EDAD%' 
                OR M.Tipo_Plan LIKE '%CUART%EDAD%'
            THEN 1 ELSE 0 END),0.0)*1.0 AS Sum_Conteo_Trx_Es_Tercera_Edad,
        ISNULL(SUM(CASE WHEN TC.TipoCliente_Nombre LIKE '%ASEGURADO%' 
                OR M.Tipo_Plan LIKE '%ASEGURADO%' 
            THEN 1 ELSE 0 END),0.0)*1.0 AS Sum_Conteo_Trx_Es_Asegurado,
        ISNULL(SUM(CASE WHEN FE.Valor_Acum_Monedero > 0 
            THEN 1 ELSE 0 END),0.0)*1.0 AS Sum_Conteo_Trx_Acumula_Monedero,
        ISNULL(SUM(CAST(FP.contiene_farma AS INT)),0.0)*1.0 AS Sum_Conteo_Trx_Contiene_Farma
    FROM {{ ref('BI_Kielsa_Hecho_FacturaEncabezado') }} FE
    INNER JOIN ResumenPosiciones FP
    ON FE.Emp_Id = FP.Emp_Id
    AND FE.Suc_Id = FP.Suc_Id
    AND FE.TipoDoc_id = FP.TipoDoc_id
    AND FE.Caja_Id = FP.Caja_Id
    AND FE.Factura_Id = FP.Factura_Id
    AND FE.Factura_Fecha = FP.Factura_Fecha
    INNER JOIN {{ ref('BI_Kielsa_Dim_Sucursal') }} S
    ON FE.Emp_Id = S.Emp_Id
    AND FE.Suc_Id = S.Sucursal_Id
    INNER JOIN {{ ref('BI_Kielsa_Dim_Monedero') }} M
    ON FE.Emp_Id = M.Emp_Id
    AND FE.Monedero_Id = M.Monedero_Id
    INNER JOIN {{ ref('BI_Kielsa_Dim_Cliente') }} C
    ON FE.Emp_Id = C.Emp_Id
    AND FE.Cliente_Id = C.Cliente_Id
    INNER JOIN {{ ref('BI_Kielsa_Dim_TipoCliente') }} TC
    ON FE.Emp_Id = TC.Emp_Id
    AND C.Tipo_Cliente_Id = TC.TipoCliente_Id
    WHERE FE.Factura_Fecha >= '{{ v_fecha_inicio }}' 
    AND FE.Factura_Fecha < '{{ v_fecha_fin }}' 
    AND FE.AnioMes_Id >= {{ v_anio_mes_inicio }}
    GROUP BY FE.Factura_Fecha, FE.Hora_Id, FE.Emp_Id, FE.Suc_Id
),
    MetricasCompletas AS (
    SELECT 
        ISNULL(Emp_Id,0) AS Emp_Id,
        ISNULL(Suc_Id,0) AS Suc_Id,
        ISNULL(Factura_Fecha,'19000101') AS Factura_Fecha,
        ISNULL(Hora_Id,0) AS Hora_Id,
        {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=19, table_alias='')}} [EmpSuc_Id],
        Min_Hora_Dia,
        Max_Hora_Dia,
        Min_Hora,
        Max_Hora,
        Sum_Cantidad_Padre,
        Sum_Cantidad_Articulos,
        Sum_Valor_Bruto,
        Sum_Valor_Neto,
        Sum_Valor_Costo,
        Sum_Valor_Descuento,
        Sum_Valor_Descuento_Financiero,
        Sum_Valor_Acum_Monedero,
        Sum_Valor_Descuento_Cupon,
        Sum_Valor_Descuento_Proveedor,
        Sum_Valor_Descuento_Tercera_Edad,
        Sum_Conteo_Transacciones,
        Sum_Valor_Precio_Unitario_Promedio,
        Valor_Precio_Unitario_Promedio,
        Sum_Conteo_Trx_Es_Tercera_Edad,
        Sum_Conteo_Trx_Es_Asegurado,
        Sum_Conteo_Trx_Acumula_Monedero,
        Sum_Conteo_Trx_Contiene_Farma,
        CASE WHEN (ABS(Sum_Cantidad_Padre) - Sum_Cantidad_Articulos) < 0
            THEN 0.0 ELSE ABS(Sum_Cantidad_Padre) - Sum_Cantidad_Articulos END AS Sum_Cantidad_Unidades_Relativa
        {# TRY_CAST(Sum_Cantidad_Padre / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Cantidad_Padre,
        TRY_CAST(Sum_Cantidad_Articulos / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Cantidad_Articulos,
        TRY_CAST(Sum_Valor_Bruto / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Bruto,
        TRY_CAST(Sum_Valor_Neto / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Neto,
        TRY_CAST(Sum_Valor_Costo / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Costo,
        TRY_CAST(Sum_Valor_Descuento / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Descuento,
        TRY_CAST(Sum_Valor_Descuento_Financiero / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Financiero,
        TRY_CAST(Sum_Valor_Acum_Monedero / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Acum_Monedero,
        TRY_CAST(Sum_Valor_Descuento_Cupon / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Cupon,
        TRY_CAST(Sum_Valor_Descuento_Proveedor / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Proveedor,
        TRY_CAST(Sum_Valor_Descuento_Tercera_Edad / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Tercera_Edad,
        TRY_CAST(Sum_Conteo_Transacciones / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Conteo_Transacciones,
        TRY_CAST(Sum_Valor_Precio_Unitario_Promedio / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Valor_Precio_Unitario_Promedio,
        TRY_CAST(Sum_Conteo_Trx_Es_Tercera_Edad / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Es_Tercera_Edad,
        TRY_CAST(Sum_Conteo_Trx_Es_Asegurado / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Es_Asegurado,
        TRY_CAST(Sum_Conteo_Trx_Acumula_Monedero / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Acumula_Monedero,
        TRY_CAST(Sum_Conteo_Trx_Contiene_Farma / (DATEDIFF(HOUR, Min_Hora_Dia, Max_Hora_Dia) + 1.0) AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Contiene_Farma, #}
        {# TRY_CAST(Sum_Cantidad_Padre / NULLIF(SUM(Sum_Cantidad_Padre) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Cantidad_Padre,
        TRY_CAST(Sum_Cantidad_Articulos / NULLIF(SUM(Sum_Cantidad_Articulos) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Cantidad_Articulos,
        TRY_CAST(Sum_Valor_Bruto / NULLIF(SUM(Sum_Valor_Bruto) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Bruto,
        TRY_CAST(Sum_Valor_Neto / NULLIF(SUM(Sum_Valor_Neto) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Neto,
        TRY_CAST(Sum_Valor_Costo / NULLIF(SUM(Sum_Valor_Costo) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Costo,
        TRY_CAST(Sum_Valor_Descuento / NULLIF(SUM(Sum_Valor_Descuento) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Descuento,
        TRY_CAST(Sum_Valor_Descuento_Financiero / NULLIF(SUM(Sum_Valor_Descuento_Financiero) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Descuento_Financiero,
        TRY_CAST(Sum_Valor_Acum_Monedero / NULLIF(SUM(Sum_Valor_Acum_Monedero) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Acum_Monedero,
        TRY_CAST(Sum_Valor_Descuento_Cupon / NULLIF(SUM(Sum_Valor_Descuento_Cupon) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Descuento_Cupon,
        TRY_CAST(Sum_Valor_Descuento_Proveedor / NULLIF(SUM(Sum_Valor_Descuento_Proveedor) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Descuento_Proveedor,
        TRY_CAST(Sum_Valor_Descuento_Tercera_Edad / NULLIF(SUM(Sum_Valor_Descuento_Tercera_Edad) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Descuento_Tercera_Edad,
        TRY_CAST(Sum_Conteo_Transacciones / NULLIF(SUM(Sum_Conteo_Transacciones) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Conteo_Transacciones,
        TRY_CAST(Sum_Valor_Precio_Unitario_Promedio / NULLIF(SUM(Sum_Valor_Precio_Unitario_Promedio) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Valor_Precio_Unitario_Promedio,
        TRY_CAST(Sum_Conteo_Trx_Es_Tercera_Edad / NULLIF(SUM(Sum_Conteo_Trx_Es_Tercera_Edad) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Conteo_Trx_Es_Tercera_Edad,
        TRY_CAST(Sum_Conteo_Trx_Es_Asegurado / NULLIF(SUM(Sum_Conteo_Trx_Es_Asegurado) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Conteo_Trx_Es_Asegurado,
        TRY_CAST(Sum_Conteo_Trx_Acumula_Monedero / NULLIF(SUM(Sum_Conteo_Trx_Acumula_Monedero) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Conteo_Trx_Acumula_Monedero,
        TRY_CAST(Sum_Conteo_Trx_Contiene_Farma / NULLIF(SUM(Sum_Conteo_Trx_Contiene_Farma) OVER(PARTITION BY Emp_Id, Suc_Id, Factura_Fecha),0) AS DECIMAL(16,12)) AS Part_Conteo_Trx_Contiene_Farma #}
    FROM ResumenFinal
    )
SELECT MET.*,
    CAST(COEF.intercepto * Sum_Conteo_Transacciones + 
        COEF.log_cantidad_productos * LOG(ABS(Sum_Cantidad_Articulos)+1) +
        COEF.contiene_farma_1 * Sum_Conteo_Trx_Contiene_Farma +
        COEF.es_tercera_edad_1 * Sum_Conteo_Trx_Es_Tercera_Edad +
        COEF.log_cantidad_unidades_relativa * LOG(ABS(Sum_Cantidad_Unidades_Relativa)+1) +
        COEF.log_precio_unitario_prom * LOG(Sum_Valor_Precio_Unitario_Promedio+1) +
        COEF.acumula_monedero_1 * Sum_Conteo_Trx_Acumula_Monedero +
        COEF.es_asegurado_1 * Sum_Conteo_Trx_Es_Asegurado AS DECIMAL(16,2)) 
        AS Sum_Segundos_Transaccion_Estimado,
    GETDATE() AS Fecha_Actualizado
FROM MetricasCompletas MET
CROSS APPLY (
    SELECT 
        MAX(CASE WHEN variable = 'intercepto' THEN coeficiente_ajustado ELSE 0.0 END) AS intercepto,
        MAX(CASE WHEN variable = 'log_cantidad_productos' THEN coeficiente_ajustado ELSE 0.0 END) AS log_cantidad_productos,
        MAX(CASE WHEN variable = 'contiene_farma_1' THEN coeficiente_ajustado ELSE 0.0 END) AS contiene_farma_1,
        MAX(CASE WHEN variable = 'es_tercera_edad_1' THEN coeficiente_ajustado ELSE 0.0 END) AS es_tercera_edad_1,
        MAX(CASE WHEN variable = 'log_cantidad_unidades_relativa' THEN coeficiente_ajustado ELSE 0.0 END) AS log_cantidad_unidades_relativa,
        MAX(CASE WHEN variable = 'log_precio_unitario_prom' THEN coeficiente_ajustado ELSE 0.0 END) AS log_precio_unitario_prom,
        MAX(CASE WHEN variable = 'acumula_monedero_1' THEN coeficiente_ajustado ELSE 0.0 END) AS acumula_monedero_1,
        MAX(CASE WHEN variable = 'es_asegurado_1' THEN coeficiente_ajustado ELSE 0.0 END) AS es_asegurado_1
    FROM [DL_FARINTER].[nocodb_data_gf].[kielsa_tiempo_transaccion_coeficiente]
) COEF