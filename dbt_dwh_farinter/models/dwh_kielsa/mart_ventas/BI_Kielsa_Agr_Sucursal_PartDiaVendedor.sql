{% set unique_key_list = ["Emp_Id","Suc_Id","Vendedor_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="ignore",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}
{% set v_semanas_muestra = 12 %}
{% set v_dias_muestra = v_semanas_muestra * 7 %}
{% set v_fecha_inicio = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=v_dias_muestra)).strftime('%Y%m%d') %}
{% set v_fecha_fin = modules.datetime.datetime.now().strftime('%Y%m%d')  %}
{% set v_anio_mes_inicio =  v_fecha_inicio[:6]  %}

/*
--1. Pesos de cada dia de la semana por sucursal, valor y peso
DECLARE @Inicio AS DATE = GETDATE()
DECLARE @SemanasPonderacion AS INT = 12
DECLARE @DiasPonderacion AS INT = @SemanasPonderacion*7 --Historia para ponderar
DROP TABLE IF EXISTS #Temp
;*/
WITH ResumenBase
AS
(
    SELECT 
        Emp_Id,
        Suc_Id,
        FP.Vendedor_Id as Vendedor_Id,
        {{ v_dias_muestra }} AS Dias_Muestra,
        ISNULL(SUM(FP.Cantidad_Padre),0)*1.0 AS Sum_Cantidad_Padre,
        ISNULL(SUM(FP.Valor_Bruto),0)*1.0 AS Sum_Valor_Bruto,
        ISNULL(SUM(FP.Valor_Neto),0)*1.0 AS Sum_Valor_Neto,
        ISNULL(SUM(FP.Valor_Costo),0)*1.0 AS Sum_Valor_Costo,
        ISNULL(SUM(FP.Valor_Descuento),0)*1.0 AS Sum_Valor_Descuento,
        ISNULL(SUM(FP.Valor_Descuento_Financiero),0)*1.0 AS Sum_Valor_Descuento_Financiero,
        ISNULL(SUM(FP.Valor_Acum_Monedero),0)*1.0 AS Sum_Valor_Acum_Monedero,
        ISNULL(SUM(FP.Valor_Descuento_Cupon),0)*1.0 AS Sum_Valor_Descuento_Cupon,
        ISNULL(SUM(FP.Descuento_Proveedor),0)*1.0 AS Sum_Descuento_Proveedor,
        ISNULL(SUM(FP.Valor_Descuento_Tercera_Edad),0)*1.0 AS Sum_Valor_Descuento_Tercera_Edad,
        ISNULL(COUNT(DISTINCT FP.EmpSucDocCajFac_Id),0)*1.0 AS Sum_Conteo_Transacciones
    FROM {{ ref ('BI_Kielsa_Hecho_FacturaPosicion') }} FP 
    WHERE Factura_Fecha >= '{{ v_fecha_inicio }}' AND Factura_Fecha < '{{ v_fecha_fin }}' AND AnioMes_Id >= {{ v_anio_mes_inicio }}
    --WHERE Factura_Fecha >= DATEADD(DAY,- @DiasPonderacion, @Inicio ) AND Factura_Fecha < @inicio
    GROUP BY Emp_Id, Suc_Id, FP.Vendedor_Id
)
SELECT 
    ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Suc_Id,0) AS Suc_Id,
    ISNULL(Vendedor_Id,0) AS Vendedor_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=19, table_alias='')}} [EmpSuc_Id],
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Vendedor_Id'], input_length=29, table_alias='')}} [EmpVen_Id],
    ISNULL(COUNT(Vendedor_Id) OVER (PARTITION BY Emp_Id, Suc_Id),0) AS Vendedores_Ponderacion,
    CAST(Sum_Cantidad_Padre / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Padre,
    CAST(Sum_Valor_Bruto / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Bruto,
    CAST(Sum_Valor_Neto / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Neto,
    CAST(Sum_Valor_Costo / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Costo,
    CAST(Sum_Valor_Descuento / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento,
    CAST(Sum_Valor_Descuento_Financiero / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Financiero,
    CAST(Sum_Valor_Acum_Monedero / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Acum_Monedero,
    CAST(Sum_Valor_Descuento_Cupon / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Cupon,
    CAST(Sum_Descuento_Proveedor / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Proveedor,
    CAST(Sum_Valor_Descuento_Tercera_Edad / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Tercera_Edad,
    CAST(Sum_Conteo_Transacciones / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Transacciones,
    ISNULL(CAST(Sum_Cantidad_Padre / NULLIF(SUM(Sum_Cantidad_Padre) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Cantidad_Padre,
    ISNULL(CAST(Sum_Valor_Bruto / NULLIF(SUM(Sum_Valor_Bruto) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Bruto,
    ISNULL(CAST(Sum_Valor_Neto / NULLIF(SUM(Sum_Valor_Neto) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Neto,
    ISNULL(CAST(Sum_Valor_Costo / NULLIF(SUM(Sum_Valor_Costo) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Costo,
    ISNULL(CAST(Sum_Valor_Descuento / NULLIF(SUM(Sum_Valor_Descuento) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento,
    ISNULL(CAST(Sum_Valor_Descuento_Financiero / NULLIF(SUM(Sum_Valor_Descuento_Financiero) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento_Financiero,
    ISNULL(CAST(Sum_Valor_Acum_Monedero / NULLIF(SUM(Sum_Valor_Acum_Monedero) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Acum_Monedero,
    ISNULL(CAST(Sum_Valor_Descuento_Cupon / NULLIF(SUM(Sum_Valor_Descuento_Cupon) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento_Cupon,
    ISNULL(CAST(Sum_Descuento_Proveedor / NULLIF(SUM(Sum_Descuento_Proveedor) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento_Proveedor,
    ISNULL(CAST(Sum_Valor_Descuento_Tercera_Edad / NULLIF(SUM(Sum_Valor_Descuento_Tercera_Edad) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento_Tercera_Edad,
    ISNULL(CAST(Sum_Conteo_Transacciones / NULLIF(SUM(Sum_Conteo_Transacciones) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Conteo_Transacciones
--INTO #Temp
FROM ResumenBase


