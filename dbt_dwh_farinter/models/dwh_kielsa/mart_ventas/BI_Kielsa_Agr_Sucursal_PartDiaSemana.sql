{% set unique_key_list = ["Emp_Id","Suc_Id","Dia_Semana_Iso_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}
{% set v_semanas_muestra = 12 %}
{% set v_dias_ponderacion = v_semanas_muestra * 7 %}
{% set v_fecha_inicio = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=v_dias_ponderacion)).strftime('%Y%m%d') %}
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
        FP.Emp_Id,
        FP.Suc_Id,
        C.Dia_de_la_Semana as Dia_Semana_Iso_Id,
        --@SemanasPonderacion AS Semanas_Muestra,
        COUNT(DISTINCT C.Dia_de_la_Semana)*1.0 AS Semanas_Muestra,
        ISNULL(SUM(FP.Sum_Cantidad_Padre),0)*1.0 AS Sum_Cantidad_Padre,
        ISNULL(SUM(FP.Sum_Cantidad_Articulos),0)*1.0 AS Sum_Cantidad_Articulos,
        ISNULL(SUM(FP.Sum_Valor_Bruto),0)*1.0 AS Sum_Valor_Bruto,
        ISNULL(SUM(FP.Sum_Valor_Neto),0)*1.0 AS Sum_Valor_Neto,
        ISNULL(SUM(FP.Sum_Valor_Costo),0)*1.0 AS Sum_Valor_Costo,
        ISNULL(SUM(FP.Sum_Valor_Descuento),0)*1.0 AS Sum_Valor_Descuento,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Financiero),0)*1.0 AS Sum_Valor_Descuento_Financiero,
        ISNULL(SUM(FP.Sum_Valor_Acum_Monedero),0)*1.0 AS Sum_Valor_Acum_Monedero,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Cupon),0)*1.0 AS Sum_Valor_Descuento_Cupon,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Proveedor),0)*1.0 AS Sum_Valor_Descuento_Proveedor,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Tercera_Edad),0)*1.0 AS Sum_Valor_Descuento_Tercera_Edad,
        ISNULL(SUM(FP.Sum_Conteo_Transacciones),0)*1.0 AS Sum_Conteo_Transacciones,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Es_Tercera_Edad),0)*1.0 AS Sum_Conteo_Trx_Es_Tercera_Edad,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Es_Asegurado),0)*1.0 AS Sum_Conteo_Trx_Es_Asegurado,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Acumula_Monedero),0)*1.0 AS Sum_Conteo_Trx_Acumula_Monedero,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Contiene_Farma),0)*1.0 AS Sum_Conteo_Trx_Contiene_Farma,
        ISNULL(SUM(FP.Sum_Cantidad_Unidades_Relativa),0)*1.0 AS Sum_Cantidad_Unidades_Relativa,
        ISNULL(SUM(FP.Sum_Segundos_Transaccion_Estimado),0)*1.0 AS Sum_Segundos_Transaccion_Estimado
    FROM {{ ref ('BI_Kielsa_Agr_Sucursal_FechaHora') }} FP 
    INNER JOIN {{ ref ('BI_Dim_Calendario_Dinamico') }} C 
    ON FP.Factura_Fecha = C.Fecha_Calendario
    WHERE FP.Factura_Fecha >= '{{ v_fecha_inicio }}' AND FP.Factura_Fecha < '{{ v_fecha_fin }}' 
    --WHERE Factura_Fecha >= DATEADD(DAY,- @DiasPonderacion, @Inicio ) AND Factura_Fecha < @inicio
    GROUP BY FP.Emp_Id, FP.Suc_Id, C.Dia_de_la_Semana
),
Metricas AS
(
SELECT 
    ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Suc_Id,0) AS Suc_Id,
    ISNULL(Dia_Semana_Iso_Id,0) AS Dia_Semana_Iso_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=19, table_alias='')}} [EmpSuc_Id],
    ISNULL(Semanas_Muestra,0) AS Semanas_Muestra,
    CAST(Sum_Cantidad_Padre / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Padre,
    CAST(Sum_Cantidad_Articulos / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Articulos,
    CAST(Sum_Valor_Bruto / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Bruto,
    CAST(Sum_Valor_Neto / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Neto,
    CAST(Sum_Valor_Costo / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Costo,
    CAST(Sum_Valor_Descuento / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento,
    CAST(Sum_Valor_Descuento_Financiero / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Financiero,
    CAST(Sum_Valor_Acum_Monedero / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Acum_Monedero,
    CAST(Sum_Valor_Descuento_Cupon / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Cupon,
    CAST(Sum_Valor_Descuento_Proveedor / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Proveedor,
    CAST(Sum_Valor_Descuento_Tercera_Edad / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Tercera_Edad,
    CAST(Sum_Conteo_Transacciones / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Transacciones,
    CAST(Sum_Conteo_Trx_Es_Tercera_Edad / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Es_Tercera_Edad,
    CAST(Sum_Conteo_Trx_Es_Asegurado / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Es_Asegurado,
    CAST(Sum_Conteo_Trx_Acumula_Monedero / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Acumula_Monedero,
    CAST(Sum_Conteo_Trx_Contiene_Farma / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Contiene_Farma,
    CAST(Sum_Cantidad_Unidades_Relativa / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Unidades_Relativa,
    CAST(Sum_Segundos_Transaccion_Estimado / Semanas_Muestra AS DECIMAL(16,6)) AS Prom_Segundos_Transaccion_Estimado,
    ISNULL(CAST(Sum_Cantidad_Padre / NULLIF(SUM(Sum_Cantidad_Padre) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Cantidad_Padre,
    ISNULL(CAST(Sum_Cantidad_Articulos / NULLIF(SUM(Sum_Cantidad_Articulos) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Cantidad_Articulos,
    ISNULL(CAST(Sum_Valor_Bruto / NULLIF(SUM(Sum_Valor_Bruto) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Bruto,
    ISNULL(CAST(Sum_Valor_Neto / NULLIF(SUM(Sum_Valor_Neto) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Neto,
    ISNULL(CAST(Sum_Valor_Costo / NULLIF(SUM(Sum_Valor_Costo) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Costo,
    ISNULL(CAST(Sum_Valor_Descuento / NULLIF(SUM(Sum_Valor_Descuento) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento,
    ISNULL(CAST(Sum_Valor_Descuento_Financiero / NULLIF(SUM(Sum_Valor_Descuento_Financiero) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento_Financiero,
    ISNULL(CAST(Sum_Valor_Acum_Monedero / NULLIF(SUM(Sum_Valor_Acum_Monedero) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Acum_Monedero,
    ISNULL(CAST(Sum_Valor_Descuento_Cupon / NULLIF(SUM(Sum_Valor_Descuento_Cupon) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento_Cupon,
    ISNULL(CAST(Sum_Valor_Descuento_Proveedor / NULLIF(SUM(Sum_Valor_Descuento_Proveedor) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento_Proveedor,
    ISNULL(CAST(Sum_Valor_Descuento_Tercera_Edad / NULLIF(SUM(Sum_Valor_Descuento_Tercera_Edad) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Valor_Descuento_Tercera_Edad,
    ISNULL(CAST(Sum_Conteo_Transacciones / NULLIF(SUM(Sum_Conteo_Transacciones) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Conteo_Transacciones,
    ISNULL(CAST(Sum_Conteo_Trx_Es_Tercera_Edad / NULLIF(SUM(Sum_Conteo_Trx_Es_Tercera_Edad) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Conteo_Trx_Es_Tercera_Edad,
    ISNULL(CAST(Sum_Conteo_Trx_Es_Asegurado / NULLIF(SUM(Sum_Conteo_Trx_Es_Asegurado) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Conteo_Trx_Es_Asegurado,
    ISNULL(CAST(Sum_Conteo_Trx_Acumula_Monedero / NULLIF(SUM(Sum_Conteo_Trx_Acumula_Monedero) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Conteo_Trx_Acumula_Monedero,
    ISNULL(CAST(Sum_Conteo_Trx_Contiene_Farma / NULLIF(SUM(Sum_Conteo_Trx_Contiene_Farma) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Conteo_Trx_Contiene_Farma,
    ISNULL(CAST(Sum_Cantidad_Unidades_Relativa / NULLIF(SUM(Sum_Cantidad_Unidades_Relativa) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Cantidad_Unidades_Relativa,
    ISNULL(CAST(Sum_Segundos_Transaccion_Estimado / NULLIF(SUM(Sum_Segundos_Transaccion_Estimado) OVER(PARTITION BY Emp_Id, Suc_Id),0) AS DECIMAL(16,12)),0)  AS Part_Segundos_Transaccion_Estimado
--INTO #Temp
FROM ResumenBase
)
SELECT *,
    Part_Cantidad_Padre*7 AS Peso_Cantidad_Padre,
    Part_Cantidad_Articulos*7 AS Peso_Cantidad_Articulos,
    Part_Valor_Bruto*7 AS Peso_Valor_Bruto,
    Part_Valor_Neto*7 AS Peso_Valor_Neto,
    Part_Valor_Costo*7 AS Peso_Valor_Costo,
    Part_Valor_Descuento*7 AS Peso_Valor_Descuento,
    Part_Valor_Descuento_Financiero*7 AS Peso_Valor_Descuento_Financiero,
    Part_Valor_Acum_Monedero*7 AS Peso_Valor_Acum_Monedero,
    Part_Valor_Descuento_Cupon*7 AS Peso_Valor_Descuento_Cupon,
    Part_Valor_Descuento_Proveedor*7 AS Peso_Valor_Descuento_Proveedor,
    Part_Valor_Descuento_Tercera_Edad*7 AS Peso_Valor_Descuento_Tercera_Edad,
    Part_Conteo_Transacciones*7 AS Peso_Conteo_Transacciones,
    Part_Conteo_Trx_Es_Tercera_Edad*7 AS Peso_Conteo_Trx_Es_Tercera_Edad,
    Part_Conteo_Trx_Es_Asegurado*7 AS Peso_Conteo_Trx_Es_Asegurado,
    Part_Conteo_Trx_Acumula_Monedero*7 AS Peso_Conteo_Trx_Acumula_Monedero,
    Part_Conteo_Trx_Contiene_Farma*7 AS Peso_Conteo_Trx_Contiene_Farma,
    Part_Cantidad_Unidades_Relativa*7 AS Peso_Cantidad_Unidades_Relativa,
    Part_Segundos_Transaccion_Estimado*7 AS Peso_Segundos_Transaccion_Estimado
FROM Metricas
WHERE Suc_Id = 1 and Emp_Id = 1