{% set unique_key_list = ["Emp_Id","Suc_Id"] %}
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
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}
{% set v_semanas_muestra = 56 %}
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
        --@SemanasPonderacion AS Semanas_Ponderacion,
        ISNULL(COUNT(DISTINCT CAL.Fecha_Calendario)*1.0,1.0) AS Dias_Muestra,
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
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa' ) }} EMP
    ON EMP.Empresa_Id = FP.Emp_Id
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Dim_Pais' ) }} PAIS
    ON PAIS.Pais_Id = EMP.Pais_Id
    INNER JOIN {{ ref('BI_Dim_Calendario_LaboralPais') }} CAL
    on CAL.Fecha_Calendario = FP.Factura_Fecha
    AND CAL.[Es_Dia_Feriado] =1
    AND PAIS.Pais_ISO2 = CAL.Pais_ISO2
    WHERE FP.Factura_Fecha >= '{{ v_fecha_inicio }}' AND FP.Factura_Fecha < '{{ v_fecha_fin }}' 
    --WHERE Factura_Fecha >= DATEADD(DAY,- @DiasPonderacion, @Inicio ) AND Factura_Fecha < @inicio
    GROUP BY FP.Emp_Id, FP.Suc_Id, PAIS.Pais_Id
)
SELECT 
    ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Suc_Id,0) AS Suc_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=19, table_alias='')}} [EmpSuc_Id],
    CAST(Dias_Muestra AS INT) AS Dias_Muestra,
    CAST(Sum_Cantidad_Padre / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Padre,
    CAST(Sum_Cantidad_Articulos / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Articulos,
    CAST(Sum_Valor_Bruto / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Bruto,
    CAST(Sum_Valor_Neto / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Neto ,
    CAST(Sum_Valor_Costo / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Costo,
    CAST(Sum_Valor_Descuento / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento,
    CAST(Sum_Valor_Descuento_Financiero / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Financiero,
    CAST(Sum_Valor_Acum_Monedero / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Acum_Monedero,
    CAST(Sum_Valor_Descuento_Cupon / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Cupon,
    CAST(Sum_Valor_Descuento_Proveedor / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Proveedor,
    CAST(Sum_Valor_Descuento_Tercera_Edad / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Tercera_Edad,
    CAST(Sum_Conteo_Transacciones / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Transacciones,
    CAST(Sum_Conteo_Trx_Es_Tercera_Edad / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Es_Tercera_Edad,
    CAST(Sum_Conteo_Trx_Es_Asegurado / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Es_Asegurado,
    CAST(Sum_Conteo_Trx_Acumula_Monedero / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Acumula_Monedero,
    CAST(Sum_Conteo_Trx_Contiene_Farma / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Contiene_Farma,
    CAST(Sum_Cantidad_Unidades_Relativa / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Unidades_Relativa,
    CAST(Sum_Segundos_Transaccion_Estimado / Dias_Muestra AS DECIMAL(16,6)) AS Prom_Segundos_Transaccion_Estimado
--INTO #Temp
FROM ResumenBase


