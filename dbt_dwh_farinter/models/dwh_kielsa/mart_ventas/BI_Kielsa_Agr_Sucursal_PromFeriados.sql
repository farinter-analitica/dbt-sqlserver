{% set unique_key_list = ["Emp_Id","Suc_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}
{% set v_semanas_ponderacion = 56 %}
{% set v_dias_ponderacion = v_semanas_ponderacion * 7 %}
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
        COUNT(DISTINCT CAL.Fecha_Calendario)*1.0 AS Dias_Ponderacion,
        SUM(FP.Valor_Neto) AS Sum_Valor_Neto,
        SUM(FP.Valor_Costo) AS Sum_Valor_Costo,
        SUM(FP.Valor_Descuento) AS Sum_Valor_Descuento,
        SUM(FP.Valor_Descuento_Financiero) AS Sum_Valor_Descuento_Financiero,
        SUM(FP.Valor_Acum_Monedero) AS Sum_Valor_Acum_Monedero,
        SUM(FP.Valor_Descuento_Cupon) AS Sum_Valor_Descuento_Cupon,
        SUM(FP.Descuento_Proveedor) AS Sum_Descuento_Proveedor,
        SUM(FP.Valor_Descuento_Tercera_Edad) AS Sum_Valor_Descuento_Tercera_Edad,
        COUNT(DISTINCT FP.EmpSucDocCajFac_Id) AS Sum_Conteo_Transacciones
    FROM {{ ref ('BI_Kielsa_Hecho_FacturaPosicion') }} FP 
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa' ) }} EMP
    ON EMP.Empresa_Id = FP.Emp_Id
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Dim_Pais' ) }} PAIS
    ON PAIS.Pais_Id = EMP.Pais_Id
    INNER JOIN {{ ref('BI_Dim_Calendario_LaboralPais') }} CAL
    on CAL.Fecha_Calendario = FP.Factura_Fecha AND CAL.AnioMes_Id = FP.AnioMes_Id
    AND CAL.[Es_Dia_Feriado] =1
    AND PAIS.Pais_ISO2 = CAL.Pais_ISO2
    WHERE FP.Factura_Fecha >= '{{ v_fecha_inicio }}' AND FP.Factura_Fecha < '{{ v_fecha_fin }}' AND FP.AnioMes_Id >= {{ v_anio_mes_inicio }}
    --WHERE Factura_Fecha >= DATEADD(DAY,- @DiasPonderacion, @Inicio ) AND Factura_Fecha < @inicio
    GROUP BY FP.Emp_Id, FP.Suc_Id, PAIS.Pais_Id
)
SELECT 
    ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Suc_Id,0) AS Suc_Id,
    Dias_Ponderacion,
    Sum_Valor_Neto / Dias_Ponderacion AS Prom_Valor_Venta,
    Sum_Valor_Costo / Dias_Ponderacion AS Prom_Valor_Costo,
    Sum_Valor_Descuento / Dias_Ponderacion AS Prom_Valor_Descuento,
    Sum_Valor_Descuento_Financiero / Dias_Ponderacion AS Prom_Valor_Descuento_Financiero,
    Sum_Valor_Acum_Monedero / Dias_Ponderacion AS Prom_Valor_Acum_Monedero,
    Sum_Valor_Descuento_Cupon / Dias_Ponderacion AS Prom_Valor_Descuento_Cupon,
    Sum_Descuento_Proveedor / Dias_Ponderacion AS Prom_Valor_Descuento_Proveedor,
    Sum_Valor_Descuento_Tercera_Edad / Dias_Ponderacion AS Prom_Valor_Descuento_Tercera_Edad,
    Sum_Conteo_Transacciones / Dias_Ponderacion AS Prom_Conteo_Transacciones
--INTO #Temp
FROM ResumenBase


