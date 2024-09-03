{% set unique_key_list = ["Emp_Id","Suc_Id","Dia_Semana_Iso_Id"] %}
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
{% set v_semanas_ponderacion = 12 %}
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
        Emp_Id,
        Suc_Id,
        FP.Dia_de_la_Semana as Dia_Semana_Iso_Id,
        --@SemanasPonderacion AS Semanas_Ponderacion,
        {{ v_semanas_ponderacion }} AS Semanas_Ponderacion,
        SUM(FP.Valor_Neto) AS Sum_Valor_Neto,
        SUM(FP.Valor_Costo) AS Sum_Valor_Costo,
        SUM(FP.Valor_Descuento) AS Sum_Valor_Descuento,
        SUM(FP.Valor_Descuento_Financiero) AS Sum_Valor_Descuento_Financiero,
        SUM(FP.Valor_Acum_Monedero) AS Sum_Valor_Acum_Monedero,
        SUM(FP.Valor_Descuento_Cupon) AS Sum_Valor_Descuento_Cupon,
        SUM(FP.Descuento_Proveedor) AS Sum_Descuento_Proveedor,
        SUM(FP.Valor_Descuento_Tercera_Edad) AS Sum_Valor_Descuento_Tercera_Edad,
        COUNT(distinct FP.EmpSucDocCajFac_Id) AS Sum_Conteo_Transacciones
    FROM {{ ref ('BI_Kielsa_Hecho_FacturaPosicion') }} FP 
    WHERE Factura_Fecha >= '{{ v_fecha_inicio }}' AND Factura_Fecha < '{{ v_fecha_fin }}' AND AnioMes_Id >= {{ v_anio_mes_inicio }}
    --WHERE Factura_Fecha >= DATEADD(DAY,- @DiasPonderacion, @Inicio ) AND Factura_Fecha < @inicio
    GROUP BY Emp_Id, Suc_Id, Dia_de_la_Semana
)
SELECT 
    ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Suc_Id,0) AS Suc_Id,
    ISNULL(Dia_Semana_Iso_Id,0) AS Dia_Semana_Iso_Id,
    Sum_Valor_Neto / Semanas_Ponderacion AS Prom_Valor_Venta,
    Sum_Valor_Costo / Semanas_Ponderacion AS Prom_Valor_Costo,
    Sum_Valor_Descuento / Semanas_Ponderacion AS Prom_Valor_Descuento,
    Sum_Valor_Descuento_Financiero / Semanas_Ponderacion AS Prom_Valor_Descuento_Financiero,
    Sum_Valor_Acum_Monedero / Semanas_Ponderacion AS Prom_Valor_Acum_Monedero,
    Sum_Valor_Descuento_Cupon / Semanas_Ponderacion AS Prom_Valor_Descuento_Cupon,
    Sum_Descuento_Proveedor / Semanas_Ponderacion AS Prom_Valor_Descuento_Proveedor,
    Sum_Valor_Descuento_Tercera_Edad / Semanas_Ponderacion AS Prom_Valor_Descuento_Tercera_Edad,
    Sum_Conteo_Transacciones / Semanas_Ponderacion AS Prom_Conteo_Transacciones,
    ISNULL(Sum_Valor_Neto / NULLIF(SUM(Sum_Valor_Neto) OVER(PARTITION BY Emp_Id, Suc_Id),0),0)  AS Part_Valor_Venta,
    ISNULL(Sum_Valor_Costo / NULLIF(SUM(Sum_Valor_Costo) OVER(PARTITION BY Emp_Id, Suc_Id),0),0)  AS Part_Valor_Costo,
    ISNULL(Sum_Valor_Descuento / NULLIF(SUM(Sum_Valor_Descuento) OVER(PARTITION BY Emp_Id, Suc_Id),0),0)  AS Part_Valor_Descuento,
    ISNULL(Sum_Valor_Descuento_Financiero / NULLIF(SUM(Sum_Valor_Descuento_Financiero) OVER(PARTITION BY Emp_Id, Suc_Id),0),0)  AS Part_Valor_Descuento_Financiero,
    ISNULL(Sum_Valor_Acum_Monedero / NULLIF(SUM(Sum_Valor_Acum_Monedero) OVER(PARTITION BY Emp_Id, Suc_Id),0),0)  AS Part_Valor_Acum_Monedero,
    ISNULL(Sum_Valor_Descuento_Cupon / NULLIF(SUM(Sum_Valor_Descuento_Cupon) OVER(PARTITION BY Emp_Id, Suc_Id),0),0)  AS Part_Valor_Descuento_Cupon,
    ISNULL(Sum_Descuento_Proveedor / NULLIF(SUM(Sum_Descuento_Proveedor) OVER(PARTITION BY Emp_Id, Suc_Id),0),0)  AS Part_Valor_Descuento_Proveedor,
    ISNULL(Sum_Valor_Descuento_Tercera_Edad / NULLIF(SUM(Sum_Valor_Descuento_Tercera_Edad) OVER(PARTITION BY Emp_Id, Suc_Id),0),0)  AS Part_Valor_Descuento_Tercera_Edad,
    ISNULL(Sum_Conteo_Transacciones / NULLIF(SUM(Sum_Conteo_Transacciones) OVER(PARTITION BY Emp_Id, Suc_Id),0),0)  AS Part_Conteo_Transacciones
--INTO #Temp
FROM ResumenBase


