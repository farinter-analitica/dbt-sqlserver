{% set unique_key_list = ["Emp_Id","Suc_Id","Dia_Semana_Iso_Id", "Hora_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
        on_schema_change="fail",
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
{%- set metric_fields = [
    "Cantidad_Padre",
    "Cantidad_Articulos",
    "Valor_Bruto",
    "Valor_Neto",
    "Valor_Costo",
    "Valor_Descuento",
    "Valor_Descuento_Financiero",
    "Valor_Acum_Monedero",
    "Valor_Descuento_Cupon",
    "Valor_Descuento_Proveedor",
    "Valor_Descuento_Tercera_Edad",
    "Conteo_Transacciones",
    "Conteo_Trx_Es_Tercera_Edad",
    "Conteo_Trx_Es_Asegurado",
    "Conteo_Trx_Acumula_Monedero",
    "Conteo_Trx_Contiene_Farma",
    "Cantidad_Unidades_Relativa",
    "Segundos_Transaccion_Estimado",
    "Segundos_Actividad_Estimado"
] %}


WITH ResumenBase
AS
(
    SELECT 
        FP.Emp_Id,
        FP.Suc_Id,
        C.Dia_de_la_Semana as Dia_Semana_Iso_Id,
        FP.Hora_Id as Hora_Id,
        --@SemanasPonderacion AS Semanas_Muestra,
        --@HorasPonderacion AS Horas_Muestra,
        COUNT(DISTINCT C.Dia_de_la_Semana)*1.0 AS Semanas_Muestra,
        COUNT(DISTINCT FP.Hora_Id)*1.0 AS Horas_Muestra,
        {%- for field in metric_fields %}
        ISNULL(SUM(FP.Sum_{{ field }}),0)*1.0 AS Sum_{{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}        
    FROM {{ ref ('BI_Kielsa_Agr_Sucursal_FechaHora') }} FP 
    INNER JOIN {{ ref ('BI_Dim_Calendario_Dinamico') }} C 
    ON FP.Factura_Fecha = C.Fecha_Calendario
    WHERE FP.Factura_Fecha >= '{{ v_fecha_inicio }}' AND FP.Factura_Fecha < '{{ v_fecha_fin }}' 
    --WHERE Factura_Fecha >= DATEADD(DAY,- @DiasPonderacion, @Inicio ) AND Factura_Fecha < @inicio

    GROUP BY FP.Emp_Id, FP.Suc_Id, C.Dia_de_la_Semana, FP.Hora_Id
)
SELECT 
    ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Suc_Id,0) AS Suc_Id,
    ISNULL(Dia_Semana_Iso_Id,0) AS Dia_Semana_Iso_Id,
    ISNULL(Hora_Id,0) AS Hora_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=19, table_alias='')}} [EmpSuc_Id],
    ISNULL(Horas_Muestra,0) AS Horas_Muestra,
    {%- for field in metric_fields %}
    CAST(Sum_{{ field }} / Horas_Muestra AS DECIMAL(16,6)) AS Prom_{{ field }},
    ISNULL(CAST(Sum_{{ field }} / NULLIF(SUM(Sum_{{ field }}) 
        OVER(PARTITION BY Emp_Id, Suc_Id, Dia_Semana_Iso_Id),0) AS DECIMAL(16,12)),0)  AS Part_{{ field }}{% if not loop.last %},{% endif %}
    {%- endfor %}        
--INTO #Temp
FROM ResumenBase
--WHERE Suc_Id = 1 and Emp_Id = 1