{% set unique_key_list = ["Emp_Id","Suc_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["automation/periodo_mensual_inicio", "periodo_unico/si",  "automation_only"],
		materialized="table",
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
{% set v_meses_completos = 24 %}  {# Define months, adjust this as per requirement #}
{% set v_fecha_base = (modules.datetime.datetime.now().replace(day=1) - modules.datetime.timedelta(days=1)).replace(day=1) %}
{% set v_fecha_inicio_dt = (v_fecha_base.replace(year=v_fecha_base.year - (v_meses_completos // 12), month=v_fecha_base.month - (v_meses_completos % 12))) %}
{% set v_fecha_fin_dt = modules.datetime.datetime.now().replace(day=1) %}
{% set v_dias_muestra = (v_fecha_fin_dt - v_fecha_inicio_dt).days %}
{% set v_fecha_inicio = v_fecha_inicio_dt.strftime('%Y%m%d') %}
{% set v_fecha_fin = v_fecha_fin_dt.strftime('%Y%m%d') %}
{% set v_anio_mes_inicio = v_fecha_inicio[:6] %}

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
        COUNT(DISTINCT FP.Factura_Fecha)*1.0 AS Dias_Muestra,
        {%- for field in metric_fields %}
        ISNULL(SUM(FP.Sum_{{ field }}),0)*1.0 AS Sum_{{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}        
    FROM {{ ref ('BI_Kielsa_Agr_Sucursal_FechaHora') }} FP 
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa' ) }} EMP
        ON EMP.Empresa_Id = FP.Emp_Id
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Dim_Pais' ) }} PAIS
        ON PAIS.Pais_Id = EMP.Pais_Id
    INNER JOIN --Para ignorar dias feriados
        {{ ref('BI_Dim_Calendario_LaboralPais') }} CAL
        ON CAL.Fecha_Calendario = FP.Factura_Fecha
        AND PAIS.Pais_ISO2 = CAL.Pais_ISO2
    WHERE FP.Factura_Fecha >= '{{ v_fecha_inicio }}' AND FP.Factura_Fecha < '{{ v_fecha_fin }}' 
    AND CAL.[Es_Dia_Feriado] = 0
    --WHERE Factura_Fecha >= DATEADD(DAY,- @DiasPonderacion, @Inicio ) AND Factura_Fecha < @inicio
    GROUP BY FP.Emp_Id, FP.Suc_Id
)
SELECT 
    ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Suc_Id,0) AS Suc_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=19, table_alias='')}} [EmpSuc_Id],
    CAST(Dias_Muestra AS INT) AS Dias_Muestra,
    {%- for field in metric_fields %}
    CAST(Sum_{{ field }} / Dias_Muestra  AS DECIMAL(16,6)) AS Prom_{{ field }}{% if not loop.last %},{% endif %}
    {%- endfor %}        
--INTO #Temp
FROM ResumenBase
