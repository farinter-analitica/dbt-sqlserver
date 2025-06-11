{% set unique_key_list = ["Emp_Id","Suc_Id","Mes_Id"] %}
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
{% set v_meses_completos = 24 %}
{% set v_fecha_base = (modules.datetime.datetime.now().replace(day=1) - modules.datetime.timedelta(days=1)).replace(day=1) %}
{% set v_fecha_inicio_dt = (v_fecha_base.replace(year=v_fecha_base.year - (v_meses_completos // 12), month=v_fecha_base.month - (v_meses_completos % 12))) %}
{% set v_fecha_fin_dt = modules.datetime.datetime.now().replace(day=1) %}
{% set v_fecha_inicio = v_fecha_inicio_dt.strftime('%Y%m%d') %}
{% set v_fecha_fin = v_fecha_fin_dt.strftime('%Y%m%d') %}
{% set v_anio_mes_inicio = v_fecha_inicio[:6] %}

{#TODO: Hacer que las sucursales sin suficientes meses usen una aproximacion distinta #}

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

WITH ResumenBase AS (
    SELECT 
        Emp_Id,
        Suc_Id,
        MONTH(FP.Factura_Fecha) as Mes_Id_Original,     
        COUNT(DISTINCT FP.Factura_Fecha)/365.0 AS Anios_Muestra,
        {%- for field in metric_fields %}
        ISNULL(SUM(FP.Sum_{{ field }}),0.0)*1.0 AS Sum_{{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}        
    FROM {{ ref ('BI_Kielsa_Agr_Sucursal_FechaHora') }} FP 
    WHERE Factura_Fecha >= '{{ v_fecha_inicio }}' 
    AND Factura_Fecha < '{{ v_fecha_fin }}' 
    GROUP BY Emp_Id, Suc_Id, MONTH(Factura_Fecha)
),
VerificarCompletitud AS
(
    SELECT 
        Emp_Id,
        Suc_Id,
        COUNT(DISTINCT Mes_Id_Original) AS Meses_Sucursal
    FROM ResumenBase
    GROUP BY Emp_Id, Suc_Id
),
CompletarMeses AS
(
    SELECT 
        M.Mes_Id,
        R.*,
        CASE WHEN V.Meses_Sucursal = 12 THEN 1 ELSE 0 END AS Es_Sucursal_Meses_Completos
    FROM BI_FARINTER.dbo.BI_Dim_Mes M --{{ source('BI_FARINTER', 'BI_Dim_Mes') }}
    LEFT JOIN ResumenBase R ON M.Mes_Id = R.Mes_Id_Original
    LEFT JOIN VerificarCompletitud V ON R.Emp_Id = V.Emp_Id AND R.Suc_Id = V.Suc_Id
),
Metricas AS
(
SELECT 
    ISNULL(Emp_Id,0) AS Emp_Id,
    ISNULL(Suc_Id,0) AS Suc_Id,
    ISNULL(Mes_Id,0) AS Mes_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=19, table_alias='')}} [EmpSuc_Id],
    ISNULL(Anios_Muestra,0) AS Anios_Muestra,
    Es_Sucursal_Meses_Completos,
    {%- for field in metric_fields %}
    CAST(Sum_{{ field }} / Anios_Muestra AS DECIMAL(16,6)) AS Prom_{{ field }},
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_{{ field }} / NULLIF(SUM(Sum_{{ field }}) OVER(PARTITION BY Emp_Id, Suc_Id),0) 
        ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_{{ field }}{% if not loop.last %},{% endif %}
    {%- endfor %}        
FROM CompletarMeses
)
SELECT *,
    {%- for field in metric_fields %}
    CASE WHEN Part_{{ field }}*12 > 1.05 THEN 1.05 WHEN Part_{{ field }}*12 <0.95 THEN 0.95 ELSE Part_{{ field }}*12  END AS Peso_{{ field }}{% if not loop.last %},{% endif %}
    {%- endfor %}        
FROM Metricas