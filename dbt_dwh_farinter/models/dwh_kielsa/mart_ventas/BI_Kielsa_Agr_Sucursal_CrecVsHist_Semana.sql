{% set unique_key_list = ["Emp_Id","Suc_Id"] %}
{{ 
    config(
        as_columnstore=true,
		tags=["automation/periodo_semanal_1", "periodo_unico/si",  "automation_only"],
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
{% set v_semanas_completas = 12 %}
{% set v_anios_historicos = 2 %}
{% set v_fecha_base = modules.datetime.datetime.now() %}
{% set v_fecha_inicio_actual_dt = (v_fecha_base - modules.datetime.timedelta(days=v_fecha_base.isoweekday() + v_semanas_completas*7) ) %}
{% set v_semanas_evaluadas = [] %}
{% for i in range(v_semanas_completas) %}
    {% set week = (v_fecha_inicio_actual_dt + modules.datetime.timedelta(days=(i+1)*7)).isocalendar()[1] %}
    {% do v_semanas_evaluadas.append(week) %}
{% endfor %}

{% set v_fecha_inicio = v_fecha_inicio_actual_dt.replace(year=v_fecha_inicio_actual_dt.year - v_anios_historicos, day=1).strftime('%Y%m%d') %}
{% set v_fecha_inicio_actual = v_fecha_inicio_actual_dt.strftime('%Y%m%d') %}
{% set v_fecha_fin_iso = (v_fecha_base - modules.datetime.timedelta(days=v_fecha_base.isoweekday())) %}
{% set v_fecha_fin = v_fecha_fin_iso.strftime('%Y%m%d') %}
{% set v_anio_mes_inicio = v_fecha_inicio[:6] %}
{% set v_anio_actual = v_fecha_fin[:4] %}

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
        FP.Emp_Id,
        FP.Suc_Id,
        C.Anio_ISO AS Anio_Id,
        COUNT(DISTINCT FP.Factura_Fecha)/365.0 AS Anios_Muestra,
        COUNT(DISTINCT FP.Factura_Fecha)/7.0 AS Semanas_Muestra,
        C.Semana_del_Anio_ISO as Semana_del_Anio_ISO_Original,
        MIN(C.Fecha_Calendario) AS Fecha_Inicio_Semana,
        {%- for field in metric_fields %}
        ISNULL(SUM(FP.Sum_{{ field }}),0.0)*1.0 AS Sum_{{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}        
    FROM {{ ref ('BI_Kielsa_Agr_Sucursal_FechaHora') }} FP 
    INNER JOIN {{ ref ('BI_Dim_Calendario_Dinamico') }} C 
    ON FP.Factura_Fecha = C.Fecha_Calendario
    WHERE FP.Factura_Fecha >= '{{ v_fecha_inicio }}' 
    AND FP.Factura_Fecha < '{{ v_fecha_fin }}' 
    AND C.Semana_del_Anio_ISO IN ({{ v_semanas_evaluadas | join(',') }})
    GROUP BY FP.Emp_Id, FP.Suc_Id, C.Anio_ISO, C.Semana_del_Anio_ISO
),
Historico AS
(
    SELECT 
        Emp_Id,
        Suc_Id,
        SUM(Anios_Muestra) AS Anios_Muestra,
        CASE WHEN SUM(Anios_Muestra) < {{ v_anios_historicos*0.7 }} 
            OR SUM(Semanas_Muestra) < {{ v_semanas_completas*0.7 }} 
            THEN 0 ELSE 1 END AS Es_Sucursal_Anios_Completos,
        {%- for field in metric_fields %}
        SUM(Sum_{{ field }}) / SUM(Anios_Muestra)*SUM(Semanas_Muestra) AS Prom_{{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}        
    FROM ResumenBase
    WHERE Fecha_Inicio_Semana < '{{ v_fecha_inicio_actual }}'
    GROUP BY Emp_Id, Suc_Id
),
Actual AS
(
    SELECT 
        Emp_Id,
        Suc_Id,
        {%- for field in metric_fields %}
        SUM(Sum_{{ field }}) / SUM(Semanas_Muestra) AS Prom_{{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}        
    FROM ResumenBase
    WHERE Fecha_Inicio_Semana >= '{{ v_fecha_inicio_actual }}'
    GROUP BY Emp_Id, Suc_Id
),
Diferencias AS
(
    SELECT 
        ISNULL(Actual.Emp_Id,Historico.Emp_Id) AS Emp_Id,
        ISNULL(Actual.Suc_Id,Historico.Suc_Id) AS Suc_Id,
        ISNULL(Historico.Anios_Muestra,0) AS Anios_Muestra,
        ISNULL(Historico.Es_Sucursal_Anios_Completos,0) AS Es_Sucursal_Anios_Completos,
        {%- for field in metric_fields %}
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_{{ field }},0) - Historico.Prom_{{ field }} 
            ELSE 0 END AS Dif_{{ field }},
        ISNULL(Actual.Prom_{{ field }},0) as Prom_{{ field }}_Actual,
        ISNULL(Historico.Prom_{{ field }},0) as Prom_{{ field }}_Historico{% if not loop.last %},{% endif %}
        {%- endfor %}        
    FROM Actual
    FULL JOIN Historico 
    ON Actual.Emp_Id = Historico.Emp_Id AND Actual.Suc_Id = Historico.Suc_Id
),
Tendencia AS    
(
    SELECT 
        ISNULL(Emp_Id,0) AS Emp_Id,
        ISNULL(Suc_Id,0) AS Suc_Id,
        ISNULL(Anios_Muestra,0) AS Anios_Muestra,
        Es_Sucursal_Anios_Completos,
        {%- for field in metric_fields %}
        CASE WHEN Prom_{{ field }}_Historico > 0
            THEN  (1.0 + Dif_{{ field }} / Prom_{{ field }}_Historico)
            ELSE 1.02 END AS Crec_{{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}        
    FROM Diferencias
)
SELECT Emp_Id,
        Suc_Id,
        Anios_Muestra,
        Es_Sucursal_Anios_Completos,
        {%- for field in metric_fields %}
        CASE WHEN Crec_{{ field }}<0 THEN 0 
            WHEN Crec_{{ field }}>2 THEN 2 ELSE Crec_{{ field }} END AS Crec_{{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}                

FROM Tendencia