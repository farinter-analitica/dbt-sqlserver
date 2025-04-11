{% set unique_key_list = ["Emp_Id","Suc_Id","Fecha_Id"] %}
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
-}}

{%- set v_fecha_hoy = (modules.datetime.datetime.now()).strftime('%Y%m%d') %}
{%- set v_dias_auto_correcion = 31 %}
{%- set v_fecha_inicio_correccion = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=v_dias_auto_correcion)).strftime('%Y%m%d') %}

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
    "Segundos_Transaccion_Estimado"
] -%}

WITH Calculo AS
(
    SELECT *
    FROM {{ref('BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora_Staging')}}
    WHERE Fecha_Id >= '{{ v_fecha_inicio_correccion }}' 
),
-- Calculate percentiles for all dates (historical and future)
Percentiles_1 AS (
    SELECT DISTINCT
        Emp_Id,
        Suc_Id,
        Fecha_Id,
        {% for field in metric_fields -%}
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {{ field }}) 
            OVER (PARTITION BY Emp_Id, Suc_Id, Fecha_Id) AS Proyec_P25_{{ field }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM Calculo
),
Percentiles_2 AS (
    SELECT DISTINCT
        Emp_Id,
        Suc_Id,
        Fecha_Id,
        {% for field in metric_fields -%}
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {{ field }}) 
            OVER (PARTITION BY Emp_Id, Suc_Id, Fecha_Id) AS Proyec_P75_{{ field }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM Calculo
),
PercentilesProyectados AS
(
    SELECT 
        P1.Emp_Id,
        P1.Suc_Id,
        P1.Fecha_Id,
        {% for field in metric_fields -%}
        P1.Proyec_P25_{{ field }},
        P2.Proyec_P75_{{ field }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM Percentiles_1 P1
    INNER JOIN Percentiles_2 P2
    ON P1.Emp_Id = P2.Emp_Id
    AND P1.Suc_Id = P2.Suc_Id
    AND P1.Fecha_Id = P2.Fecha_Id
)
-- Consulta final
SELECT 
    *,
    GETDATE() AS Fecha_Carga
FROM PercentilesProyectados
