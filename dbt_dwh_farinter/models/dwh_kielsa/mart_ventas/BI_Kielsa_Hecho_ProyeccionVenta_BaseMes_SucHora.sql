{% set unique_key_list = ["Emp_Id","Suc_Id","Fecha_Id","Hora_Id"] %}
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

{% set v_fecha_hoy = (modules.datetime.datetime.now()).strftime('%Y%m%d') %}
{% set v_dias_auto_correcion = 31 %}
{% set v_fecha_inicio_correccion = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=v_dias_auto_correcion)).strftime('%Y%m%d') %}

--Correccion 20250409 de varios problemas en modelos upstream
--Mejora de escalado por percentiles retroalimentando data real vs proyectada
/*
--1. Pesos de cada dia de la semana por sucursal, valor y peso
DECLARE @Inicio AS DATE = GETDATE()
DECLARE @SemanasPonderacion AS INT = 12
DECLARE @DiasPonderacion AS INT = @SemanasPonderacion*7 --Historia para ponderar
DROP TABLE IF EXISTS #Temp
;

        ISNULL(SUM(FP.Sum_Conteo_Trx_Es_Tercera_Edad),0)*1.0 AS Sum_Conteo_Trx_Es_Tercera_Edad,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Es_Asegurado),0)*1.0 AS Sum_Conteo_Trx_Es_Asegurado,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Acumula_Monedero),0)*1.0 AS Sum_Conteo_Trx_Acumula_Monedero,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Contiene_Farma),0)*1.0 AS Sum_Conteo_Trx_Contiene_Farma,
        ISNULL(SUM(FP.Sum_Cantidad_Unidades_Relativa),0)*1.0 AS Sum_Cantidad_Unidades_Relativa,
        ISNULL(SUM(FP.Sum_Segundos_Transaccion_Estimado),0)*1.0 AS Sum_Segundos_Transaccion_Estimado


*/
{% set metric_fields = [
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
] %}

WITH Calculo AS
(
    SELECT *
    FROM {{ref('BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora_Staging')}}
),
-- Calculate percentiles for all dates (historical and future)
PercentilesProyectados AS (
    SELECT * FROM {{ref('BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora_Staging_Percentil')}}
),
-- Calculate percentiles for real data (historical only)
PercentilesReales AS (
    SELECT DISTINCT
        Emp_Id,
        Suc_Id,
        Factura_Fecha AS Fecha_Id,
        {% for field in metric_fields %}
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Sum_{{ field }}) OVER (PARTITION BY Emp_Id, Suc_Id, Factura_Fecha) AS Real_P25_{{ field }},
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Sum_{{ field }}) OVER (PARTITION BY Emp_Id, Suc_Id, Factura_Fecha) AS Real_P75_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM {{ ref('BI_Kielsa_Agr_Sucursal_FechaHora') }}
    WHERE Factura_Fecha >= '{{ v_fecha_inicio_correccion }}' 
      AND Factura_Fecha < '{{ v_fecha_hoy }}'
),
-- Calculate ratio of real to projected percentiles per day (for historical dates)
RatiosPercentiles AS (
    SELECT 
        pr.Emp_Id,
        pr.Suc_Id,
        pr.Fecha_Id,
        {% for field in metric_fields %}
        CASE 
            WHEN pp.Proyec_P25_{{ field }} > 0 
            THEN pr.Real_P25_{{ field }} / pp.Proyec_P25_{{ field }}
            ELSE 1
        END AS Ratio_P25_{{ field }},
        CASE 
            WHEN pp.Proyec_P75_{{ field }} > 0 
            THEN pr.Real_P75_{{ field }} / pp.Proyec_P75_{{ field }}
            ELSE 1
        END AS Ratio_P75_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM PercentilesReales pr
    JOIN PercentilesProyectados pp 
        ON pr.Emp_Id = pp.Emp_Id 
        AND pr.Suc_Id = pp.Suc_Id 
        AND pr.Fecha_Id = pp.Fecha_Id
),
-- Average the ratios by Emp_Id, Suc_Id
RatiosPromedios AS (
    SELECT 
        Emp_Id,
        Suc_Id,
        {% for field in metric_fields %}
        AVG(Ratio_P25_{{ field }}) AS Avg_Ratio_P25_{{ field }},
        AVG(Ratio_P75_{{ field }}) AS Avg_Ratio_P75_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM RatiosPercentiles
    GROUP BY 
        Emp_Id,
        Suc_Id
),
-- Limitar los ratios de percentiles para evitar correcciones extremas
RatiosPercentilLimitados AS (
    SELECT
        Emp_Id,
        Suc_Id,
        {% for field in metric_fields %}
        CASE 
            WHEN Avg_Ratio_P25_{{ field }} > 1.5 THEN 1.5
            WHEN Avg_Ratio_P25_{{ field }} < 0.5 THEN 0.5
            ELSE Avg_Ratio_P25_{{ field }} 
        END AS Ratio_P25_Limitado_{{ field }},
        CASE 
            WHEN Avg_Ratio_P75_{{ field }} > 1.5 THEN 1.5
            WHEN Avg_Ratio_P75_{{ field }} < 0.5 THEN 0.5
            ELSE Avg_Ratio_P75_{{ field }} 
        END AS Ratio_P75_Limitado_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM RatiosPromedios
),
-- Clasificar datos históricos por rango (bajo, medio, alto)
DatosHistoricosClasificados AS (
    SELECT 
        c.Emp_Id,
        c.Suc_Id,
        c.Fecha_Id,
        c.Hora_Id,
        {% for field in metric_fields %}
        c.{{ field }},
        CASE 
            WHEN c.{{ field }} < pp.Proyec_P25_{{ field }} THEN 1
            WHEN c.{{ field }} > pp.Proyec_P75_{{ field }} THEN 3
            ELSE 2
        END AS Rango_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM Calculo c
    JOIN PercentilesProyectados pp 
        ON c.Emp_Id = pp.Emp_Id 
        AND c.Suc_Id = pp.Suc_Id 
        AND c.Fecha_Id = pp.Fecha_Id
    WHERE c.Fecha_Id >= '{{ v_fecha_inicio_correccion }}' 
      AND c.Fecha_Id < '{{ v_fecha_hoy }}'
),
-- Obtener datos reales históricos
DatosRealesHistoricos AS (
    SELECT 
        d.Emp_Id,
        d.Suc_Id,
        d.Factura_Fecha AS Fecha_Id,
        d.Hora_Id AS Hora_Id,
        {% for field in metric_fields %}
        d.Sum_{{ field }},
        CASE 
            WHEN d.Sum_{{ field }} < pr.Real_P25_{{ field }} THEN 1
            WHEN d.Sum_{{ field }} > pr.Real_P75_{{ field }} THEN 3
            ELSE 2
        END AS Rango_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM {{ ref('BI_Kielsa_Agr_Sucursal_FechaHora') }} d
    JOIN PercentilesReales pr
        ON d.Emp_Id = pr.Emp_Id 
        AND d.Suc_Id = pr.Suc_Id 
        AND d.Factura_Fecha = pr.Fecha_Id
    WHERE d.Factura_Fecha >= '{{ v_fecha_inicio_correccion }}' 
      AND d.Factura_Fecha < '{{ v_fecha_hoy }}'
),
-- Calcular promedios por rango para datos proyectados
PromediosProyectadosPorRango AS (
    SELECT 
        Emp_Id,
        Suc_Id,
        {% for field in metric_fields %}
        AVG(CASE WHEN Rango_{{ field }} = 1 THEN {{ field }} ELSE NULL END) AS Proyec_Bajo_{{ field }},
        AVG(CASE WHEN Rango_{{ field }} = 2 THEN {{ field }} ELSE NULL END) AS Proyec_Medio_{{ field }},
        AVG(CASE WHEN Rango_{{ field }} = 3 THEN {{ field }} ELSE NULL END) AS Proyec_Alto_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM DatosHistoricosClasificados
    GROUP BY 
        Emp_Id,
        Suc_Id
),
-- Calcular promedios por rango para datos reales
PromediosRealesPorRango AS (
    SELECT 
        Emp_Id,
        Suc_Id,
        {% for field in metric_fields %}
        AVG(CASE WHEN Rango_{{ field }} = 1 THEN Sum_{{ field }} ELSE NULL END) AS Real_Bajo_{{ field }},
        AVG(CASE WHEN Rango_{{ field }} = 2 THEN Sum_{{ field }} ELSE NULL END) AS Real_Medio_{{ field }},
        AVG(CASE WHEN Rango_{{ field }} = 3 THEN Sum_{{ field }} ELSE NULL END) AS Real_Alto_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM DatosRealesHistoricos
    GROUP BY 
        Emp_Id,
        Suc_Id
),
-- Calcular factores de ajuste por rango
FactoresAjustePorRango AS (
    SELECT 
        pr.Emp_Id,
        pr.Suc_Id,
        {% for field in metric_fields %}
        CASE 
            WHEN pp.Proyec_Bajo_{{ field }} > 0 THEN pr.Real_Bajo_{{ field }} / pp.Proyec_Bajo_{{ field }}
            ELSE 1
        END AS Ajuste_Bajo_{{ field }},
        CASE 
            WHEN pp.Proyec_Medio_{{ field }} > 0 THEN pr.Real_Medio_{{ field }} / pp.Proyec_Medio_{{ field }}
            ELSE 1
        END AS Ajuste_Medio_{{ field }},
        CASE 
            WHEN pp.Proyec_Alto_{{ field }} > 0 THEN pr.Real_Alto_{{ field }} / pp.Proyec_Alto_{{ field }}
            ELSE 1
        END AS Ajuste_Alto_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM PromediosRealesPorRango pr
    JOIN PromediosProyectadosPorRango pp 
        ON pr.Emp_Id = pp.Emp_Id 
        AND pr.Suc_Id = pp.Suc_Id
),
-- Limitar los factores de ajuste para evitar correcciones extremas
FactoresAjusteLimitados AS (
    SELECT
        Emp_Id,
        Suc_Id,
        {% for field in metric_fields %}
        CASE 
            WHEN Ajuste_Bajo_{{ field }} > 1.5 THEN 1.5
            WHEN Ajuste_Bajo_{{ field }} < 0.5 THEN 0.5
            ELSE Ajuste_Bajo_{{ field }} 
        END AS Ajuste_Bajo_Limitado_{{ field }},
        CASE 
            WHEN Ajuste_Medio_{{ field }} > 1.5 THEN 1.5
            WHEN Ajuste_Medio_{{ field }} < 0.5 THEN 0.5
            ELSE Ajuste_Medio_{{ field }} 
        END AS Ajuste_Medio_Limitado_{{ field }},
        CASE 
            WHEN Ajuste_Alto_{{ field }} > 1.5 THEN 1.5
            WHEN Ajuste_Alto_{{ field }} < 0.5 THEN 0.5
            ELSE Ajuste_Alto_{{ field }} 
        END AS Ajuste_Alto_Limitado_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM FactoresAjustePorRango
),
-- Aplicar los factores de ajuste a las proyecciones según el rango
ProyeccionesAjustadas AS (
    SELECT 
        c.Emp_Id,
        c.Suc_Id,
        c.Fecha_Id,
        c.Hora_Id,
        c.EmpSuc_Id,
        c.Dia_Semana_Iso_Id,
        {% for field in metric_fields %}
        CAST(
            CASE 
                -- Valores por debajo de P25 se ajustan con factor para valores bajos
                WHEN c.{{ field }} < pp.Proyec_P25_{{ field }}
                THEN c.{{ field }} * fal.Ajuste_Bajo_Limitado_{{ field }}
                
                -- Valores por encima de P75 se ajustan con factor para valores altos
                WHEN c.{{ field }} > pp.Proyec_P75_{{ field }}
                THEN c.{{ field }} * fal.Ajuste_Alto_Limitado_{{ field }}
                
                -- Valores en el rango central (P25-P75) se ajustan con factor para valores medios
                ELSE c.{{ field }} * fal.Ajuste_Medio_Limitado_{{ field }}
            END AS DECIMAL(16,6)
        ) AS {{ field }},
        {% endfor %}
        1 as dummy_field
    FROM {{ref('BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora_Staging')}} c
    LEFT JOIN PercentilesProyectados pp 
        ON c.Emp_Id = pp.Emp_Id 
        AND c.Suc_Id = pp.Suc_Id 
        AND c.Fecha_Id = pp.Fecha_Id
    LEFT JOIN FactoresAjusteLimitados fal 
        ON c.Emp_Id = fal.Emp_Id 
        AND c.Suc_Id = fal.Suc_Id
),
-- Proyeccion Final
ResultadoFinal AS (
    -- Proyecciones ajustadas para todas las fechas
    SELECT 
        Emp_Id,
        Suc_Id,
        Fecha_Id,
        Hora_Id,
        EmpSuc_Id,
        {% for field in metric_fields %}
        {{ field }},
        {% endfor %}
        GETDATE() AS Fecha_Actualizado
    FROM ProyeccionesAjustadas
)

-- Consulta final
SELECT 
    *,
    GETDATE() AS Fecha_Carga
FROM ResultadoFinal




    /*

--Comprobar

SELECT a.Hora_Id,
    a.Cantidad_Padre as Cantidad_SucCanHora,
    b.Cantidad_Padre as Cantidad_BaseMes,
    a.Valor_Neto as ValorNeto_SucCanHora,
    b.Valor_Neto as ValorNeto_BaseMes,
    a.Cantidad_Padre - b.Cantidad_Padre as Diferencia_Cantidad,
    a.Valor_Neto - b.Valor_Neto as Diferencia_Valor
FROM 
    "BI_FARINTER"."dbo".BI_Kielsa_Hecho_ProyeccionVenta_SucCanHora a
    FULL OUTER JOIN "BI_FARINTER"."dbo".BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora b
    ON a.emp_id = b.emp_id 
    AND a.Suc_Id = b.Suc_Id
	and a.Fecha_Id = b.fecha_id
	and a.Hora_Id = b.hora_id
WHERE 
    a.emp_id = 1 
    AND a.Suc_Id = 1

    */