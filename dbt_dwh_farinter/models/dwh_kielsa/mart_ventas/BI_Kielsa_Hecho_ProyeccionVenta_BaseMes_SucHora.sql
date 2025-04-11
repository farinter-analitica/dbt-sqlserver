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
-- Calculate percentiles for projected data by Emp_Id, Suc_Id, Fecha_Id
PercentilesProyectados AS (
    SELECT DISTINCT -- DISTINCT para evitar duplicados por bugs de window functions
        Emp_Id,
        Suc_Id,
        Fecha_Id,
        {% for field in metric_fields %}
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {{ field }}) OVER (PARTITION BY Emp_Id, Suc_Id, Fecha_Id) AS Proyec_P75_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM Calculo
    WHERE Fecha_Id >= '{{ v_fecha_inicio_correccion }}' 
      AND Fecha_Id < '{{ v_fecha_hoy }}'
),
-- Calculate percentiles for real data by Emp_Id, Suc_Id, Fecha_Id
PercentilesReales AS (
    SELECT DISTINCT -- DISTINCT para evitar duplicados por bugs de window functions
        Emp_Id,
        Suc_Id,
        Factura_Fecha AS Fecha_Id,
        {% for field in metric_fields %}
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Sum_{{ field }}) OVER (PARTITION BY Emp_Id, Suc_Id, Factura_Fecha) AS Real_P75_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM {{ ref('BI_Kielsa_Agr_Sucursal_FechaHora') }}
    WHERE Factura_Fecha >= '{{ v_fecha_inicio_correccion }}' 
      AND Factura_Fecha < '{{ v_fecha_hoy }}'
),
-- Calculate ratio of real to projected percentiles per day
RatiosPercentiles AS (
    SELECT 
        pr.Emp_Id,
        pr.Suc_Id,
        pr.Fecha_Id,
        {% for field in metric_fields %}
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
            WHEN Avg_Ratio_P75_{{ field }} > 1.5 THEN 1.5
            WHEN Avg_Ratio_P75_{{ field }} < 0.5 THEN 0.5
            ELSE Avg_Ratio_P75_{{ field }} 
        END AS Ratio_P75_Limitado_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM RatiosPromedios
),
-- Scale outliers based on limited percentile ratios
DatosEscalados AS (
    SELECT 
        c.Emp_Id,
        c.Suc_Id,
        c.Fecha_Id,
        c.Hora_Id,
        c.EmpSuc_Id,
        c.Dia_Semana_Iso_Id,
        {% for field in metric_fields %}
        CASE 
            WHEN c.{{ field }} > pp.Proyec_P75_{{ field }} AND pp.Proyec_P75_{{ field }} > 0
            THEN c.{{ field }} * rpl.Ratio_P75_Limitado_{{ field }}
            ELSE c.{{ field }}
        END AS {{ field }}_Escalado,
        {% endfor %}
        1 as dummy_field
    FROM Calculo c
    LEFT JOIN PercentilesProyectados pp 
        ON c.Emp_Id = pp.Emp_Id 
        AND c.Suc_Id = pp.Suc_Id 
        AND c.Fecha_Id = pp.Fecha_Id
    LEFT JOIN RatiosPercentilLimitados rpl 
        ON c.Emp_Id = rpl.Emp_Id 
        AND c.Suc_Id = rpl.Suc_Id
    WHERE c.Fecha_Id >= '{{ v_fecha_inicio_correccion }}' 
      AND c.Fecha_Id < '{{ v_fecha_hoy }}'
),
-- Calculate daily sums on the scaled data
DatosProyectados AS (
    SELECT 
        Emp_Id,
        Suc_Id,
        Fecha_Id,
        {% for field in metric_fields %}
        SUM({{ field }}_Escalado) AS {{ field }}_Sum,
        {% endfor %}
        1 as dummy_field
    FROM DatosEscalados
    GROUP BY 
        Emp_Id,
        Suc_Id,
        Fecha_Id
),
-- Calculate average of sums across dates per Suc_Id
PromediosProyectados AS (
    SELECT 
        Emp_Id,
        Suc_Id,
        {% for field in metric_fields %}
        AVG({{ field }}_Sum) AS Proyec_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM DatosProyectados
    GROUP BY 
        Emp_Id,
        Suc_Id
),
-- Obtener datos reales diarios
DatosReales AS (
    SELECT 
        Emp_Id,
        Suc_Id,
        Factura_Fecha AS Fecha_Id,
        {% for field in metric_fields %}
        SUM(Sum_{{ field }}) AS {{ field }}_Sum,
        {% endfor %}
        1 as dummy_field
    FROM {{ ref('BI_Kielsa_Agr_Sucursal_FechaHora') }}
    WHERE Factura_Fecha >= '{{ v_fecha_inicio_correccion }}' 
      AND Factura_Fecha < '{{ v_fecha_hoy }}'
    GROUP BY 
        Emp_Id,
        Suc_Id,
        Factura_Fecha
),
-- Calculate average of real sums across dates per Suc_Id
PromediosReales AS (
    SELECT 
        Emp_Id,
        Suc_Id,
        {% for field in metric_fields %}
        AVG({{ field }}_Sum) AS Real_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM DatosReales
    GROUP BY 
        Emp_Id,
        Suc_Id
),
-- Comparar promedios y obtener valor de ajuste
ValorAjuste AS (
    SELECT 
        a.Emp_Id,
        a.Suc_Id,
        {% for field in metric_fields %}
        CASE 
            WHEN NULLIF(b.Proyec_{{ field }}, 0) IS NULL THEN 1
            ELSE 1 + ((a.Real_{{ field }} - b.Proyec_{{ field }})/NULLIF(b.Proyec_{{ field }}, 0))
        END AS Ajuste_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM PromediosReales a
    JOIN PromediosProyectados b ON a.Emp_Id = b.Emp_Id AND a.Suc_Id = b.Suc_Id
),
-- Limitar los factores de ajuste para evitar correcciones extremas
AjustesLimitados AS (
    SELECT
        Emp_Id,
        Suc_Id,
        -- Limitar los ajustes a un rango razonable (entre 0.5 y 1.5)
        {% for field in metric_fields %}
        CASE 
            WHEN Ajuste_{{ field }} > 1.5 THEN 1.5
            WHEN Ajuste_{{ field }} < 0.5 THEN 0.5
            ELSE Ajuste_{{ field }} 
        END AS Ajuste_{{ field }},
        {% endfor %}
        1 as dummy_field
    FROM ValorAjuste
),
-- Aplicar los factores de ajuste a las proyecciones futuras
ProyeccionesAjustadas AS (
    SELECT 
        c.Emp_Id,
        c.Suc_Id,
        c.Fecha_Id,
        c.Hora_Id,
        c.EmpSuc_Id,
        c.Dia_Semana_Iso_Id,
        -- Primero escalar con percentiles y luego aplicar ajuste de promedios
        {% for field in metric_fields %}
        CAST(
            (CASE 
                WHEN c.{{ field }} > pp.Proyec_P75_{{ field }} AND pp.Proyec_P75_{{ field }} > 0
                THEN c.{{ field }} * rpl.Ratio_P75_Limitado_{{ field }}
                ELSE c.{{ field }}
            END) * ISNULL(al.Ajuste_{{ field }}, 1) AS DECIMAL(16,6)
        ) AS {{ field }},
        {% endfor %}
        1 as dummy_field
    FROM {{ref('BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora_Staging')}} c
    LEFT JOIN AjustesLimitados al 
        ON c.Emp_Id = al.Emp_Id 
        AND c.Suc_Id = al.Suc_Id
    LEFT JOIN PercentilesProyectados pp 
        ON c.Emp_Id = pp.Emp_Id 
        AND c.Suc_Id = pp.Suc_Id 
        AND c.Fecha_Id = pp.Fecha_Id
    LEFT JOIN RatiosPercentilLimitados rpl 
        ON c.Emp_Id = rpl.Emp_Id 
        AND c.Suc_Id = rpl.Suc_Id
),
-- Proyeccion Final
ResultadoFinal AS (
    -- Proyecciones ajustadas para fechas futuras
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