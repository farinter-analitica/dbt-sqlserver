{% set unique_key_list = ["Emp_Id","Suc_Id","Fecha_Id","Hora_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["automation/periodo_semanal_1", "periodo_unico/si",  "automation_only"],
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

{% set v_fecha_inicio = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=31)).strftime('%Y%m%d') %}
{% set v_fecha_fin = (modules.datetime.datetime.now() + modules.datetime.timedelta(days=120)).strftime('%Y%m%d') %}
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
WITH Calculo AS
(
    SELECT *
    FROM {{ref('BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora_Staging')}}
),
DatosProyectados AS (
    SELECT 
        FH.Emp_Id,
        FH.Suc_Id,
        FH.Fecha_Id AS Fecha_Id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Cantidad_Padre)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Cantidad_Padre,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Cantidad_Articulos)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Cantidad_Articulos,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Valor_Bruto)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Valor_Bruto,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Valor_Neto)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Valor_Neto,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Valor_Costo)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Valor_Costo,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Valor_Descuento)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Valor_Descuento,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Valor_Descuento_Financiero)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Valor_Descuento_Financiero,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Valor_Acum_Monedero)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Valor_Acum_Monedero,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Valor_Descuento_Cupon)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Valor_Descuento_Cupon,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Valor_Descuento_Proveedor)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Valor_Descuento_Proveedor,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Valor_Descuento_Tercera_Edad)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Valor_Descuento_Tercera_Edad,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Conteo_Transacciones)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Conteo_Transacciones,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Conteo_Trx_Es_Tercera_Edad)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Conteo_Trx_Es_Tercera_Edad,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Conteo_Trx_Es_Asegurado)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Conteo_Trx_Es_Asegurado,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Conteo_Trx_Acumula_Monedero)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Conteo_Trx_Acumula_Monedero,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Conteo_Trx_Contiene_Farma)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Conteo_Trx_Contiene_Farma,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Cantidad_Unidades_Relativa)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Cantidad_Unidades_Relativa,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Segundos_Transaccion_Estimado)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Proyec_Segundos_Transaccion_Estimado
    FROM Calculo FH
    WHERE FH.Fecha_Id >= '{{ v_fecha_inicio_correccion }}' 
      AND FH.Fecha_Id < '{{ v_fecha_hoy }}'
    GROUP BY 
    FH.Emp_Id,
    FH.Suc_Id,
    FH.Fecha_Id
),
-- Obtener datos reales de los últimos v_dias_auto_correcion días para comparar con proyecciones
DatosReales AS (
    SELECT 
        FH.Emp_Id,
        FH.Suc_Id,
        FH.Factura_Fecha AS Fecha_Id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Cantidad_Padre)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Cantidad_Padre,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Cantidad_Articulos)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Cantidad_Articulos,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Valor_Bruto)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Valor_Bruto,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Valor_Neto)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Valor_Neto,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Valor_Costo)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Valor_Costo,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Valor_Descuento)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Valor_Descuento,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Valor_Descuento_Financiero)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Valor_Descuento_Financiero,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Valor_Acum_Monedero)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Valor_Acum_Monedero,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Valor_Descuento_Cupon)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Valor_Descuento_Cupon,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Valor_Descuento_Proveedor)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Valor_Descuento_Proveedor,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Valor_Descuento_Tercera_Edad)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Valor_Descuento_Tercera_Edad,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Conteo_Transacciones)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Conteo_Transacciones,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Conteo_Trx_Es_Tercera_Edad)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Conteo_Trx_Es_Tercera_Edad,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Conteo_Trx_Es_Asegurado)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Conteo_Trx_Es_Asegurado,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Conteo_Trx_Acumula_Monedero)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Conteo_Trx_Acumula_Monedero,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Conteo_Trx_Contiene_Farma)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Conteo_Trx_Contiene_Farma,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Cantidad_Unidades_Relativa)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Cantidad_Unidades_Relativa,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(FH.Sum_Segundos_Transaccion_Estimado)) OVER (PARTITION BY Emp_Id, Suc_Id) AS Real_Segundos_Transaccion_Estimado
    FROM {{ ref('BI_Kielsa_Agr_Sucursal_FechaHora') }} FH
    WHERE FH.Factura_Fecha >= '{{ v_fecha_inicio_correccion }}' 
      AND FH.Factura_Fecha < '{{ v_fecha_hoy }}'
    GROUP BY 
    FH.Emp_Id,
    FH.Suc_Id,
    FH.Factura_Fecha
),
-- Comparar con la mediana y obtener valor de ajuste
ValorAjuste AS
(
    SELECT 
        a.Emp_Id,
        a.Suc_Id,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Cantidad_Padre), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Cantidad_Padre - b.Proyec_Cantidad_Padre)/NULLIF(AVG(b.Proyec_Cantidad_Padre), 0))
        END AS Ajuste_Cantidad_Padre,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Cantidad_Articulos), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Cantidad_Articulos - b.Proyec_Cantidad_Articulos)/NULLIF(AVG(b.Proyec_Cantidad_Articulos), 0))
        END AS Ajuste_Cantidad_Articulos,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Valor_Bruto), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Valor_Bruto - b.Proyec_Valor_Bruto)/NULLIF(AVG(b.Proyec_Valor_Bruto), 0))
        END AS Ajuste_Valor_Bruto,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Valor_Neto), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Valor_Neto - b.Proyec_Valor_Neto)/NULLIF(AVG(b.Proyec_Valor_Neto), 0))
        END AS Ajuste_Valor_Neto,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Valor_Costo), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Valor_Costo - b.Proyec_Valor_Costo)/NULLIF(AVG(b.Proyec_Valor_Costo), 0))
        END AS Ajuste_Valor_Costo,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Valor_Descuento), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Valor_Descuento - b.Proyec_Valor_Descuento)/NULLIF(AVG(b.Proyec_Valor_Descuento), 0))
        END AS Ajuste_Valor_Descuento,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Valor_Descuento_Financiero), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Valor_Descuento_Financiero - b.Proyec_Valor_Descuento_Financiero)/NULLIF(AVG(b.Proyec_Valor_Descuento_Financiero), 0))
        END AS Ajuste_Valor_Descuento_Financiero,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Valor_Acum_Monedero), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Valor_Acum_Monedero - b.Proyec_Valor_Acum_Monedero)/NULLIF(AVG(b.Proyec_Valor_Acum_Monedero), 0))
        END AS Ajuste_Valor_Acum_Monedero,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Valor_Descuento_Cupon), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Valor_Descuento_Cupon - b.Proyec_Valor_Descuento_Cupon)/NULLIF(AVG(b.Proyec_Valor_Descuento_Cupon), 0))
        END AS Ajuste_Valor_Descuento_Cupon,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Valor_Descuento_Proveedor), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Valor_Descuento_Proveedor - b.Proyec_Valor_Descuento_Proveedor)/NULLIF(AVG(b.Proyec_Valor_Descuento_Proveedor), 0))
        END AS Ajuste_Valor_Descuento_Proveedor,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Valor_Descuento_Tercera_Edad), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Valor_Descuento_Tercera_Edad - b.Proyec_Valor_Descuento_Tercera_Edad)/NULLIF(AVG(b.Proyec_Valor_Descuento_Tercera_Edad), 0))
        END AS Ajuste_Valor_Descuento_Tercera_Edad,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Conteo_Transacciones), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Conteo_Transacciones - b.Proyec_Conteo_Transacciones)/NULLIF(AVG(b.Proyec_Conteo_Transacciones), 0))
        END AS Ajuste_Conteo_Transacciones,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Conteo_Trx_Es_Tercera_Edad), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Conteo_Trx_Es_Tercera_Edad - b.Proyec_Conteo_Trx_Es_Tercera_Edad)/NULLIF(AVG(b.Proyec_Conteo_Trx_Es_Tercera_Edad), 0))
        END AS Ajuste_Conteo_Trx_Es_Tercera_Edad,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Conteo_Trx_Es_Asegurado), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Conteo_Trx_Es_Asegurado - b.Proyec_Conteo_Trx_Es_Asegurado)/NULLIF(AVG(b.Proyec_Conteo_Trx_Es_Asegurado), 0))
        END AS Ajuste_Conteo_Trx_Es_Asegurado,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Conteo_Trx_Acumula_Monedero), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Conteo_Trx_Acumula_Monedero - b.Proyec_Conteo_Trx_Acumula_Monedero)/NULLIF(AVG(b.Proyec_Conteo_Trx_Acumula_Monedero), 0))
        END AS Ajuste_Conteo_Trx_Acumula_Monedero,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Conteo_Trx_Contiene_Farma), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Conteo_Trx_Contiene_Farma - b.Proyec_Conteo_Trx_Contiene_Farma)/NULLIF(AVG(b.Proyec_Conteo_Trx_Contiene_Farma), 0))
        END AS Ajuste_Conteo_Trx_Contiene_Farma,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Cantidad_Unidades_Relativa), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Cantidad_Unidades_Relativa - b.Proyec_Cantidad_Unidades_Relativa)/NULLIF(AVG(b.Proyec_Cantidad_Unidades_Relativa), 0))
        END AS Ajuste_Cantidad_Unidades_Relativa,
        CASE 
            WHEN NULLIF(AVG(b.Proyec_Segundos_Transaccion_Estimado), 0) IS NULL THEN 1
            ELSE 1 + (AVG(a.Real_Segundos_Transaccion_Estimado - b.Proyec_Segundos_Transaccion_Estimado)/NULLIF(AVG(b.Proyec_Segundos_Transaccion_Estimado), 0))
        END AS Ajuste_Segundos_Transaccion_Estimado
    FROM DatosReales a
    JOIN DatosProyectados b
    ON a.Emp_Id = b.Emp_Id
    AND a.Suc_Id = b.Suc_Id
    AND a.Fecha_Id = b.Fecha_Id
    GROUP BY a.Emp_Id, a.Suc_Id
),
-- Limitar los factores de ajuste para evitar correcciones extremas
AjustesLimitados AS (
    SELECT
        Emp_Id,
        Suc_Id,
        -- Limitar los ajustes a un rango razonable (entre 0.5 y 1.5)
        CASE WHEN 1.5>Ajuste_Cantidad_Padre AND Ajuste_Cantidad_Padre >0.5 THEN Ajuste_Cantidad_Padre ELSE 1 END AS Ajuste_Cantidad_Padre,
        CASE WHEN 1.5>Ajuste_Cantidad_Articulos AND Ajuste_Cantidad_Articulos >0.5 THEN Ajuste_Cantidad_Articulos ELSE 1 END AS Ajuste_Cantidad_Articulos,
        CASE WHEN 1.5>Ajuste_Valor_Bruto AND Ajuste_Valor_Bruto >0.5 THEN Ajuste_Valor_Bruto ELSE 1 END AS Ajuste_Valor_Bruto,
        CASE WHEN 1.5>Ajuste_Valor_Neto AND Ajuste_Valor_Neto >0.5 THEN Ajuste_Valor_Neto ELSE 1 END AS Ajuste_Valor_Neto,
        CASE WHEN 1.5>Ajuste_Valor_Costo AND Ajuste_Valor_Costo >0.5 THEN Ajuste_Valor_Costo ELSE 1 END AS Ajuste_Valor_Costo,
        CASE WHEN 1.5>Ajuste_Valor_Descuento AND Ajuste_Valor_Descuento >0.5 THEN Ajuste_Valor_Descuento ELSE 1 END AS Ajuste_Valor_Descuento,
        CASE WHEN 1.5>Ajuste_Valor_Descuento_Financiero AND Ajuste_Valor_Descuento_Financiero >0.5 THEN Ajuste_Valor_Descuento_Financiero ELSE 1 END AS Ajuste_Valor_Descuento_Financiero,
        CASE WHEN 1.5>Ajuste_Valor_Acum_Monedero AND Ajuste_Valor_Acum_Monedero >0.5 THEN Ajuste_Valor_Acum_Monedero ELSE 1 END AS Ajuste_Valor_Acum_Monedero,
        CASE WHEN 1.5>Ajuste_Valor_Descuento_Cupon AND Ajuste_Valor_Descuento_Cupon >0.5 THEN Ajuste_Valor_Descuento_Cupon ELSE 1 END AS Ajuste_Valor_Descuento_Cupon,
        CASE WHEN 1.5>Ajuste_Valor_Descuento_Proveedor AND Ajuste_Valor_Descuento_Proveedor >0.5 THEN Ajuste_Valor_Descuento_Proveedor ELSE 1 END AS Ajuste_Valor_Descuento_Proveedor,
        CASE WHEN 1.5>Ajuste_Valor_Descuento_Tercera_Edad AND Ajuste_Valor_Descuento_Tercera_Edad >0.5 THEN Ajuste_Valor_Descuento_Tercera_Edad ELSE 1 END AS Ajuste_Valor_Descuento_Tercera_Edad,
        CASE WHEN 1.5>Ajuste_Conteo_Transacciones AND Ajuste_Conteo_Transacciones >0.5 THEN Ajuste_Conteo_Transacciones ELSE 1 END AS Ajuste_Conteo_Transacciones,
        CASE WHEN 1.5>Ajuste_Conteo_Trx_Es_Tercera_Edad AND Ajuste_Conteo_Trx_Es_Tercera_Edad >0.5 THEN Ajuste_Conteo_Trx_Es_Tercera_Edad ELSE 1 END AS Ajuste_Conteo_Trx_Es_Tercera_Edad,
        CASE WHEN 1.5>Ajuste_Conteo_Trx_Es_Asegurado AND Ajuste_Conteo_Trx_Es_Asegurado >0.5 THEN Ajuste_Conteo_Trx_Es_Asegurado ELSE 1 END AS Ajuste_Conteo_Trx_Es_Asegurado,
        CASE WHEN 1.5>Ajuste_Conteo_Trx_Acumula_Monedero AND Ajuste_Conteo_Trx_Acumula_Monedero >0.5 THEN Ajuste_Conteo_Trx_Acumula_Monedero ELSE 1 END AS Ajuste_Conteo_Trx_Acumula_Monedero,
        CASE WHEN 1.5>Ajuste_Conteo_Trx_Contiene_Farma AND Ajuste_Conteo_Trx_Contiene_Farma >0.5 THEN Ajuste_Conteo_Trx_Contiene_Farma ELSE 1 END AS Ajuste_Conteo_Trx_Contiene_Farma,
        CASE WHEN 1.5>Ajuste_Cantidad_Unidades_Relativa AND Ajuste_Cantidad_Unidades_Relativa >0.5 THEN Ajuste_Cantidad_Unidades_Relativa ELSE 1 END AS Ajuste_Cantidad_Unidades_Relativa,
        CASE WHEN 1.5>Ajuste_Segundos_Transaccion_Estimado AND Ajuste_Segundos_Transaccion_Estimado >0.5 THEN Ajuste_Segundos_Transaccion_Estimado ELSE 1 END AS Ajuste_Segundos_Transaccion_Estimado
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
        -- Aplicar factores de ajuste a todas las métricas
        CAST(c.Cantidad_Padre * ISNULL(al.Ajuste_Cantidad_Padre, 1) AS DECIMAL(16,6)) AS Cantidad_Padre,
        CAST(c.Cantidad_Articulos * ISNULL(al.Ajuste_Cantidad_Articulos, 1) AS DECIMAL(16,6)) AS Cantidad_Articulos,
        CAST(c.Valor_Bruto * ISNULL(al.Ajuste_Valor_Bruto, 1) AS DECIMAL(16,6)) AS Valor_Bruto,
        CAST(c.Valor_Neto * ISNULL(al.Ajuste_Valor_Neto, 1) AS DECIMAL(16,6)) AS Valor_Neto,
        CAST(c.Valor_Costo * ISNULL(al.Ajuste_Valor_Costo, 1) AS DECIMAL(16,6)) AS Valor_Costo,
        CAST(c.Valor_Descuento * ISNULL(al.Ajuste_Valor_Descuento, 1) AS DECIMAL(16,6)) AS Valor_Descuento,
        CAST(c.Valor_Descuento_Financiero * ISNULL(al.Ajuste_Valor_Descuento_Financiero, 1) AS DECIMAL(16,6)) AS Valor_Descuento_Financiero,
        CAST(c.Valor_Acum_Monedero * ISNULL(al.Ajuste_Valor_Acum_Monedero, 1) AS DECIMAL(16,6)) AS Valor_Acum_Monedero,
        CAST(c.Valor_Descuento_Cupon * ISNULL(al.Ajuste_Valor_Descuento_Cupon, 1) AS DECIMAL(16,6)) AS Valor_Descuento_Cupon,
        CAST(c.Valor_Descuento_Proveedor * ISNULL(al.Ajuste_Valor_Descuento_Proveedor, 1) AS DECIMAL(16,6)) AS Valor_Descuento_Proveedor,
        CAST(c.Valor_Descuento_Tercera_Edad * ISNULL(al.Ajuste_Valor_Descuento_Tercera_Edad, 1) AS DECIMAL(16,6)) AS Valor_Descuento_Tercera_Edad,
        CAST(c.Conteo_Transacciones * ISNULL(al.Ajuste_Conteo_Transacciones, 1) AS DECIMAL(16,6)) AS Conteo_Transacciones,
        CAST(c.Conteo_Trx_Es_Tercera_Edad * ISNULL(al.Ajuste_Conteo_Trx_Es_Tercera_Edad, 1) AS DECIMAL(16,6)) AS Conteo_Trx_Es_Tercera_Edad,
        CAST(c.Conteo_Trx_Es_Asegurado * ISNULL(al.Ajuste_Conteo_Trx_Es_Asegurado, 1) AS DECIMAL(16,6)) AS Conteo_Trx_Es_Asegurado,
        CAST(c.Conteo_Trx_Acumula_Monedero * ISNULL(al.Ajuste_Conteo_Trx_Acumula_Monedero, 1) AS DECIMAL(16,6)) AS Conteo_Trx_Acumula_Monedero,
        CAST(c.Conteo_Trx_Contiene_Farma * ISNULL(al.Ajuste_Conteo_Trx_Contiene_Farma, 1) AS DECIMAL(16,6)) AS Conteo_Trx_Contiene_Farma,
        CAST(c.Cantidad_Unidades_Relativa * ISNULL(al.Ajuste_Cantidad_Unidades_Relativa, 1) AS DECIMAL(16,6)) AS Cantidad_Unidades_Relativa,
        CAST(c.Segundos_Transaccion_Estimado * ISNULL(al.Ajuste_Segundos_Transaccion_Estimado, 1) AS DECIMAL(16,6)) AS Segundos_Transaccion_Estimado
    FROM {{ref('BI_Kielsa_Hecho_ProyeccionVenta_BaseMes_SucHora_Staging')}} c
    LEFT JOIN AjustesLimitados al
    ON c.Emp_Id = al.Emp_Id
    AND c.Suc_Id = al.Suc_Id
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
        Cantidad_Padre,
        Cantidad_Articulos,
        Valor_Bruto,
        Valor_Neto,
        Valor_Costo,
        Valor_Descuento,
        Valor_Descuento_Financiero,
        Valor_Acum_Monedero,
        Valor_Descuento_Cupon,
        Valor_Descuento_Proveedor,
        Valor_Descuento_Tercera_Edad,
        Conteo_Transacciones,
        Conteo_Trx_Es_Tercera_Edad,
        Conteo_Trx_Es_Asegurado,
        Conteo_Trx_Acumula_Monedero,
        Conteo_Trx_Contiene_Farma,
        Cantidad_Unidades_Relativa,
        Segundos_Transaccion_Estimado,
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