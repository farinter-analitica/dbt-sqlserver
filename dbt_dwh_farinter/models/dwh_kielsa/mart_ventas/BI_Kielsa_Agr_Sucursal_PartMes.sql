{% set unique_key_list = ["Emp_Id","Suc_Id","Mes_Id"] %}
{{ 
    config(
        as_columnstore=true,
		tags=["automation/periodo_mensual_inicio", "periodo_unico/si",  "automation_only"],
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
{% set v_meses_completos = 24 %}
{% set v_fecha_base = (modules.datetime.datetime.now().replace(day=1) - modules.datetime.timedelta(days=1)).replace(day=1) %}
{% set v_fecha_inicio_dt = (v_fecha_base.replace(year=v_fecha_base.year - (v_meses_completos // 12), month=v_fecha_base.month - (v_meses_completos % 12))) %}
{% set v_fecha_fin_dt = modules.datetime.datetime.now().replace(day=1) %}
{% set v_fecha_inicio = v_fecha_inicio_dt.strftime('%Y%m%d') %}
{% set v_fecha_fin = v_fecha_fin_dt.strftime('%Y%m%d') %}
{% set v_anio_mes_inicio = v_fecha_inicio[:6] %}

{#TODO: Hacer que las sucursales sin suficientes meses usen una aproximacion distinta #}
WITH ResumenBase AS (
    SELECT 
        Emp_Id,
        Suc_Id,
        MONTH(FP.Factura_Fecha) as Mes_Id_Original,
        COUNT(DISTINCT FP.Factura_Fecha)/365.0 AS Anios_Muestra,
        ISNULL(SUM(FP.Sum_Cantidad_Padre),0)*1.0 AS Sum_Cantidad_Padre,
        ISNULL(SUM(FP.Sum_Cantidad_Articulos),0)*1.0 AS Sum_Cantidad_Articulos,
        ISNULL(SUM(FP.Sum_Valor_Bruto),0)*1.0 AS Sum_Valor_Bruto,
        ISNULL(SUM(FP.Sum_Valor_Neto),0)*1.0 AS Sum_Valor_Neto,
        ISNULL(SUM(FP.Sum_Valor_Costo),0)*1.0 AS Sum_Valor_Costo,
        ISNULL(SUM(FP.Sum_Valor_Descuento),0)*1.0 AS Sum_Valor_Descuento,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Financiero),0)*1.0 AS Sum_Valor_Descuento_Financiero,
        ISNULL(SUM(FP.Sum_Valor_Acum_Monedero),0)*1.0 AS Sum_Valor_Acum_Monedero,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Cupon),0)*1.0 AS Sum_Valor_Descuento_Cupon,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Proveedor),0)*1.0 AS Sum_Valor_Descuento_Proveedor,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Tercera_Edad),0)*1.0 AS Sum_Valor_Descuento_Tercera_Edad,
        ISNULL(SUM(FP.Sum_Conteo_Transacciones),0)*1.0 AS Sum_Conteo_Transacciones,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Es_Tercera_Edad),0)*1.0 AS Sum_Conteo_Trx_Es_Tercera_Edad,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Es_Asegurado),0)*1.0 AS Sum_Conteo_Trx_Es_Asegurado,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Acumula_Monedero),0)*1.0 AS Sum_Conteo_Trx_Acumula_Monedero,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Contiene_Farma),0)*1.0 AS Sum_Conteo_Trx_Contiene_Farma,
        ISNULL(SUM(FP.Sum_Cantidad_Unidades_Relativa),0)*1.0 AS Sum_Cantidad_Unidades_Relativa,
        ISNULL(SUM(FP.Sum_Segundos_Transaccion_Estimado),0)*1.0 AS Sum_Segundos_Transaccion_Estimado
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
    CAST(Sum_Cantidad_Padre / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Padre,
    CAST(Sum_Cantidad_Articulos / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Articulos,
    CAST(Sum_Valor_Bruto / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Bruto,
    CAST(Sum_Valor_Neto / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Neto,
    CAST(Sum_Valor_Costo / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Costo,
    CAST(Sum_Valor_Descuento / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento,
    CAST(Sum_Valor_Descuento_Financiero / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Financiero,
    CAST(Sum_Valor_Acum_Monedero / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Acum_Monedero,
    CAST(Sum_Valor_Descuento_Cupon / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Cupon,
    CAST(Sum_Valor_Descuento_Proveedor / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Proveedor,
    CAST(Sum_Valor_Descuento_Tercera_Edad / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Valor_Descuento_Tercera_Edad,
    CAST(Sum_Conteo_Transacciones / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Transacciones,
    CAST(Sum_Conteo_Trx_Es_Tercera_Edad / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Es_Tercera_Edad,
    CAST(Sum_Conteo_Trx_Es_Asegurado / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Es_Asegurado,
    CAST(Sum_Conteo_Trx_Acumula_Monedero / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Acumula_Monedero,
    CAST(Sum_Conteo_Trx_Contiene_Farma / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Conteo_Trx_Contiene_Farma,
    CAST(Sum_Cantidad_Unidades_Relativa / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Cantidad_Unidades_Relativa,
    CAST(Sum_Segundos_Transaccion_Estimado / Anios_Muestra AS DECIMAL(16,6)) AS Prom_Segundos_Transaccion_Estimado,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Cantidad_Padre / NULLIF(SUM(Sum_Cantidad_Padre) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Cantidad_Padre,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Cantidad_Articulos / NULLIF(SUM(Sum_Cantidad_Articulos) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Cantidad_Articulos,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Valor_Bruto / NULLIF(SUM(Sum_Valor_Bruto) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Valor_Bruto,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Valor_Neto / NULLIF(SUM(Sum_Valor_Neto) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Valor_Neto,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Valor_Costo / NULLIF(SUM(Sum_Valor_Costo) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Valor_Costo,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Valor_Descuento / NULLIF(SUM(Sum_Valor_Descuento) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Valor_Descuento,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Valor_Descuento_Financiero / NULLIF(SUM(Sum_Valor_Descuento_Financiero) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Valor_Descuento_Financiero,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Valor_Acum_Monedero / NULLIF(SUM(Sum_Valor_Acum_Monedero) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Valor_Acum_Monedero,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Valor_Descuento_Cupon / NULLIF(SUM(Sum_Valor_Descuento_Cupon) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Valor_Descuento_Cupon,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Valor_Descuento_Proveedor / NULLIF(SUM(Sum_Valor_Descuento_Proveedor) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Valor_Descuento_Proveedor,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Valor_Descuento_Tercera_Edad / NULLIF(SUM(Sum_Valor_Descuento_Tercera_Edad) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Valor_Descuento_Tercera_Edad,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1 
        THEN Sum_Conteo_Transacciones / NULLIF(SUM(Sum_Conteo_Transacciones) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Conteo_Transacciones,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1
        THEN Sum_Conteo_Trx_Es_Tercera_Edad / NULLIF(SUM(Sum_Conteo_Trx_Es_Tercera_Edad) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Conteo_Trx_Es_Tercera_Edad,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1
        THEN Sum_Conteo_Trx_Es_Asegurado / NULLIF(SUM(Sum_Conteo_Trx_Es_Asegurado) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Conteo_Trx_Es_Asegurado,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1
        THEN Sum_Conteo_Trx_Acumula_Monedero / NULLIF(SUM(Sum_Conteo_Trx_Acumula_Monedero) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Conteo_Trx_Acumula_Monedero,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1
        THEN Sum_Conteo_Trx_Contiene_Farma / NULLIF(SUM(Sum_Conteo_Trx_Contiene_Farma) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Conteo_Trx_Contiene_Farma,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1
        THEN Sum_Cantidad_Unidades_Relativa / NULLIF(SUM(Sum_Cantidad_Unidades_Relativa) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Cantidad_Unidades_Relativa,
    ISNULL(CAST(CASE WHEN Es_Sucursal_Meses_Completos = 1
        THEN Sum_Segundos_Transaccion_Estimado / NULLIF(SUM(Sum_Segundos_Transaccion_Estimado) OVER(PARTITION BY Emp_Id, Suc_Id),0) ELSE 1/12 END AS DECIMAL(16,12)),0) AS Part_Segundos_Transaccion_Estimado
FROM CompletarMeses
)
SELECT *,
    CASE WHEN Part_Cantidad_Padre*12 > 1.05 THEN 1.05 WHEN Part_Cantidad_Padre*12 <0.95 THEN 0.95 ELSE Part_Cantidad_Padre*12  END AS Peso_Cantidad_Padre,
    CASE WHEN Part_Cantidad_Articulos*12 > 1.05 THEN 1.05 WHEN Part_Cantidad_Articulos*12 <0.95 THEN 0.95 ELSE Part_Cantidad_Articulos*12  END AS Peso_Cantidad_Articulos,
    CASE WHEN Part_Valor_Bruto*12 > 1.05 THEN 1.05 WHEN Part_Valor_Bruto*12 <0.95 THEN 0.95 ELSE Part_Valor_Bruto*12  END AS Peso_Valor_Bruto,
    CASE WHEN Part_Valor_Neto*12 > 1.05 THEN 1.05 WHEN Part_Valor_Neto*12 <0.95 THEN 0.95 ELSE Part_Valor_Neto*12  END AS Peso_Valor_Neto,
    CASE WHEN Part_Valor_Costo*12 > 1.05 THEN 1.05 WHEN Part_Valor_Costo*12 <0.95 THEN 0.95 ELSE Part_Valor_Costo*12  END AS Peso_Valor_Costo,
    CASE WHEN Part_Valor_Descuento*12 > 1.05 THEN 1.05 WHEN Part_Valor_Descuento*12 <0.95 THEN 0.95 ELSE Part_Valor_Descuento*12  END AS Peso_Valor_Descuento,
    CASE WHEN Part_Valor_Descuento_Financiero*12 > 1.05 THEN 1.05 WHEN Part_Valor_Descuento_Financiero*12 <0.95 THEN 0.95 ELSE Part_Valor_Descuento_Financiero*12  END AS Peso_Valor_Descuento_Financiero,
    CASE WHEN Part_Valor_Acum_Monedero*12 > 1.05 THEN 1.05 WHEN Part_Valor_Acum_Monedero*12 <0.95 THEN 0.95 ELSE Part_Valor_Acum_Monedero*12  END AS Peso_Valor_Acum_Monedero,
    CASE WHEN Part_Valor_Descuento_Cupon*12 > 1.05 THEN 1.05 WHEN Part_Valor_Descuento_Cupon*12 <0.95 THEN 0.95 ELSE Part_Valor_Descuento_Cupon*12  END AS Peso_Valor_Descuento_Cupon,
    CASE WHEN Part_Valor_Descuento_Proveedor*12 > 1.05 THEN 1.05 WHEN Part_Valor_Descuento_Proveedor*12 <0.95 THEN 0.95 ELSE Part_Valor_Descuento_Proveedor*12  END AS Peso_Valor_Descuento_Proveedor,
    CASE WHEN Part_Valor_Descuento_Tercera_Edad*12 > 1.05 THEN 1.05 WHEN Part_Valor_Descuento_Tercera_Edad*12 <0.95 THEN 0.95 ELSE Part_Valor_Descuento_Tercera_Edad*12  END AS Peso_Valor_Descuento_Tercera_Edad,
    CASE WHEN Part_Conteo_Transacciones*12 > 1.05 THEN 1.05 WHEN Part_Conteo_Transacciones*12 <0.95 THEN 0.95 ELSE Part_Conteo_Transacciones*12  END AS Peso_Conteo_Transacciones,
    CASE WHEN Part_Conteo_Trx_Es_Tercera_Edad*12 > 1.05 THEN 1.05 WHEN Part_Conteo_Trx_Es_Tercera_Edad*12 <0.95 THEN 0.95 ELSE Part_Conteo_Trx_Es_Tercera_Edad*12  END AS Peso_Conteo_Trx_Es_Tercera_Edad,
    CASE WHEN Part_Conteo_Trx_Es_Asegurado*12 > 1.05 THEN 1.05 WHEN Part_Conteo_Trx_Es_Asegurado*12 <0.95 THEN 0.95 ELSE Part_Conteo_Trx_Es_Asegurado*12  END AS Peso_Conteo_Trx_Es_Asegurado,
    CASE WHEN Part_Conteo_Trx_Acumula_Monedero*12 > 1.05 THEN 1.05 WHEN Part_Conteo_Trx_Acumula_Monedero*12 <0.95 THEN 0.95 ELSE Part_Conteo_Trx_Acumula_Monedero*12  END AS Peso_Conteo_Trx_Acumula_Monedero,
    CASE WHEN Part_Conteo_Trx_Contiene_Farma*12 > 1.05 THEN 1.05 WHEN Part_Conteo_Trx_Contiene_Farma*12 <0.95 THEN 0.95 ELSE Part_Conteo_Trx_Contiene_Farma*12  END AS Peso_Conteo_Trx_Contiene_Farma,
    CASE WHEN Part_Cantidad_Unidades_Relativa*12 > 1.05 THEN 1.05 WHEN Part_Cantidad_Unidades_Relativa*12 <0.95 THEN 0.95 ELSE Part_Cantidad_Unidades_Relativa*12  END AS Peso_Cantidad_Unidades_Relativa,
    CASE WHEN Part_Segundos_Transaccion_Estimado*12 > 1.05 THEN 1.05 WHEN Part_Segundos_Transaccion_Estimado*12 <0.95 THEN 0.95 ELSE Part_Segundos_Transaccion_Estimado*12  END AS Peso_Segundos_Transaccion_Estimado
FROM Metricas