{% set unique_key_list = ["Emp_Id","Suc_Id"] %}
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
{% set v_semanas_completas = 4 %}
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


WITH ResumenBase AS (
    SELECT 
        FP.Emp_Id,
        FP.Suc_Id,
        C.Anio_ISO AS Anio_Id,
        C.Semana_del_Anio_ISO as Semana_del_Anio_ISO_Original,
        MIN(C.Fecha_Calendario) AS Fecha_Inicio_Semana,
        ISNULL(SUM(FP.Sum_Cantidad_Padre),0)*1.0 AS Sum_Cantidad_Padre,
        ISNULL(SUM(FP.Sum_Cantidad_Articulos),0)*1.0 AS Sum_Cantidad_Articulos,
        ISNULL(SUM(FP.Sum_Valor_Bruto),0)*1.0 AS Sum_Valor_Bruto,
        ISNULL(SUM(FP.Sum_Valor_Neto),0)*1.0 AS Sum_Valor_Neto,
        ISNULL(SUM(FP.Sum_Valor_Costo),0)*1.0 AS Sum_Valor_Costo,
        ISNULL(SUM(FP.Sum_Valor_Descuento),0)*1.0 AS Sum_Valor_Descuento,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Financiero),0)*1.0 AS Sum_Valor_Descuento_Financiero,
        ISNULL(SUM(FP.Sum_Valor_Acum_Monedero),0)*1.0 AS Sum_Valor_Acum_Monedero,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Cupon),0)*1.0 AS Sum_Valor_Descuento_Cupon,
        ISNULL(SUM(FP.Sum_Descuento_Proveedor),0)*1.0 AS Sum_Descuento_Proveedor,
        ISNULL(SUM(FP.Sum_Valor_Descuento_Tercera_Edad),0)*1.0 AS Sum_Valor_Descuento_Tercera_Edad,
        ISNULL(SUM(FP.Sum_Conteo_Transacciones),0)*1.0 AS Sum_Conteo_Transacciones,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Es_Tercera_Edad),0)*1.0 AS Sum_Conteo_Trx_Es_Tercera_Edad,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Es_Asegurado),0)*1.0 AS Sum_Conteo_Trx_Es_Asegurado,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Acumula_Monedero),0)*1.0 AS Sum_Conteo_Trx_Acumula_Monedero,
        ISNULL(SUM(FP.Sum_Conteo_Trx_Contiene_Farma),0)*1.0 AS Sum_Conteo_Trx_Contiene_Farma,
        ISNULL(SUM(FP.Sum_Cantidad_Unidades_Relativa),0)*1.0 AS Sum_Cantidad_Unidades_Relativa,
        ISNULL(SUM(FP.Sum_Segundos_Transaccion_Estimado),0)*1.0 AS Sum_Segundos_Transaccion_Estimado
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
        COUNT(DISTINCT Anio_Id) AS Anios_Muestra,
        CASE WHEN COUNT(DISTINCT Anio_Id)  <  {{ v_anios_historicos }} THEN 0 ELSE 1 END AS Es_Sucursal_Anios_Completos,
        SUM(Sum_Cantidad_Padre) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Cantidad_Padre,
        SUM(Sum_Cantidad_Articulos) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Cantidad_Articulos,
        SUM(Sum_Valor_Bruto) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Valor_Bruto,
        SUM(Sum_Valor_Neto) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Valor_Neto,
        SUM(Sum_Valor_Costo) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Valor_Costo,
        SUM(Sum_Valor_Descuento) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Valor_Descuento,
        SUM(Sum_Valor_Descuento_Financiero) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Valor_Descuento_Financiero,
        SUM(Sum_Valor_Acum_Monedero) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Valor_Acum_Monedero,
        SUM(Sum_Valor_Descuento_Cupon) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Valor_Descuento_Cupon,
        SUM(Sum_Descuento_Proveedor) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Valor_Descuento_Proveedor,
        SUM(Sum_Valor_Descuento_Tercera_Edad) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Valor_Descuento_Tercera_Edad,
        SUM(Sum_Conteo_Transacciones) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Conteo_Transacciones,
        SUM(Sum_Conteo_Trx_Es_Tercera_Edad) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Conteo_Trx_Es_Tercera_Edad,
        SUM(Sum_Conteo_Trx_Es_Asegurado) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Conteo_Trx_Es_Asegurado,
        SUM(Sum_Conteo_Trx_Acumula_Monedero) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Conteo_Trx_Acumula_Monedero,
        SUM(Sum_Conteo_Trx_Contiene_Farma) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Conteo_Trx_Contiene_Farma,
        SUM(Sum_Cantidad_Unidades_Relativa) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Cantidad_Unidades_Relativa,
        SUM(Sum_Segundos_Transaccion_Estimado) / {{ v_anios_historicos*v_semanas_completas }} AS Prom_Segundos_Transaccion_Estimado
    FROM ResumenBase
    WHERE Fecha_Inicio_Semana < '{{ v_fecha_inicio_actual }}'
    GROUP BY Emp_Id, Suc_Id
),
Actual AS
(
    SELECT 
        Emp_Id,
        Suc_Id,
        SUM(Sum_Cantidad_Padre) / {{ v_semanas_completas }} AS Prom_Cantidad_Padre,
        SUM(Sum_Cantidad_Articulos) / {{ v_semanas_completas }} AS Prom_Cantidad_Articulos,
        SUM(Sum_Valor_Bruto) / {{ v_semanas_completas }} AS Prom_Valor_Bruto,
        SUM(Sum_Valor_Neto) / {{ v_semanas_completas }} AS Prom_Valor_Neto,
        SUM(Sum_Valor_Costo) / {{ v_semanas_completas }} AS Prom_Valor_Costo,
        SUM(Sum_Valor_Descuento) / {{ v_semanas_completas }} AS Prom_Valor_Descuento,
        SUM(Sum_Valor_Descuento_Financiero) / {{ v_semanas_completas }} AS Prom_Valor_Descuento_Financiero,
        SUM(Sum_Valor_Acum_Monedero) / {{ v_semanas_completas }} AS Prom_Valor_Acum_Monedero,
        SUM(Sum_Valor_Descuento_Cupon) / {{ v_semanas_completas }} AS Prom_Valor_Descuento_Cupon,
        SUM(Sum_Descuento_Proveedor) / {{ v_semanas_completas }} AS Prom_Valor_Descuento_Proveedor,
        SUM(Sum_Valor_Descuento_Tercera_Edad) / {{ v_semanas_completas }} AS Prom_Valor_Descuento_Tercera_Edad,
        SUM(Sum_Conteo_Transacciones) / {{ v_semanas_completas }} AS Prom_Conteo_Transacciones,
        SUM(Sum_Conteo_Trx_Es_Tercera_Edad) / {{ v_semanas_completas }} AS Prom_Conteo_Trx_Es_Tercera_Edad,
        SUM(Sum_Conteo_Trx_Es_Asegurado) / {{ v_semanas_completas }} AS Prom_Conteo_Trx_Es_Asegurado,
        SUM(Sum_Conteo_Trx_Acumula_Monedero) / {{ v_semanas_completas }} AS Prom_Conteo_Trx_Acumula_Monedero,
        SUM(Sum_Conteo_Trx_Contiene_Farma) / {{ v_semanas_completas }} AS Prom_Conteo_Trx_Contiene_Farma,
        SUM(Sum_Cantidad_Unidades_Relativa) / {{ v_semanas_completas }} AS Prom_Cantidad_Unidades_Relativa,
        SUM(Sum_Segundos_Transaccion_Estimado) / {{ v_semanas_completas }} AS Prom_Segundos_Transaccion_Estimado
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
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Cantidad_Padre,0) - Historico.Prom_Cantidad_Padre 
            ELSE 0 END AS Dif_Cantidad_Padre,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1
            THEN ISNULL(Actual.Prom_Cantidad_Articulos,0) - Historico.Prom_Cantidad_Articulos
            ELSE 0 END AS Dif_Cantidad_Articulos,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Valor_Bruto,0) - Historico.Prom_Valor_Bruto 
            ELSE 0 END AS Dif_Valor_Bruto,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Valor_Neto,0) - Historico.Prom_Valor_Neto 
            ELSE 0 END AS Dif_Valor_Neto,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Valor_Costo,0) - Historico.Prom_Valor_Costo 
            ELSE 0 END AS Dif_Valor_Costo,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Valor_Descuento,0) - Historico.Prom_Valor_Descuento 
            ELSE 0 END AS Dif_Valor_Descuento,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Valor_Descuento_Financiero,0) - Historico.Prom_Valor_Descuento_Financiero 
            ELSE 0 END AS Dif_Valor_Descuento_Financiero,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Valor_Acum_Monedero,0) - Historico.Prom_Valor_Acum_Monedero 
            ELSE 0 END AS Dif_Valor_Acum_Monedero,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Valor_Descuento_Cupon,0) - Historico.Prom_Valor_Descuento_Cupon 
            ELSE 0 END AS Dif_Valor_Descuento_Cupon,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Valor_Descuento_Proveedor,0) - Historico.Prom_Valor_Descuento_Proveedor 
            ELSE 0 END AS Dif_Valor_Descuento_Proveedor,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Valor_Descuento_Tercera_Edad,0) - Historico.Prom_Valor_Descuento_Tercera_Edad 
            ELSE 0 END AS Dif_Valor_Descuento_Tercera_Edad,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1 
            THEN ISNULL(Actual.Prom_Conteo_Transacciones,0) - Historico.Prom_Conteo_Transacciones 
            ELSE 0 END AS Dif_Conteo_Transacciones,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1
            THEN ISNULL(Actual.Prom_Conteo_Trx_Es_Tercera_Edad,0) - Historico.Prom_Conteo_Trx_Es_Tercera_Edad
            ELSE 0 END AS Dif_Conteo_Trx_Es_Tercera_Edad,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1
            THEN ISNULL(Actual.Prom_Conteo_Trx_Es_Asegurado,0) - Historico.Prom_Conteo_Trx_Es_Asegurado
            ELSE 0 END AS Dif_Conteo_Trx_Es_Asegurado,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1
            THEN ISNULL(Actual.Prom_Conteo_Trx_Acumula_Monedero,0) - Historico.Prom_Conteo_Trx_Acumula_Monedero
            ELSE 0 END AS Dif_Conteo_Trx_Acumula_Monedero,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1
            THEN ISNULL(Actual.Prom_Conteo_Trx_Contiene_Farma,0) - Historico.Prom_Conteo_Trx_Contiene_Farma
            ELSE 0 END AS Dif_Conteo_Trx_Contiene_Farma,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1
            THEN ISNULL(Actual.Prom_Cantidad_Unidades_Relativa,0) - Historico.Prom_Cantidad_Unidades_Relativa
            ELSE 0 END AS Dif_Cantidad_Unidades_Relativa,
        CASE WHEN Historico.Es_Sucursal_Anios_Completos =1
            THEN ISNULL(Actual.Prom_Segundos_Transaccion_Estimado,0) - Historico.Prom_Segundos_Transaccion_Estimado
            ELSE 0 END AS Dif_Segundos_Transaccion_Estimado,
        ISNULL(Actual.Prom_Cantidad_Padre,0) as Prom_Cantidad_Padre_Actual,
        ISNULL(Actual.Prom_Cantidad_Articulos,0) as Prom_Cantidad_Articulos_Actual,
        ISNULL(Actual.Prom_Valor_Bruto,0) as Prom_Valor_Bruto_Actual,
        ISNULL(Actual.Prom_Valor_Neto,0) as Prom_Valor_Neto_Actual,
        ISNULL(Actual.Prom_Valor_Costo,0) as Prom_Valor_Costo_Actual,
        ISNULL(Actual.Prom_Valor_Descuento,0) as Prom_Valor_Descuento_Actual,
        ISNULL(Actual.Prom_Valor_Descuento_Financiero,0) as Prom_Valor_Descuento_Financiero_Actual,
        ISNULL(Actual.Prom_Valor_Acum_Monedero,0) as Prom_Valor_Acum_Monedero_Actual,
        ISNULL(Actual.Prom_Valor_Descuento_Cupon,0) as Prom_Valor_Descuento_Cupon_Actual,
        ISNULL(Actual.Prom_Valor_Descuento_Proveedor,0) as Prom_Valor_Descuento_Proveedor_Actual,
        ISNULL(Actual.Prom_Valor_Descuento_Tercera_Edad,0) as Prom_Valor_Descuento_Tercera_Edad_Actual,
        ISNULL(Actual.Prom_Conteo_Transacciones,0) as Prom_Conteo_Transacciones_Actual,
        ISNULL(Actual.Prom_Conteo_Trx_Es_Tercera_Edad,0) as Prom_Conteo_Trx_Es_Tercera_Edad_Actual,
        ISNULL(Actual.Prom_Conteo_Trx_Es_Asegurado,0) as Prom_Conteo_Trx_Es_Asegurado_Actual,
        ISNULL(Actual.Prom_Conteo_Trx_Acumula_Monedero,0) as Prom_Conteo_Trx_Acumula_Monedero_Actual,
        ISNULL(Actual.Prom_Conteo_Trx_Contiene_Farma,0) as Prom_Conteo_Trx_Contiene_Farma_Actual,
        ISNULL(Actual.Prom_Cantidad_Unidades_Relativa,0) as Prom_Cantidad_Unidades_Relativa_Actual,
        ISNULL(Actual.Prom_Segundos_Transaccion_Estimado,0) as Prom_Segundos_Transaccion_Estimado_Actual,
        ISNULL(Historico.Prom_Cantidad_Padre,0) as Prom_Cantidad_Padre_Historico,
        ISNULL(Historico.Prom_Valor_Bruto,0) as Prom_Valor_Bruto_Historico,
        ISNULL(Historico.Prom_Valor_Neto,0) as Prom_Valor_Neto_Historico,
        ISNULL(Historico.Prom_Valor_Costo,0) as Prom_Valor_Costo_Historico,
        ISNULL(Historico.Prom_Valor_Descuento,0) as Prom_Valor_Descuento_Historico,
        ISNULL(Historico.Prom_Valor_Descuento_Financiero,0) as Prom_Valor_Descuento_Financiero_Historico,
        ISNULL(Historico.Prom_Valor_Acum_Monedero,0) as Prom_Valor_Acum_Monedero_Historico,
        ISNULL(Historico.Prom_Valor_Descuento_Cupon,0) as Prom_Valor_Descuento_Cupon_Historico,
        ISNULL(Historico.Prom_Valor_Descuento_Proveedor,0) as Prom_Valor_Descuento_Proveedor_Historico,
        ISNULL(Historico.Prom_Valor_Descuento_Tercera_Edad,0) as Prom_Valor_Descuento_Tercera_Edad_Historico,
        ISNULL(Historico.Prom_Conteo_Transacciones,0) as Prom_Conteo_Transacciones_Historico,
        ISNULL(Historico.Prom_Conteo_Trx_Es_Tercera_Edad,0) as Prom_Conteo_Trx_Es_Tercera_Edad_Historico,
        ISNULL(Historico.Prom_Conteo_Trx_Es_Asegurado,0) as Prom_Conteo_Trx_Es_Asegurado_Historico,
        ISNULL(Historico.Prom_Conteo_Trx_Acumula_Monedero,0) as Prom_Conteo_Trx_Acumula_Monedero_Historico,
        ISNULL(Historico.Prom_Conteo_Trx_Contiene_Farma,0) as Prom_Conteo_Trx_Contiene_Farma_Historico,
        ISNULL(Historico.Prom_Cantidad_Unidades_Relativa,0) as Prom_Cantidad_Unidades_Relativa_Historico,
        ISNULL(Historico.Prom_Segundos_Transaccion_Estimado,0) as Prom_Segundos_Transaccion_Estimado_Historico

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
        CASE WHEN Prom_Cantidad_Padre_Actual > 0 
            THEN  (1 + Dif_Cantidad_Padre / Prom_Cantidad_Padre_Actual)
            ELSE 1 END AS Crec_Cantidad_Padre,
        CASE WHEN Prom_Cantidad_Articulos_Actual > 0
            THEN  (1 + Dif_Cantidad_Articulos / Prom_Cantidad_Articulos_Actual)
            ELSE 1 END AS Crec_Cantidad_Articulos,
        CASE WHEN Prom_Valor_Bruto_Actual > 0 
            THEN  (1 + Dif_Valor_Bruto / Prom_Valor_Bruto_Actual)
            ELSE 1 END AS Crec_Valor_Bruto,
        CASE WHEN Prom_Valor_Neto_Actual > 0 
            THEN  (1 + Dif_Valor_Neto / Prom_Valor_Neto_Actual)
            ELSE 1 END AS Crec_Valor_Neto,
        CASE WHEN Prom_Valor_Costo_Actual > 0 
            THEN  (1 + Dif_Valor_Costo / Prom_Valor_Costo_Actual)
            ELSE 1 END AS Crec_Valor_Costo,
        CASE WHEN Prom_Valor_Descuento_Actual > 0 
            THEN  (1 + Dif_Valor_Descuento / Prom_Valor_Descuento_Actual)
            ELSE 1 END AS Crec_Valor_Descuento,
        CASE WHEN Prom_Valor_Descuento_Financiero_Actual > 0 
            THEN  (1 + Dif_Valor_Descuento_Financiero / Prom_Valor_Descuento_Financiero_Actual)
            ELSE 1 END AS Crec_Valor_Descuento_Financiero,
        CASE WHEN Prom_Valor_Acum_Monedero_Actual > 0 
            THEN  (1 + Dif_Valor_Acum_Monedero / Prom_Valor_Acum_Monedero_Actual)
            ELSE 1 END AS Crec_Valor_Acum_Monedero,
        CASE WHEN Prom_Valor_Descuento_Cupon_Actual > 0 
            THEN  (1 + Dif_Valor_Descuento_Cupon / Prom_Valor_Descuento_Cupon_Actual)
            ELSE 1 END AS Crec_Valor_Descuento_Cupon,
        CASE WHEN Prom_Valor_Descuento_Proveedor_Actual > 0 
            THEN  (1 + Dif_Valor_Descuento_Proveedor / Prom_Valor_Descuento_Proveedor_Actual)
            ELSE 1 END AS Crec_Valor_Descuento_Proveedor,
        CASE WHEN Prom_Valor_Descuento_Tercera_Edad_Actual > 0 
            THEN  (1 + Dif_Valor_Descuento_Tercera_Edad / Prom_Valor_Descuento_Tercera_Edad_Actual)
            ELSE 1 END AS Crec_Valor_Descuento_Tercera_Edad,
        CASE WHEN Prom_Conteo_Transacciones_Actual > 0 
            THEN  (1 + Dif_Conteo_Transacciones / Prom_Conteo_Transacciones_Actual)
            ELSE 1 END AS Crec_Conteo_Transacciones,
        CASE WHEN Prom_Conteo_Trx_Es_Tercera_Edad_Actual > 0
            THEN  (1 + Dif_Conteo_Trx_Es_Tercera_Edad / Prom_Conteo_Trx_Es_Tercera_Edad_Actual)
            ELSE 1 END AS Crec_Conteo_Trx_Es_Tercera_Edad,
        CASE WHEN Prom_Conteo_Trx_Es_Asegurado_Actual > 0
            THEN  (1 + Dif_Conteo_Trx_Es_Asegurado / Prom_Conteo_Trx_Es_Asegurado_Actual)
            ELSE 1 END AS Crec_Conteo_Trx_Es_Asegurado,
        CASE WHEN Prom_Conteo_Trx_Acumula_Monedero_Actual > 0
            THEN  (1 + Dif_Conteo_Trx_Acumula_Monedero / Prom_Conteo_Trx_Acumula_Monedero_Actual)
            ELSE 1 END AS Crec_Conteo_Trx_Acumula_Monedero,
        CASE WHEN Prom_Conteo_Trx_Contiene_Farma_Actual > 0
            THEN  (1 + Dif_Conteo_Trx_Contiene_Farma / Prom_Conteo_Trx_Contiene_Farma_Actual)
            ELSE 1 END AS Crec_Conteo_Trx_Contiene_Farma,
        CASE WHEN Prom_Cantidad_Unidades_Relativa_Actual > 0
            THEN  (1 + Dif_Cantidad_Unidades_Relativa / Prom_Cantidad_Unidades_Relativa_Actual)
            ELSE 1 END AS Crec_Cantidad_Unidades_Relativa,
        CASE WHEN Prom_Segundos_Transaccion_Estimado_Actual > 0
            THEN  (1 + Dif_Segundos_Transaccion_Estimado / Prom_Segundos_Transaccion_Estimado_Actual)
            ELSE 1 END AS Crec_Segundos_Transaccion_Estimado
    FROM Diferencias
)
SELECT Emp_Id,
        Suc_Id,
        Anios_Muestra,
        Es_Sucursal_Anios_Completos,
        CASE WHEN Crec_Cantidad_Padre<0 THEN 0 WHEN Crec_Cantidad_Padre>2 THEN 2 ELSE Crec_Cantidad_Padre END AS Crec_Cantidad_Padre,
        CASE WHEN Crec_Cantidad_Articulos<0 THEN 0 WHEN Crec_Cantidad_Articulos>2 THEN 2 ELSE Crec_Cantidad_Articulos END AS Crec_Cantidad_Articulos,
        CASE WHEN Crec_Valor_Bruto<0 THEN 0 WHEN Crec_Valor_Bruto>2 THEN 2 ELSE Crec_Valor_Bruto END AS Crec_Valor_Bruto,
        CASE WHEN Crec_Valor_Neto<0 THEN 0 WHEN Crec_Valor_Neto>2 THEN 2 ELSE Crec_Valor_Neto END AS Crec_Valor_Neto,
        CASE WHEN Crec_Valor_Costo<0 THEN 0 WHEN Crec_Valor_Costo>2 THEN 2 ELSE Crec_Valor_Costo END AS Crec_Valor_Costo,
        CASE WHEN Crec_Valor_Descuento<0 THEN 0 WHEN Crec_Valor_Descuento>2 THEN 2 ELSE Crec_Valor_Descuento END AS Crec_Valor_Descuento,
        CASE WHEN Crec_Valor_Descuento_Financiero<0 THEN 0 WHEN Crec_Valor_Descuento_Financiero>2 THEN 2 ELSE Crec_Valor_Descuento_Financiero END AS Crec_Valor_Descuento_Financiero,
        CASE WHEN Crec_Valor_Acum_Monedero<0 THEN 0 WHEN Crec_Valor_Acum_Monedero>2 THEN 2 ELSE Crec_Valor_Acum_Monedero END AS Crec_Valor_Acum_Monedero,
        CASE WHEN Crec_Valor_Descuento_Cupon<0 THEN 0 WHEN Crec_Valor_Descuento_Cupon>2 THEN 2 ELSE Crec_Valor_Descuento_Cupon END AS Crec_Valor_Descuento_Cupon,
        CASE WHEN Crec_Valor_Descuento_Proveedor<0 THEN 0 WHEN Crec_Valor_Descuento_Proveedor>2 THEN 2 ELSE Crec_Valor_Descuento_Proveedor END AS Crec_Valor_Descuento_Proveedor,
        CASE WHEN Crec_Valor_Descuento_Tercera_Edad<0 THEN 0 WHEN Crec_Valor_Descuento_Tercera_Edad>2 THEN 2 ELSE Crec_Valor_Descuento_Tercera_Edad END AS Crec_Valor_Descuento_Tercera_Edad,
        CASE WHEN Crec_Conteo_Transacciones<0 THEN 0 WHEN Crec_Conteo_Transacciones>2 THEN 2 ELSE Crec_Conteo_Transacciones END AS Crec_Conteo_Transacciones,
        CASE WHEN Crec_Conteo_Trx_Es_Tercera_Edad<0 THEN 0 WHEN Crec_Conteo_Trx_Es_Tercera_Edad>2 THEN 2 ELSE Crec_Conteo_Trx_Es_Tercera_Edad END AS Crec_Conteo_Trx_Es_Tercera_Edad,
        CASE WHEN Crec_Conteo_Trx_Es_Asegurado<0 THEN 0 WHEN Crec_Conteo_Trx_Es_Asegurado>2 THEN 2 ELSE Crec_Conteo_Trx_Es_Asegurado END AS Crec_Conteo_Trx_Es_Asegurado,
        CASE WHEN Crec_Conteo_Trx_Acumula_Monedero<0 THEN 0 WHEN Crec_Conteo_Trx_Acumula_Monedero>2 THEN 2 ELSE Crec_Conteo_Trx_Acumula_Monedero END AS Crec_Conteo_Trx_Acumula_Monedero,
        CASE WHEN Crec_Conteo_Trx_Contiene_Farma<0 THEN 0 WHEN Crec_Conteo_Trx_Contiene_Farma>2 THEN 2 ELSE Crec_Conteo_Trx_Contiene_Farma END AS Crec_Conteo_Trx_Contiene_Farma,
        CASE WHEN Crec_Cantidad_Unidades_Relativa<0 THEN 0 WHEN Crec_Cantidad_Unidades_Relativa>2 THEN 2 ELSE Crec_Cantidad_Unidades_Relativa END AS Crec_Cantidad_Unidades_Relativa,
        CASE WHEN Crec_Segundos_Transaccion_Estimado<0 THEN 0 WHEN Crec_Segundos_Transaccion_Estimado>2 THEN 2 ELSE Crec_Segundos_Transaccion_Estimado END AS Crec_Segundos_Transaccion_Estimado

FROM Tendencia