{% set unique_key_list = ["Emp_Id","Suc_Id","Articulo_Id","Fecha_Id","CanalVenta_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

{% set v_fecha_inicio = modules.datetime.datetime.now().strftime('%Y%m%d') %}
{% set v_fecha_fin = (modules.datetime.datetime.now().replace(day=1) + modules.datetime.timedelta(days=32)).replace(day=1).strftime('%Y%m%d') %}

/*
--1. Pesos de cada dia de la semana por sucursal, valor y peso
DECLARE @Inicio AS DATE = GETDATE()
DECLARE @SemanasPonderacion AS INT = 12
DECLARE @DiasPonderacion AS INT = @SemanasPonderacion*7 --Historia para ponderar
DROP TABLE IF EXISTS #Temp
;*/
SELECT --TOP (1000) 
	ISNULL(PR.Emp_Id,0) AS Emp_Id,
      ISNULL(PR.Suc_Id,0) AS Suc_Id,
      ISNULL(PCA.CanalVenta_Id ,0) AS CanalVenta_Id,
      ISNULL(PCA.Articulo_Id,0) AS Articulo_Id,
	  ISNULL(CAL.Fecha_Calendario,'1999-01-01') AS [Fecha_Id],
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Cantidad_Padre ELSE PR.Prom_Cantidad_Padre END)
        *ISNULL(PDS.Part_Cantidad_Padre,1)*PCA.Part_Cantidad_Padre  AS DECIMAL(16,6)) AS Cantidad_Padre,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Bruto ELSE PR.Prom_Valor_Bruto END)
        *ISNULL(PDS.Part_Valor_Bruto,1)*PCA.Part_Valor_Bruto  AS DECIMAL(16,6)) AS Valor_Bruto,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Neto ELSE PR.Prom_Valor_Neto END)
        *ISNULL(PDS.Part_Valor_Neto,1)*PCA.Part_Valor_Neto  AS DECIMAL(16,6)) AS Valor_Neto,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Costo ELSE PR.Prom_Valor_Costo END)
        *ISNULL(PDS.Part_Valor_Costo,1)*PCA.Part_Valor_Costo  AS DECIMAL(16,6)) AS Valor_Costo,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento ELSE PR.Prom_Valor_Descuento END)
        *ISNULL(PDS.Part_Valor_Descuento,1)*PCA.Part_Valor_Descuento  AS DECIMAL(16,6)) AS Valor_Descuento,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento_Financiero ELSE PR.Prom_Valor_Descuento_Financiero END)
        *ISNULL(PDS.Part_Valor_Descuento_Financiero,1)*PCA.Part_Valor_Descuento_Financiero  AS DECIMAL(16,6)) AS Valor_Descuento_Financiero,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Acum_Monedero ELSE PR.Prom_Valor_Acum_Monedero END)
        *ISNULL(PDS.Part_Valor_Acum_Monedero,1)*PCA.Part_Valor_Acum_Monedero  AS DECIMAL(16,6)) AS Valor_Acum_Monedero,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento_Cupon ELSE PR.Prom_Valor_Descuento_Cupon END)
        *ISNULL(PDS.Part_Valor_Descuento_Cupon,1)*PCA.Part_Valor_Descuento_Cupon  AS DECIMAL(16,6)) AS Valor_Descuento_Cupon,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento_Proveedor ELSE PR.Prom_Valor_Descuento_Proveedor END)
        *ISNULL(PDS.Part_Valor_Descuento_Proveedor,1)*PCA.Part_Valor_Descuento_Proveedor  AS DECIMAL(16,6)) AS Valor_Descuento_Proveedor,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento_Tercera_Edad ELSE PR.Prom_Valor_Descuento_Tercera_Edad END)
        *ISNULL(PDS.Part_Valor_Descuento_Tercera_Edad,1)*PCA.Part_Valor_Descuento_Tercera_Edad  AS DECIMAL(16,6)) AS Valor_Descuento_Tercera_Edad,
    CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Conteo_Transacciones ELSE PR.Prom_Conteo_Transacciones END)
        *ISNULL(PDS.Part_Conteo_Transacciones,1)*PCA.Part_Conteo_Transacciones  AS DECIMAL(16,6)) AS Conteo_Transacciones

  FROM {{ ref ('BI_Kielsa_Agr_Sucursal_PromDiaBaseProyec') }} PR
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa' ) }} EMP
        ON EMP.Empresa_Id = PR.Emp_Id
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Dim_Pais' ) }} PAIS
        ON PAIS.Pais_Id = EMP.Pais_Id
    INNER JOIN {{ ref('BI_Dim_Calendario_LaboralPais') }} CAL
        on CAL.[Fecha_Calendario] >= '{{ v_fecha_inicio }}' AND CAL.[Fecha_Calendario] < '{{ v_fecha_fin }}'
        AND PAIS.Pais_ISO2 = CAL.Pais_ISO2
    INNER JOIN {{ ref ('BI_Kielsa_Agr_Sucursal_PartDiaSemana') }} PDS
        ON PDS.Emp_Id = PR.Emp_Id AND PDS.Suc_Id = PR.Suc_Id
        AND PDS.Dia_Semana_Iso_Id = CAL.Dia_de_la_Semana
    INNER JOIN {{ ref ('BI_Kielsa_Agr_Sucursal_PartDiaCanArt') }} PCA
        ON PCA.Emp_Id = PR.Emp_Id AND PCA.Suc_Id = PR.Suc_Id
    LEFT JOIN {{ ref ('BI_Kielsa_Agr_Sucursal_PromDiaFeriados') }} PFER
        ON PFER.Emp_Id = PR.Emp_Id AND PFER.Suc_Id = PR.Suc_Id

    