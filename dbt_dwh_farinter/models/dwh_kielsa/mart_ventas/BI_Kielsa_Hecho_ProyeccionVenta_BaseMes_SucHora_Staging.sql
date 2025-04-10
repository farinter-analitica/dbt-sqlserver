{% set unique_key_list = ["Emp_Id","Suc_Id","Fecha_Id","Hora_Id"] %}
{{ 
    config(
		tags=["automation/periodo_semanal_1", "periodo_unico/si",  "automation_only"],
		materialized="view",
		unique_key=unique_key_list,
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}

{% set v_fecha_inicio = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=31)).strftime('%Y%m%d') %}
{% set v_fecha_fin = (modules.datetime.datetime.now() + modules.datetime.timedelta(days=120)).strftime('%Y%m%d') %}
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
    SELECT --TOP (1000) 
        ISNULL(PR.Emp_Id,0) AS Emp_Id,
        ISNULL(PR.Suc_Id,0) AS Suc_Id,
        ISNULL(CAL.Fecha_Calendario,'1999-01-01') AS [Fecha_Id],
        ISNULL(PDSH.Hora_Id,0) AS Hora_Id,
        ISNULL(PDS.Dia_Semana_Iso_Id,0) AS Dia_Semana_Iso_Id,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Cantidad_Padre ELSE PR.Prom_Cantidad_Padre END)
            *ISNULL(PDS.Peso_Cantidad_Padre,1)*ISNULL(PDSH.Part_Cantidad_Padre,1)
            *ISNULL(PM.Peso_Cantidad_Padre,1)*ISNULL(TVH.Crec_Cantidad_Padre,1) AS DECIMAL(16,6)) AS Cantidad_Padre,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Cantidad_Articulos ELSE PR.Prom_Cantidad_Articulos END)
            *ISNULL(PDS.Peso_Cantidad_Articulos,1)*ISNULL(PDSH.Part_Cantidad_Articulos,1)
            *ISNULL(PM.Peso_Cantidad_Articulos,1)*ISNULL(TVH.Crec_Cantidad_Articulos,1) AS DECIMAL(16,6)) AS Cantidad_Articulos,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Bruto ELSE PR.Prom_Valor_Bruto END)
            *ISNULL(PDS.Peso_Valor_Bruto,1)*ISNULL(PDSH.Part_Valor_Bruto,1)
            *ISNULL(PM.Peso_Valor_Bruto,1)*ISNULL(TVH.Crec_Valor_Bruto,1) AS DECIMAL(16,6)) AS Valor_Bruto,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Neto ELSE PR.Prom_Valor_Neto END)
            *ISNULL(PDS.Peso_Valor_Neto,1)*ISNULL(PDSH.Part_Valor_Neto,1)
            *ISNULL(PM.Peso_Valor_Neto,1)*ISNULL(TVH.Crec_Valor_Neto,1) AS DECIMAL(16,6)) AS Valor_Neto,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Costo ELSE PR.Prom_Valor_Costo END)
            *ISNULL(PDS.Peso_Valor_Costo,1)*ISNULL(PDSH.Part_Valor_Costo,1)
            *ISNULL(PM.Peso_Valor_Costo,1)*ISNULL(TVH.Crec_Valor_Costo,1) AS DECIMAL(16,6)) AS Valor_Costo,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento ELSE PR.Prom_Valor_Descuento END)
            *ISNULL(PDS.Peso_Valor_Descuento,1)*ISNULL(PDSH.Part_Valor_Descuento,1)
            *ISNULL(PM.Peso_Valor_Descuento,1)*ISNULL(TVH.Crec_Valor_Descuento,1) AS DECIMAL(16,6)) AS Valor_Descuento,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento_Financiero ELSE PR.Prom_Valor_Descuento_Financiero END)
            *ISNULL(PDS.Peso_Valor_Descuento_Financiero,1)*ISNULL(PDSH.Part_Valor_Descuento_Financiero,1)
            *ISNULL(PM.Peso_Valor_Descuento_Financiero,1)*ISNULL(TVH.Crec_Valor_Descuento_Financiero,1) AS DECIMAL(16,6)) AS Valor_Descuento_Financiero,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Acum_Monedero ELSE PR.Prom_Valor_Acum_Monedero END)
            *ISNULL(PDS.Peso_Valor_Acum_Monedero,1)*ISNULL(PDSH.Part_Valor_Acum_Monedero,1)
            *ISNULL(PM.Peso_Valor_Acum_Monedero,1)*ISNULL(TVH.Crec_Valor_Acum_Monedero,1) AS DECIMAL(16,6)) AS Valor_Acum_Monedero,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento_Cupon ELSE PR.Prom_Valor_Descuento_Cupon END)
            *ISNULL(PDS.Peso_Valor_Descuento_Cupon,1)*ISNULL(PDSH.Part_Valor_Descuento_Cupon,1)
            *ISNULL(PM.Peso_Valor_Descuento_Cupon,1)*ISNULL(TVH.Crec_Valor_Descuento_Cupon,1) AS DECIMAL(16,6)) AS Valor_Descuento_Cupon,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento_Proveedor ELSE PR.Prom_Valor_Descuento_Proveedor END)
            *ISNULL(PDS.Peso_Valor_Descuento_Proveedor,1)*ISNULL(PDSH.Part_Valor_Descuento_Proveedor,1)
            *ISNULL(PM.Peso_Valor_Descuento_Proveedor,1)*ISNULL(TVH.Crec_Valor_Descuento_Proveedor,1) AS DECIMAL(16,6)) AS Valor_Descuento_Proveedor,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Valor_Descuento_Tercera_Edad ELSE PR.Prom_Valor_Descuento_Tercera_Edad END)
            *ISNULL(PDS.Peso_Valor_Descuento_Tercera_Edad,1)*ISNULL(PDSH.Part_Valor_Descuento_Tercera_Edad,1)
            *ISNULL(PM.Peso_Valor_Descuento_Tercera_Edad,1)*ISNULL(TVH.Crec_Valor_Descuento_Tercera_Edad,1) AS DECIMAL(16,6)) AS Valor_Descuento_Tercera_Edad,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Conteo_Transacciones ELSE PR.Prom_Conteo_Transacciones END)
            *ISNULL(PDS.Peso_Conteo_Transacciones,1)*ISNULL(PDSH.Part_Conteo_Transacciones,1)
            *ISNULL(PM.Peso_Conteo_Transacciones,1)*ISNULL(TVH.Crec_Conteo_Transacciones,1) AS DECIMAL(16,6)) AS Conteo_Transacciones,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Conteo_Trx_Es_Tercera_Edad ELSE PR.Prom_Conteo_Trx_Es_Tercera_Edad END)
            *ISNULL(PDS.Peso_Conteo_Trx_Es_Tercera_Edad,1)*ISNULL(PDSH.Part_Conteo_Trx_Es_Tercera_Edad,1)
            *ISNULL(PM.Peso_Conteo_Trx_Es_Tercera_Edad,1)*ISNULL(TVH.Crec_Conteo_Trx_Es_Tercera_Edad,1) AS DECIMAL(16,6)) AS Conteo_Trx_Es_Tercera_Edad,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Conteo_Trx_Es_Asegurado ELSE PR.Prom_Conteo_Trx_Es_Asegurado END)
            *ISNULL(PDS.Peso_Conteo_Trx_Es_Asegurado,1)*ISNULL(PDSH.Part_Conteo_Trx_Es_Asegurado,1)
            *ISNULL(PM.Peso_Conteo_Trx_Es_Asegurado,1)*ISNULL(TVH.Crec_Conteo_Trx_Es_Asegurado,1) AS DECIMAL(16,6)) AS Conteo_Trx_Es_Asegurado,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Conteo_Trx_Acumula_Monedero ELSE PR.Prom_Conteo_Trx_Acumula_Monedero END)
            *ISNULL(PDS.Peso_Conteo_Trx_Acumula_Monedero,1)*ISNULL(PDSH.Part_Conteo_Trx_Acumula_Monedero,1)
            *ISNULL(PM.Peso_Conteo_Trx_Acumula_Monedero,1)*ISNULL(TVH.Crec_Conteo_Trx_Acumula_Monedero,1) AS DECIMAL(16,6)) AS Conteo_Trx_Acumula_Monedero,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Conteo_Trx_Contiene_Farma ELSE PR.Prom_Conteo_Trx_Contiene_Farma END)
            *ISNULL(PDS.Peso_Conteo_Trx_Contiene_Farma,1)*ISNULL(PDSH.Part_Conteo_Trx_Contiene_Farma,1)
            *ISNULL(PM.Peso_Conteo_Trx_Contiene_Farma,1)*ISNULL(TVH.Crec_Conteo_Trx_Contiene_Farma,1) AS DECIMAL(16,6)) AS Conteo_Trx_Contiene_Farma,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Cantidad_Unidades_Relativa ELSE PR.Prom_Cantidad_Unidades_Relativa END)
            *ISNULL(PDS.Peso_Cantidad_Unidades_Relativa,1)*ISNULL(PDSH.Part_Cantidad_Unidades_Relativa,1)
            *ISNULL(PM.Peso_Cantidad_Unidades_Relativa,1)*ISNULL(TVH.Crec_Cantidad_Unidades_Relativa,1) AS DECIMAL(16,6)) AS Cantidad_Unidades_Relativa,
        CAST((CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_Segundos_Transaccion_Estimado ELSE PR.Prom_Segundos_Transaccion_Estimado END)
            *ISNULL(PDS.Peso_Segundos_Transaccion_Estimado,1)*ISNULL(PDSH.Part_Segundos_Transaccion_Estimado,1)
            *ISNULL(PM.Peso_Segundos_Transaccion_Estimado,1)*ISNULL(TVH.Crec_Segundos_Transaccion_Estimado,1) AS DECIMAL(16,6)) AS Segundos_Transaccion_Estimado
    FROM {{ ref ('BI_Kielsa_Agr_Sucursal_PromDiaBaseMesesProyec') }} PR
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
        INNER JOIN {{ ref ('BI_Kielsa_Agr_Sucursal_PartDiaSemanaHora') }} PDSH
            ON PDSH.Emp_Id = PR.Emp_Id AND PDSH.Suc_Id = PR.Suc_Id
            AND PDSH.Dia_Semana_Iso_Id = CAL.Dia_de_la_Semana
        INNER JOIN {{ ref('BI_Kielsa_Agr_Sucursal_PartMes') }} PM
            ON PM.Emp_Id = PR.Emp_Id 
            AND PM.Suc_Id = PR.Suc_Id
            AND CAL.Mes_Calendario = PM.Mes_Id
        --OJO: Solo puedes incluir un solo crecimiento o un promedio de crecimientos en una proyeccion
        INNER JOIN {{ ref('BI_Kielsa_Agr_Sucursal_CrecVsHist_Semana') }} TVH
            ON TVH.Emp_Id = PR.Emp_Id AND TVH.Suc_Id = PR.Suc_Id
        LEFT JOIN {{ ref ('BI_Kielsa_Agr_Sucursal_PromDiaFeriados') }} PFER
            ON PFER.Emp_Id = PR.Emp_Id AND PFER.Suc_Id = PR.Suc_Id
)
SELECT *,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=19, table_alias='')}} [EmpSuc_Id]
FROM Calculo
    


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