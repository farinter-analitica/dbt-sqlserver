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

{% set v_fecha_inicio = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=31)).strftime('%Y%m%d') %}
{% set v_fecha_fin = (modules.datetime.datetime.now() + modules.datetime.timedelta(days=210)).strftime('%Y%m%d') %}
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
    "Segundos_Transaccion_Estimado",
    "Segundos_Actividad_Estimado"
] %}

WITH Calculo AS (
    SELECT --TOP (1000) 
        ISNULL(PR.Emp_Id, 0) AS Emp_Id,
        ISNULL(PR.Suc_Id, 0) AS Suc_Id,
        ISNULL(CAL.Fecha_Calendario, '1999-01-01') AS [Fecha_Id],
        ISNULL(PDSH.Hora_Id, 0) AS Hora_Id,
        ISNULL(PDS.Dia_Semana_Iso_Id, 0) AS Dia_Semana_Iso_Id,
        {%- for field in metric_fields %}
            CAST(
                (CASE WHEN CAL.Es_Dia_Feriado = 1 THEN PFER.Prom_{{ field }} ELSE PR.Prom_{{ field }} END)
                * ISNULL(PDS.Peso_{{ field }}, 1) * ISNULL(PDSH.Part_{{ field }}, 1)
                * ISNULL(PM.Peso_{{ field }}, 1) * ISNULL(TVH.Crec_{{ field }}, 1) AS DECIMAL(16, 6))
                AS {{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    FROM {{ ref ('BI_Kielsa_Agr_Sucursal_PromDiaBaseMesesProyec') }} AS PR
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa' ) }} AS EMP
        ON PR.Emp_Id = EMP.Empresa_Id
    INNER JOIN {{ source ('BI_FARINTER', 'BI_Dim_Pais' ) }} AS PAIS
        ON EMP.Pais_Id = PAIS.Pais_Id
    INNER JOIN {{ ref('BI_Dim_Calendario_LaboralPais') }} AS CAL
        ON
            CAL.[Fecha_Calendario] >= '{{ v_fecha_inicio }}' AND CAL.[Fecha_Calendario] < '{{ v_fecha_fin }}'
            AND PAIS.Pais_ISO2 = CAL.Pais_ISO2
    INNER JOIN {{ ref ('BI_Kielsa_Agr_Sucursal_PartDiaSemana') }} AS PDS
        ON
            PR.Emp_Id = PDS.Emp_Id AND PR.Suc_Id = PDS.Suc_Id
            AND CAL.Dia_de_la_Semana = PDS.Dia_Semana_Iso_Id
    INNER JOIN {{ ref ('BI_Kielsa_Agr_Sucursal_PartDiaSemanaHora') }} AS PDSH
        ON
            PR.Emp_Id = PDSH.Emp_Id AND PR.Suc_Id = PDSH.Suc_Id
            AND CAL.Dia_de_la_Semana = PDSH.Dia_Semana_Iso_Id
    LEFT JOIN {{ ref('BI_Kielsa_Agr_Sucursal_PartMes') }} AS PM
        ON
            PR.Emp_Id = PM.Emp_Id
            AND PR.Suc_Id = PM.Suc_Id
            AND CAL.Mes_Calendario = PM.Mes_Id
    --OJO: Solo puedes incluir un solo crecimiento o un promedio de crecimientos en una proyeccion
    INNER JOIN {{ ref('BI_Kielsa_Agr_Sucursal_CrecVsHist_Semana') }} AS TVH
        ON PR.Emp_Id = TVH.Emp_Id AND PR.Suc_Id = TVH.Suc_Id
    LEFT JOIN {{ ref ('BI_Kielsa_Agr_Sucursal_PromDiaFeriados') }} AS PFER
        ON PR.Emp_Id = PFER.Emp_Id AND PR.Suc_Id = PFER.Suc_Id
    LEFT JOIN {{ ref ('BI_Kielsa_Dim_Sucursal_Horario_DiaSemana') }} AS HSUC
        ON
            PR.Emp_Id = HSUC.Emp_Id AND PR.Suc_Id = HSUC.Suc_Id
            AND CAL.Dia_de_la_Semana = HSUC.Dia_Semana_Iso_Id
    WHERE
        HSUC.Dia_Semana_Iso_Id IS NULL OR HSUC.Es_24_Horas = 1
        OR PDSH.Hora_Id BETWEEN DATEPART(HOUR, HSUC.H_Apertura) AND HSUC.Horas_Cero_Hasta_Cierre
)

SELECT
    *,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=19, table_alias='') }} AS [EmpSuc_Id]
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
