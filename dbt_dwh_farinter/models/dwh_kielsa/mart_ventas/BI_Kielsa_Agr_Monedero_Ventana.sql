{% set unique_key_list = ["Monedero_Id","Emp_Id"] %}
{% set v_merge_exclude_columns = unique_key_list + ["Fecha_Carga"] %}
{% set v_merge_check_diff_exclude_columns = v_merge_exclude_columns + ["Fecha_Actualizado"] %}
{% set v_last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, 0, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{% set v_last_date_control = (modules.datetime.datetime.fromisoformat(v_last_date) - modules.datetime.timedelta(days=30)).strftime('%Y%m%d') %}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns= v_merge_exclude_columns,
		merge_check_diff_exclude_columns= v_merge_check_diff_exclude_columns,
		post_hook=[
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
		]
		
) }}

WITH
Fecha_Max AS (
    SELECT
        CAST(MAX(Factura_Fecha) AS DATE) AS Fecha_Max,
        Emp_Id,
        MAX(AnioMes_Id) AS AnioMes_Id_Max
    FROM {{ ref("BI_Kielsa_Hecho_FacturaEncabezado") }}
    WHERE AnioMes_Id >= '{{ v_last_date_control[0:6] }}' AND Factura_Fecha >= '{{ v_last_date_control }}'
    GROUP BY Emp_ID
),

Monederos AS (
    --Usamos de base para poder usar diferentes metricas
    SELECT
        M.Monedero_Id,
        M.Emp_Id
    FROM {{ ref("BI_Kielsa_Dim_Monedero") }} M
)

SELECT
    ISNULL(M.Monedero_Id, '') Monedero_Id,
    ISNULL(M.Emp_Id, 0) Emp_Id,
    ISNULL(MUM.Dias_Con_Compra, 0) Dias_Con_Compra,
    ISNULL(MUM.Meses_Con_Compra, 0) AS Meses_Con_Compra,
    ISNULL(CASE
        WHEN MUM.Dias_Con_Compra <= 1 THEN NULL
        ELSE
            DATEDIFF(DAY, MUM.Fecha_Primera_Factura, FM.Fecha_Max)
            / NULLIF(MUM.Dias_Con_Compra - 1, 0)
    END, NULL) AS Promedio_Dias_Entre_Compra,
    ISNULL(MUM.Fecha_Primera_Factura, '19000101') AS Fecha_Primera_Factura,
    ISNULL(MUM.Fecha_Ultima_Factura, '19000101') AS Fecha_Ultima_Factura,
    ISNULL(MDS.Dia_Semana_Iso, 0) AS Dia_Semana_Iso_Preferido,
    ISNULL(MDS.Indicador_Validez_Estadistica, 0) AS Dia_Semana_Validez_Estadistica,
    ISNULL(MUM.Dia_Minimo_Mes, 0) AS Dia_Minimo_Mes,
    ISNULL(MUM.Dia_Maximo_Mes, 0) AS Dia_Maximo_Mes,
    ISNULL(MUM.Dia_Promedio_Mes, 0) AS Dia_Promedio_Mes,
    ISNULL(MUM.Dia_Mes_Validez_Estadistica, 0) AS Dia_Mes_Validez_Estadistica,
    ISNULL(MSS.Sucursal_Id, 0) AS Sucursal_Id_Preferido,
    ISNULL(MSS.Indicador_Validez_Estadistica, 0) AS Sucursal_Validez_Estadistica,
    ISNULL(MHS.Hora_Id, 0) AS Hora_Id_Preferido,
    ISNULL(MHS.Indicador_Validez_Estadistica, 0) AS Hora_Validez_Estadistica,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Monedero_Id'], input_length=49, table_alias='M') }} [EmpMon_Id],
    GETDATE() AS Fecha_Actualizado,
    ISNULL(MUM.Ventana_Desde, MDS.Ventana_Desde) AS Ventana_Desde
FROM Monederos M
LEFT JOIN Fecha_Max FM
    ON FM.Emp_Id = M.Emp_Id
LEFT JOIN {{ ref('BI_Kielsa_Agr_Monedero_Metricas_Ventana') }} MUM
    ON
        MUM.Monedero_Id = M.Monedero_Id
        AND MUM.Emp_Id = M.Emp_Id
LEFT JOIN {{ ref('BI_Kielsa_Agr_Monedero_DiaSemana_Ventana') }} MDS
    ON
        MDS.Monedero_Id = M.Monedero_Id
        AND MDS.Emp_Id = M.Emp_Id
        AND MDS.Ranking = 1
LEFT JOIN {{ ref('BI_Kielsa_Agr_Monedero_Sucursal_Ventana') }} MSS
    ON
        MSS.Monedero_Id = M.Monedero_Id
        AND MSS.Emp_Id = M.Emp_Id
        AND MSS.Ranking = 1
LEFT JOIN {{ ref('BI_Kielsa_Agr_Monedero_Hora_Ventana') }} MHS
    ON
        MHS.Monedero_Id = M.Monedero_Id
        AND MHS.Emp_Id = M.Emp_Id
        AND MHS.Ranking = 1

WHERE
    MUM.Monedero_Id IS NOT NULL
    OR MDS.Monedero_Id IS NOT NULL
