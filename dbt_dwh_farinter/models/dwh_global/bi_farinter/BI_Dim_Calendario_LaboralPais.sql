{% set unique_key_list = ["Pais_ISO2","Fecha_Calendario"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/diario"],
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

SELECT *
	, DATEDIFF(HOUR,Hora_Inicio,Hora_Fin) as Horas_Laborales

FROM
(
SELECT * 
	,CAST(CASE WHEN Es_Dia_Habil = 0 THEN NULL
		ELSE '08:00' END AS TIME(0))  AS Hora_Inicio
	,CAST(CASE WHEN Es_Dia_Habil = 0 THEN NULL
		WHEN Dia_de_la_Semana = 6 THEN '12:00'
		ELSE '17:00' END AS TIME(0))  AS Hora_Fin	
FROM
(
SELECT 
ISNULL(P.Pais_ISO2,'') AS Pais_ISO2
,ISNULL(CD.Fecha_Calendario,'19000101') AS Fecha_Calendario
,CASE WHEN CD.Es_Dia_Habil = 0 THEN 0 
	WHEN CD.NoLaboral_Paises LIKE '%"'+P.Pais_ISO2+'"%' THEN 0
	ELSE 1 END AS Es_Dia_Habil
,CASE WHEN CD.NoLaboral_Paises LIKE '%"'+P.Pais_ISO2+'"%' THEN 1
	ELSE 0 END AS Es_Dia_Feriado
,CD.Mes_Calendario
,CD.Dia_de_la_Semana
,CD.NoLaboral_Paises
,CD.Mes_Relativo
,CD.Anio_Relativo
,CD.AnioMes_Id
,GETDATE() AS [Fecha_Actualizado]
FROM {{ ref('BI_Dim_Calendario_Dinamico') }} CD
CROSS JOIN (SELECT P.* FROM BI_Dim_Pais P WHERE Pais_Id<>0 ) P

WHERE CD.Anio_Relativo BETWEEN -5 AND 2) CL ) CLF
--SELECT * FROM [dbo].[BI_Dim_Calendario_Dinamico] WHERE Anio_Calendario = 2023
--SELECT * FROM [dbo].[BI_Dim_Calendario_LaboralPais] WHERE NoLaboral_Paises IS NOT NULL

