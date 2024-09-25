{% set unique_key_list = ["AnioMes_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
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

--DBT DAGSTER

--20240903: Creado

SELECT DISTINCT
	C.[Anio_Calendario]
	, C.[Mes_Calendario]
	, C.Trimestre_Calendario
	, C.AnioMes_Id
	, C.Mes_Inicio
	, C.Mes_Fin
	, C.Dias_en_Mes
	, C.Mes_Nombre
	, Mes_Nombre_Corto
	, [Es_Bisiesto]
	, [YYYYMM]
	, Periodo_Anual
	, Periodo_Anual_Orden
	, Periodo_Mensual
	, Periodo_Mensual_Orden
	, Meses_Desde_Fecha_Procesado
	, Años_Desde_Fecha_Procesado
	, GETDATE() AS Fecha_Procesado
	, [Mes_Relativo]
	, [Anio_Relativo]
    , [Mes_NN_Relativo]
	, [Mes_Relativo_Tipo_Semestral]
	, [Mes_Relativo_Tipo_Trimestral]
    , Ciclo12_N
    , Ciclo12_Periodo
FROM	{{ ref ('BI_Dim_Calendario_Dinamico') }} C
--WHERE Es_Inicio_Mes=1

/*
SELECT TOP 100 * FROM BI_Dim_Calendario_Dinamico WHERE Mes_Relativo BETWEEN -12 AND  12 
	and abs(Mes_Relativo) <> abs(SUBSTRING(Mes_NN_Relativo,5,5)) ORDER BY Fecha_Calendario 
*/