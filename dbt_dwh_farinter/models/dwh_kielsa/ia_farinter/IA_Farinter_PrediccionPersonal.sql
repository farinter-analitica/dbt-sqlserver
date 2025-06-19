{% set unique_key_list = ["Fecha_Id", "Suc_Id", "Hora", "Emp_Id"] %}

{{ 
	config(
		group="kielsa_analitica_atributos",
		as_columnstore=true,
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga", "Fecha_Actualizado"],
          tags=["automation/periodo_mensual_inicio", "periodo_unico/si", "automation_only"],
		post_hook=[
			"{{ dwh_farinter_remove_incremental_temp_table() }}",
			"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
		]
	)
}}
WITH 
ProyeccionConPersonal AS (
    SELECT 
        [Fecha_Id],
        hora_id [Hora],
        Dia_de_la_Semana [Dia_Semana],
        Conteo_Transacciones as [Cantidad_Transacciones],
        personal_recomendado AS [Personal_Necesario],
        personal_minimo_absoluto as personal_sin_redondeo,
        Segundos_Transaccion_Estimado,
        [Suc_Id],
        [Emp_Id]
    FROM {{ source('IA_FARINTER', 'IA_Kielsa_Proyeccion_Personal_Necesario') }}
    --WHERE Suc_Id=115
    --AND Fecha_Id>='20250324' AND Fecha_Id<'20250331'
	WHERE Emp_Id=1

)
SELECT [Fecha_Id],
    [Hora],
    [Dia_Semana],
    [Cantidad_Transacciones],
    [Personal_Necesario],
    [Suc_Id],
    [Emp_Id],
    GETDATE() AS Fecha_Actualizado
FROM ProyeccionConPersonal
