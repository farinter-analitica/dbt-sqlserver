{%- set unique_key_list = ["Sociedad_Id", "Gpo_Plan_Id"] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "automation/periodo_mensual_inicio", "periodo_unico/si"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
		]
		
) }}

-- Modelo incremental para la tabla GposPlan de PLANNING_DB.
-- Incluye todos los campos y respeta los defaults y claves del modelo fuente.

SELECT
    ISNULL(sociedad_id COLLATE DATABASE_DEFAULT, '') AS Sociedad_Id,
    ISNULL(gpo_plan_id COLLATE DATABASE_DEFAULT, '') AS Gpo_Plan_Id,
    ISNULL(gpo_plan_nombre COLLATE DATABASE_DEFAULT, '') AS Gpo_Plan_Nombre,
    GETDATE() AS Fecha_Actualizado
FROM {{ var('P_SQLLDSUBS_LS') }}.{{ source('PLANNING_DB', 'GposPlan') }}
