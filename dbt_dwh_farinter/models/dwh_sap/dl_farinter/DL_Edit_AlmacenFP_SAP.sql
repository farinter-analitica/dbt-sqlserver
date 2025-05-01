
{%- set unique_key_list = ["Centro_Id", "Almacen_Id"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
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

SELECT 
    ISNULL(almacen_id COLLATE DATABASE_DEFAULT, '') AS Almacen_Id,
    ISNULL(centro_id COLLATE DATABASE_DEFAULT, '') AS Centro_Id,
    ISNULL(almacen COLLATE DATABASE_DEFAULT, '') AS Almacen_Id_Solo,
    ISNULL(almacen_nombre_ref COLLATE DATABASE_DEFAULT, '') AS Almacen_Nombre_Ref,
    ISNULL(gpo_almacen COLLATE DATABASE_DEFAULT, '') AS Grupo_Almacen,
    ISNULL(tipo_almacen COLLATE DATABASE_DEFAULT, '') AS Tipo_Almacen,
    ISNULL(planificado COLLATE DATABASE_DEFAULT, 'N') AS Planificado,
    ISNULL(control_proximos_vencer COLLATE DATABASE_DEFAULT, 'N') AS Control_Proximos_Vencer,
    ISNULL(almacen_etiquetas COLLATE DATABASE_DEFAULT, '') AS Almacen_Etiquetas,
    ISNULL(abastece_almacen_id COLLATE DATABASE_DEFAULT, '') AS Abastece_Almacen_Id,
    ISNULL(gpo_plan_ent_ids COLLATE DATABASE_DEFAULT, '') AS Grupo_Plan_Ent_Ids,
    ISNULL(mapear_a COLLATE DATABASE_DEFAULT, '') AS Mapear_A,
    COALESCE(mapear_a, almacen_id, '') COLLATE DATABASE_DEFAULT AS Almacen_Id_Mapeado,
    GETDATE() AS Fecha_Actualizado
FROM {{ var('P_SQLLDSUBS_LS') }}.{{ source('PLANNING_DB', 'Almacenes') }}