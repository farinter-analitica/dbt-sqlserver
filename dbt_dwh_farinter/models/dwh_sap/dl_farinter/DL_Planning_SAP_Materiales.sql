{%- set unique_key_list = ["Material_Id"] -%}

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

-- Modelo incremental para la tabla Materiales de PLANNING_DB.
-- Incluye todos los campos y respeta los defaults y claves del modelo fuente.

SELECT
    ISNULL(material_id COLLATE DATABASE_DEFAULT, '') AS Material_Id,
    material_nombre_ref COLLATE DATABASE_DEFAULT AS Material_Nombre_Ref,
    u_x_c AS U_X_C,
    u_x_t AS U_X_T,
    gpo_art_id_ref COLLATE DATABASE_DEFAULT AS Gpo_Art_Id_Ref,
    gpo_art_nombre_ref COLLATE DATABASE_DEFAULT AS Gpo_Art_Nombre_Ref,
    compra_solo_uxc COLLATE DATABASE_DEFAULT AS Compra_Solo_UXC,
    compra_fact_en_cajas COLLATE DATABASE_DEFAULT AS Compra_Fact_En_Cajas,
    etiquetas COLLATE DATABASE_DEFAULT AS Etiquetas,
    notas COLLATE DATABASE_DEFAULT AS Notas,
    ISNULL(sub_gpo_id COLLATE DATABASE_DEFAULT, 'ST') AS Sub_Gpo_Id,
    GETDATE() AS Fecha_Actualizado
FROM {{ var('P_SQLLDSUBS_LS') }}.{{ source('PLANNING_DB', 'Materiales') }}