{% set unique_key_list = ["GrupoClientes_Id"] %}
{{ 
    config(
    tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}
/*
    full_refresh= true,
*/

SELECT distinct
    GrupoClientes_Id,
    GrupoClientes_Nombre
FROM {{ ref("DL_Edit_GrupoClientes_SAP") }}
