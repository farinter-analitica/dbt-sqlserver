
{% set unique_key_list = ["TipoBodega_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "automation/periodo_por_hora"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}
		SELECT DISTINCT
			Emp_Id,
			[TipoBodega_Id],
			Bodega_Tipo as TipoBodega_Nombre
		FROM {{ref ('BI_Kielsa_Dim_Bodega')}} B