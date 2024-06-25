
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{% set unique_key_list = ["Sociedad_Id"] %}
{{ 
    config(
		as_columnstore=False,
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		post_hook=[
			"{{ dwh_farinter_remove_incremental_temp_table(this) }}"
			,"{{ dwh_farinter_create_primary_key(this,columns=unique_key_list, create_clustered=True, is_incremental=is_incremental(),if_another_exists_drop_it=True) }}"
			,"{{ dwh_farinter_create_dummy_data(unique_key=unique_key_list, is_incremental=0, show_info=false) }}"
		]
	) 
}}

/*
Prueba de macro, si la macro hace call statement, y tambien esta en post_hook, se ejecutará dos veces ese statement		:
{{ dwh_farinter_create_dummy_data(unique_key=unique_key_list, is_incremental=0, show_info=false)  	}}
*/

SELECT
	ISNULL(BUKRS,'') COLLATE DATABASE_DEFAULT AS [Sociedad_Id]
	, BUTXT COLLATE DATABASE_DEFAULT AS [Sociedad_Nombre]
	, ORT01 COLLATE DATABASE_DEFAULT AS [Poblacion]
	, LAND1 COLLATE DATABASE_DEFAULT AS [Pais_Id]
	, WAERS COLLATE DATABASE_DEFAULT AS [Moneda_Id]
	, SPRAS COLLATE DATABASE_DEFAULT AS [Idioma_Id]
	, KTOPL COLLATE DATABASE_DEFAULT AS [PlanCuentas_Id]
	, PERIV COLLATE DATABASE_DEFAULT AS [VarianteEjercicio_Id]
	, OPVAR COLLATE DATABASE_DEFAULT AS [VariantePeriodoContable_Id]
	, KKBER COLLATE DATABASE_DEFAULT AS [AreaCredito_Id]
	, ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
	, ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
	, '' bORRAR
FROM {{ source('DL_FARINTER', 'DL_SAP_T001')}} T
WHERE OPVAR LIKE 'Z%'
{% if is_incremental() %}
  and T.Fecha_Actualizado >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '1900-01-01')
{% else %}
  and T.Fecha_Actualizado >= '1900-01-01'
{% endif %}

