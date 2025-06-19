
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{% set unique_key_list = ["Sociedad_Id"] %}
{{ 
    config(
		as_columnstore=False,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado'], included_columns=['Sociedad_Nombre']) }}",
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}

/*
		full_refresh=true,
Prueba de macro, si la macro hace call statement, y tambien esta en post_hook, se ejecutará dos veces ese statement		:
{{ dwh_farinter_create_dummy_data(unique_key=unique_key_list, is_incremental=0, show_info=false)  	}}
*/

SELECT
	ISNULL(CAST([BUKRS] AS VARCHAR(4)),'') COLLATE DATABASE_DEFAULT AS [Sociedad_Id]
	, BUTXT COLLATE DATABASE_DEFAULT AS [Sociedad_Nombre]
	, ORT01 COLLATE DATABASE_DEFAULT AS [Poblacion]
	, LAND1 COLLATE DATABASE_DEFAULT AS [Pais_Id]
	, WAERS COLLATE DATABASE_DEFAULT AS [Moneda_Id]
	, SPRAS COLLATE DATABASE_DEFAULT AS [Idioma_Id]
	, ISNULL(CAST([KTOPL] AS VARCHAR(4)),'X') COLLATE DATABASE_DEFAULT AS [PlanCuentas_Id]
	, PERIV COLLATE DATABASE_DEFAULT AS [VarianteEjercicio_Id]
	, OPVAR COLLATE DATABASE_DEFAULT AS [VariantePeriodoContable_Id]
	, KKBER COLLATE DATABASE_DEFAULT AS [AreaCredito_Id]
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ source('DL_FARINTER', 'DL_SAP_T001')}} S
WHERE OPVAR LIKE 'Z%'
{% if is_incremental() %}
  and S.Fecha_Actualizado >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '19000101')
{% else %}
  and S.Fecha_Actualizado >= '19000101'
{% endif %}

