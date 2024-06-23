
--dwh_farinter_create_primary_key(relation, columns=None, create_clustered=False, is_incremental=False, if_another_exists_delete=False) 
--add dwh_farinter_remove_incremental_temp_table to all incremental models
{{ 
    config(
		as_columnstore=False,
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=["Sociedad_Id"],
		on_schema_change="sync_all_columns",
		post_hook=[
			"{{dwh_farinter_remove_incremental_temp_table(this)}}"
			,"{{dwh_farinter_create_primary_key(this,columns=config.get('unique_key'), create_clustered=True, is_incremental=0,if_another_exists_delete=True, show_info=True)}}"
		]
		
) }}

/*
Prueba de macro:
{{dwh_farinter_remove_incremental_temp_table(this)}}
{{dwh_farinter_create_primary_key(this,columns=config.get('unique_key'), create_clustered=True, is_incremental=0,if_another_exists_delete=True)}}
{{is_incremental()}}
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
FROM	DL_FARINTER.dbo.DL_SAP_T001 T
WHERE OPVAR LIKE 'Z%'
{% if is_incremental() %}
  and T.Fecha_Actualizado >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '1900-01-01')
{% else %}
  and T.Fecha_Actualizado >= '1900-01-01'
{% endif %}
UNION ALL
SELECT 'X', 'N/D', '', '', '', '', '', '', '', '', GETDATE() as Fecha_Carga, GETDATE() as Fecha_Actualizado


