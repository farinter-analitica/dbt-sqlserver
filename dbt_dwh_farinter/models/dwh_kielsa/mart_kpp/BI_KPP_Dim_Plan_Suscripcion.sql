{% set unique_key_list = ["Plan_Id"] %}
{{ 
    config(
		as_columnstore=False,
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
    pre_hook=[
      "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            ],
		post_hook=[
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}
SELECT ISNULL(CAST([Plan_Id] as INT), 0) as [Plan_Id]
      ,ISNULL(CAST([Nombre] as VARCHAR(50)), '') as [Nombre]
      ,ISNULL(CAST([Costo] as DECIMAL(16,4)), 0) as [Costo]
      ,ISNULL(CAST([Contrato] as VARCHAR(MAX)), 0) as [Contrato]
      ,ISNULL(CAST([CodPlanKielsaClinica] as VARCHAR(50)), '')  as [PlanKielsaClinica_Id]
      ,[PlanType] as [Descripcion]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
  --FROM [REPLICASLD].[KPP_DB].[dbo].[Plan_Suscripcion]
FROM {{ var('P_SQLLDSUBS_LS') }}.{{ source('Kielsa_KPP', 'Plan_Suscripcion') }}