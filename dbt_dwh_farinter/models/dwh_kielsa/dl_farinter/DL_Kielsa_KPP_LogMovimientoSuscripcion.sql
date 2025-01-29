
{%- set unique_key_list = ["id"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
      "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['TarjetaKC_Id']) }}",
      "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Sucursal_Registro']) }}",
		]
		
) }}

{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}



SELECT ISNULL(CAST([id] AS INT),0) AS Id
      ,ISNULL(CAST([TarjetaKC_Id] AS VARCHAR(50)) COLLATE DATABASE_DEFAULT ,'') AS TarjetaKC_Id
      ,ISNULL([Fecha],'19000101') AS [Fecha]
      ,[CodPlanKielsaClinica] COLLATE DATABASE_DEFAULT AS CodPlanKielsaClinica
      ,[Tipo_Ingreso] COLLATE DATABASE_DEFAULT AS Tipo_Ingreso
      ,[Origen] 
      ,[Tipo_Documento] COLLATE DATABASE_DEFAULT AS Tipo_Documento
      ,[Sucursal_Registro]
      ,[Usuario_Registro] COLLATE DATABASE_DEFAULT AS Usuario_Registro
      ,[TipoPlan]
      ,[Tipo_Registro] COLLATE DATABASE_DEFAULT  AS Tipo_Registro
      , GETDATE() AS Fecha_Actualizado
  FROM  {{ var('P_SQLLDSUBS_LS') }}.{{ source('KPP_DB', 'LogMovimientoSuscripcion') }} --[KPP_DB].[dbo].[LogMovimientoSuscripcion]
  {% if is_incremental() %}
  WHERE Fecha > '{{ last_date }}'
  {% else %}
  UNION ALL
  SELECT ISNULL(CAST([id] AS INT),0) AS Id
      ,ISNULL(CAST([TarjetaKC_Id] AS VARCHAR(50)),'') AS TarjetaKC_Id
      ,ISNULL([Fecha],'19000101') AS [Fecha]
      ,[CodPlanKielsaClinica]
      ,[Tipo_Ingreso]
      ,[Origen]
      ,[Tipo_Documento]
      ,[Sucursal_Registro]
      ,[Usuario_Registro]
      ,[TipoPlan]
      ,[Tipo_Registro]
      , GETDATE() AS Fecha_Actualizado
    FROM [DL_FARINTER].[dbo].[DL_Kielsa_KPP_LogMovimientoSuscripcion_Hist] -- {{ ref ('DL_Kielsa_KPP_LogMovimientoSuscripcion_Hist') }}
  {% endif %}


