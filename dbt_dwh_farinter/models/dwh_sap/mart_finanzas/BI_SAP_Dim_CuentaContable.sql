
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{% set unique_key_list = ['PlanCuentas_Id','Cuenta_Id'] %}
{{ 
    config(
		as_columnstore=false,
		materialized="table",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		pre_hook=[],
		post_hook=[
			dwh_farinter_remove_incremental_temp_table()
			,dwh_farinter_create_primary_key(this,columns=unique_key_list, create_clustered=True, is_incremental=0,if_another_exists_drop_it=True, show_info=True)
		]
		
) }}

/*{{dwh_farinter_create_primary_key(this,columns=unique_key_list, create_clustered=True, is_incremental=0,if_another_exists_drop_it=True, show_info=False)}}*/
with
staging as
(
SELECT --TOP 10 
ISNULL(CAST(A.[KTOPL] AS VARCHAR(4)),'') COLLATE DATABASE_DEFAULT AS [PlanCuentas_Id] -- X-Plan de cuentas-Check:T004-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[SAKNR] AS VARCHAR(10)),'') COLLATE DATABASE_DEFAULT AS [Cuenta_Id] -- X-Número de la cuenta de mayor-Check:SKA1-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[TXT20] AS VARCHAR(20)),'') COLLATE DATABASE_DEFAULT AS [Nombre_Corto] --  -Texto breve de las cuentas de mayor-Check: -Datatype:CHAR-Len:(20,0)
    , ISNULL(CAST(A.[TXT50] AS VARCHAR(50)),'') COLLATE DATABASE_DEFAULT AS [Nombre_Largo] --  -Texto explicativo de la cuenta de mayor-Check: -Datatype:CHAR-Len:(50,0)
    , ISNULL(CAST(A.[MCOD1] AS VARCHAR(25)),'') COLLATE DATABASE_DEFAULT AS [Busqueda] --  -Criterio de búsqueda para utilizar matchcode-Check: -Datatype:CHAR-Len:(25,0)
	, ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
	, ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM DL_FARINTER.dbo.DL_SAP_SKAT A
WHERE EXISTS 
(SELECT TOP 1 1 FROM {{ ref('BI_SAP_Dim_Sociedad') }}  B 
WHERE A.KTOPL = B.PlanCuentas_Id)
{% if is_incremental() %}
  and A.Fecha_Actualizado >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '1900-01-01')
{% endif %}
)
select [PlanCuentas_Id]
	, [Cuenta_Id]
	, [Nombre_Corto]
	, [Nombre_Largo]
	, [Busqueda]
	, ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_PlanCuenta] --IdUnicoPlanCuenta, no cambiar orden
	, [Fecha_Carga]
	, [Fecha_Actualizado] 
from staging
{{dwh_farinter_union_all_dummy_data(unique_key=unique_key_list, is_incremental=0) }}
