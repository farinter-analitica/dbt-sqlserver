
{{ 
    config(
		as_columnstore=false,
		materialized="table",
		unique_key=['PlanCuentas_Id','Cuenta_Id'],
		on_schema_change="sync_all_columns",
		post_hook=[
			"{{dwh_farinter_remove_incremental_temp_table(this)}}"
			,"{{dwh_farinter_create_primary_key(this,columns=config.get('unique_key'), create_clustered=True, is_incremental=0,if_another_exists_delete=True, show_info=False)}}"
		]
		
) }}

/*

{{dwh_farinter_create_primary_key(this,columns=['PlanCuentas_Id','Cuenta_Id'], create_clustered=True, is_incremental=0,if_another_exists_delete=True, show_info=True)}}
*/
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
	, {{ dwh_farinter_hash_column(config.get('unique_key')) }} AS [Hash_CuentaContable] --IdUnicoPlanCuenta, no cambiar orden
	, [Fecha_Carga]
	, [Fecha_Actualizado] 
from staging