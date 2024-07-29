
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{% set unique_key_list = ['PlanCuentas_Id','Cuenta_Id'] %}
{{ 
    config(
		as_columnstore=false,
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		pre_hook=[],
		post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['HashStr_PlanCuenta'], create_unique=true) }}",
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}

/*
		full_refresh=true,

*/
with
staging as
(
SELECT --TOP 10 
ISNULL(CAST(A.[KTOPL] AS VARCHAR(4)),'X') COLLATE DATABASE_DEFAULT AS [PlanCuentas_Id] -- X-Plan de cuentas-Check:T004-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[SAKNR] AS VARCHAR(10)),'X') COLLATE DATABASE_DEFAULT AS [Cuenta_Id] -- X-Número de la cuenta de mayor-Check:SKA1-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[TXT20] AS VARCHAR(20)),'') COLLATE DATABASE_DEFAULT AS [Nombre_Corto] --  -Texto breve de las cuentas de mayor-Check: -Datatype:CHAR-Len:(20,0)
    , ISNULL(CAST(A.[TXT50] AS VARCHAR(50)),'') COLLATE DATABASE_DEFAULT AS [Nombre_Largo] --  -Texto explicativo de la cuenta de mayor-Check: -Datatype:CHAR-Len:(50,0)
    , ISNULL(CAST(A.[MCOD1] AS VARCHAR(25)),'') COLLATE DATABASE_DEFAULT AS [Criterio_Busqueda] --  -Criterio de búsqueda para utilizar matchcode-Check: -Datatype:CHAR-Len:(25,0)

	, ISNULL(CAST(B.[XBILK] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Indicador_CuentaBalance] --  -Indicador: ¿Es la cuenta una cuenta de balance?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(B.[SAKAN] AS VARCHAR(10)),'') COLLATE DATABASE_DEFAULT AS [Cuenta_Id_Significativo] --  -Nº de cuenta de mayor en longitud significativa-Check: -Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(B.[BILKT] AS VARCHAR(10)),'') COLLATE DATABASE_DEFAULT AS [Cuenta_Id_Grupo] --  -Número de cuenta de grupo-Check:SKA1-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(B.[ERDAT] AS DATE),'19000101') AS [Fecha_Creacion] --  -Fecha de creación del registro-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(B.[ERNAM] AS VARCHAR(12)),'') COLLATE DATABASE_DEFAULT AS [Responsable_Creacion] --  -Nombre del responsable que ha añadido el objeto-Check: -Datatype:CHAR-Len:(12,0)
	, ISNULL(CAST(B.[GVTYP] AS VARCHAR(2)),'') COLLATE DATABASE_DEFAULT AS [Tipo_Cuenta_Beneficio] --  -Tp.cta.beneficios-Check: -Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(B.[KTOKS] AS VARCHAR(4)),'') COLLATE DATABASE_DEFAULT AS [Grupo_Cuentas_Id] --  -Grupo de cuentas: cuentas de mayor-Check:T077S-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(B.[MUSTR] AS VARCHAR(10)),'') COLLATE DATABASE_DEFAULT AS [Cuenta_Muestra_Id] --  -Número de la cuenta tipo-Check:SKM1-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(B.[VBUND] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Sociedad_Asociada_Id] --  -Número de sociedad GL asociada-Check:T880-Datatype:CHAR-Len:(6,0)
	, ISNULL(CAST(B.[XLOEV] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Indicador_Bloqueado] --  -Indicador: ¿Cuenta marcada para borrado?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(B.[XSPEA] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Indicador_Creacion_Bloqueada] --  -Indicador: ¿cuenta bloqueada para creación?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(B.[XSPEB] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Indicador_Contabilizacion_Bloqueada] --  -Indicador: ¿cuenta bloqueada para contabilizaciones?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(B.[XSPEP] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Indicador_Planificacion_Bloqueada] --  -Indicador: ¿cuenta bloqueada para planificación?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(B.[FUNC_AREA] AS VARCHAR(16)),'') COLLATE DATABASE_DEFAULT AS [CustodioActivo_Id] --  -Custodio del Activo-Check:TFKB-Datatype:CHAR-Len:(16,0)

	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
	
FROM {{ source('DL_FARINTER', 'DL_SAP_SKAT') }} A
INNER JOIN {{ source('DL_FARINTER', 'DL_SAP_SKA1') }} B
	ON A.SAKNR = B.SAKNR
		AND A.KTOPL = B.KTOPL

WHERE EXISTS 
	(SELECT TOP 1 1 FROM {{ ref('BI_SAP_Dim_Sociedad') }}  B 
	WHERE A.KTOPL = B.PlanCuentas_Id)
{% if is_incremental() %}
  --and A.Fecha_Actualizado >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '19000101')
{% endif %}
)
select *
	, ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_PlanCuenta] --IdUnicoPlanCuenta, no cambiar orden
from staging
