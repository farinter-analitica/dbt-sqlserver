
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{% set unique_key_list = ['Sociedad_Id','Cuenta_Id'] %}
{{ 
    config(
		as_columnstore=false,
    	tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		pre_hook=[],
		post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['HashStr_SociedadCuenta'], create_unique=true) }}",
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
		]
		
) }}
/*
		full_refresh=true,
"{{ dwh_farinter_create_foreign_key(columns=['Sociedad_Id'], referenced_columns=['Sociedad_Id'], referenced_table='BI_SAP_Dim_Sociedad',  is_incremental=is_incremental()) }}"
			"{{ dwh_farinter_create_foreign_key(columns=['HashStr_PlanCuenta'], referenced_columns=['HashStr_PlanCuenta'], referenced_table='BI_SAP_Dim_CuentaContable',  is_incremental=is_incremental()) }}"
			"{{ dwh_farinter_create_foreign_key(columns=['PlanCuentas_Id','Cuenta_Id'], referenced_columns=['PlanCuentas_Id','Cuenta_Id'], referenced_table='BI_SAP_Dim_CuentaContable',  is_incremental=is_incremental()) }}"
*/

WITH
staging as
(
SELECT --TOP 10 
	 ISNULL(CAST(A.[BUKRS] AS VARCHAR(4)),'') COLLATE DATABASE_DEFAULT AS [Sociedad_Id] -- X-Sociedad-Check:T001-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[SAKNR] AS VARCHAR(10)),'') COLLATE DATABASE_DEFAULT AS [Cuenta_Id] -- X-Número de la cuenta de mayor-Check:SKA1-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[BEGRU] AS VARCHAR(8)),'') COLLATE DATABASE_DEFAULT AS [Grupo_Autorizacion] --  -Grupo autorizaciones-Check:*-Datatype:CHAR-Len:(4,0)
	--, ISNULL(TRY_CAST(A.[DATLZ] AS DATE),'19000101')  AS [Fecha_Ultimo_Calculo_Interes] --  -Fecha CPU del último cálculo de intereses-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(A.[ERDAT] AS DATE),'19000101') AS [Fecha_Creacion] --  -Fecha de creación del registro-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(A.[ERNAM] AS VARCHAR(12)),'') COLLATE DATABASE_DEFAULT AS [Responsable_Creacion] --  -Nombre del responsable que haánadido el objeto-Check: -Datatype:CHAR-Len:(12,0)
	, ISNULL(CAST(A.[FDLEV] AS VARCHAR(2)),'') COLLATE DATABASE_DEFAULT AS [Nivel_Tesoreria_Id] --  -Nivel gest.tesorería-Check:T036-Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[FIPLS] AS VARCHAR(3)),'') COLLATE DATABASE_DEFAULT AS [Posicion_PlanTesoreria_Id] --  -Posición del plan de tesorería-Check: -Datatype:NUMC-Len:(3,0)
	, ISNULL(CAST(A.[FSTAG] AS VARCHAR(4)),'') COLLATE DATABASE_DEFAULT AS [Grupo_Status_Campo_Id] --  -Grupo de status de campo-Check:T004F-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[HBKID] AS VARCHAR(5)),'') COLLATE DATABASE_DEFAULT AS [Banco_Propio_Id] --  -Clave breve para banco propio-Check:T012-Datatype:CHAR-Len:(5,0)
	, ISNULL(CAST(A.[HKTID] AS VARCHAR(5)),'') COLLATE DATABASE_DEFAULT AS [Banco_Cuenta_Id] --  -Clave breve para un banco/cuenta-Check:T012K-Datatype:CHAR-Len:(5,0)
	, ISNULL(CAST(A.[KDFSL] AS VARCHAR(4)),'') COLLATE DATABASE_DEFAULT AS [Diferencias_TipoCambio_Id] --  -Clave para diferencias de tipo de cambio en cuentas de ME-Check:T030S-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[MITKZ] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Cuenta_Asociada_Id] --  -La cuenta es cuenta asociada-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[MWSKZ] AS VARCHAR(2)),'') COLLATE DATABASE_DEFAULT AS [Categoria_Fiscal_Id] --  -Categoría fiscal en el maestro de cuentas-Check: -Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[VZSKZ] AS VARCHAR(2)),'') COLLATE DATABASE_DEFAULT AS [Calculo_Interes_Id] --  -Indicador de cálculo de intereses-Check:T056-Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[WAERS] AS VARCHAR(5)),'') COLLATE DATABASE_DEFAULT AS [Moneda_Id] --  -Moneda de la cuenta-Check:TCURC-Datatype:CUKY-Len:(5,0)
	, ISNULL(CAST(A.[WMETH] AS VARCHAR(2)),'') COLLATE DATABASE_DEFAULT AS [Indicador_Gestion_Sistema_Externo] --  -Indicador: Gestión de ctas. en sistema externo-Check: -Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[XGKON] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Indicador_Movimiento_Caja] --  -Cuenta de movimientos de caja-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[XLOEB] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Indicador_Borrado] --  -Indicador: ¿Cuenta marcada para borrado?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[XOPVW] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Indicador_Gestion_Partidas_Abiertas] --  -Indicador: ¿Gestión de partidas abiertas?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[XSPEB] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Indicador_ContabilizacionBloqueada] --  -Indicador: ¿cuenta bloqueada para contabilizaciones?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[ZINRT] AS SMALLINT),0)  AS [Meses_RitmoCalculoIntereses] --  -Ritmo del cálculo de intereses en meses-Check: -Datatype:NUMC-Len:(2,0)
	, ISNULL(CAST(A.[ZUAWA] AS VARCHAR(3)),'') COLLATE DATABASE_DEFAULT AS [Clave_Asignacion_Id] --  -Clave para clasificar por números de asignación-Check:TZUN-Datatype:CHAR-Len:(3,0)
	, ISNULL(CAST(A.[ALTKT] AS VARCHAR(10)),'') COLLATE DATABASE_DEFAULT AS [Cuenta_Alternativa_Id] --  -Nº de cuenta alternativo en la sociedad-Check:*-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[INFKY] AS VARCHAR(8)),'') COLLATE DATABASE_DEFAULT AS [Clave_Inflacion_Id] --  -Clave de inflación-Check:J_1AINFSKS-Datatype:CHAR-Len:(8,0)

	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
	
FROM {{ source('DL_FARINTER', 'DL_SAP_SKB1') }} A --Configuración: Cuentas contables por Sociedad

)
select A.*
	, ISNULL({{dwh_farinter_concat_key_columns(columns=unique_key_list, input_length=14,table_alias='A' )}}, '') AS [SociedadCuenta_Id]
	, ISNULL({{ dwh_farinter_hash_column(columns=unique_key_list,table_alias='A' ) }}, '') AS [HashStr_SociedadCuenta] --IdUnico, no cambiar orden
	, C.HashStr_PlanCuenta
	, S.PlanCuentas_Id
from staging A
INNER JOIN {{ref('BI_SAP_Dim_Sociedad')}} S --Sociedades
	ON A.Sociedad_Id = S.Sociedad_Id
INNER JOIN {{ref('BI_SAP_Dim_CuentaContable')}} C --Configuración: Cuentas contables
	ON S.PlanCuentas_Id = C.PlanCuentas_Id
		AND A.Cuenta_Id = C.Cuenta_Id