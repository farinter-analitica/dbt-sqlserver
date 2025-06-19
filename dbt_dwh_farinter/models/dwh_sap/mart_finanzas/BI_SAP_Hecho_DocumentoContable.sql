
{%- set nombre_esquema_particion = "ps_" + this.identifier + "_fecha" -%}
{%- if is_incremental() -%}
	{% set sql_inicializar_particion= '' %}
{%- else -%}
	{%- set sql_inicializar_particion %}
		EXEC ADM_FARINTER.dbo.pa_inicializar_particiones
			@p_base_datos = '{{this.database}}',
			@p_nombre_esquema_particion = '{{nombre_esquema_particion}}',
			@p_nombre_funcion_particion = '{{"pf_" + this.identifier + "_fecha"}}',
			@p_periodo_tipo = 'Anual', --'Anual' o 'Mensual'
			@p_tipo_datos = 'Fecha', --'Fecha' o 'AnioMes'
			@p_fecha_base = '2018-01-01';
	{% endset -%}
{%- endif -%}
{%- set on_clause = nombre_esquema_particion ~ "([Fecha_Contable])" -%}
{% set unique_key_list = ["Fecha_Contable"
	, "Sociedad_Id"
	, "Ejercicio_Id"
	, "Mes_Contable"
	, "Indicador_Debe_Haber"
	, "Division"
	, "Sociedad_CO"
	, "Centro_Costo_Id"
	, "Centro_Beneficio_Id"
	, "Cuenta_Id"
	, "Cuenta_Principal_Id"    
	, "Indicador_Cuenta_Balance"
	, "Area_Control_Creditos_Id"
	, "Clase_Costo_Id" ] %}
{{ 
    config(
		as_columnstore=false,
    	tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		on_clause_filegroup = on_clause,
		pre_hook=[sql_inicializar_particion],
		post_hook=[
			"{{ dwh_farinter_remove_incremental_temp_table() }}",
      		"{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
			"EXEC ADM_FARINTER.dbo.pa_comprimir_indices_particiones_anteriores 
				@p_base_datos = '{{this.database}}',
				@p_esquema_tabla = '{{this.schema}}', 
				@p_nombre_tabla = '{{this.identifier}}', 
				@p_tipo_datos = 'Fecha';"		
		]
		
) }}

{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '20240101')  from  """ ~ this, relation_not_found_value='20240101') %}
	{% set last_date2 = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Contable)), 112), '20240101')  from  """ ~ this, relation_not_found_value='20240101') %}
    {% set last_date = last_date if last_date < last_date2 else last_date2 %}
{% else %}
    {% set last_date = '20240101' %}
{% endif %}

WITH
{% if is_incremental() %}
incremental as
(
SELECT DISTINCT B.AnioMesCreado_Id
	, B.GJAHR
	, B.BUKRS
	, B.BUDAT
FROM {{ source('DL_FARINTER', 'DL_SAP_BSEG') }} A
INNER JOIN {{ source('DL_FARINTER', 'DL_SAP_BKPF') }} B --Cabecera de documento contable
	ON A.BELNR = B.BELNR
		AND A.BUKRS = B.BUKRS
		AND A.GJAHR = B.GJAHR
		AND A.AnioMes_Id = B.AnioMesCreado_Id
WHERE A.AEDAT >= '{{last_date}}'
AND A.AnioMes_Id >= (YEAR(GETDATE())-2) * 100 + 01
),
{% endif %}
staging as
(
SELECT --TOP 10 
	  ISNULL(CAST(A.[BUKRS] AS VARCHAR(4)) ,'') COLLATE DATABASE_DEFAULT AS [Sociedad_Id] -- X-Sociedad-Check:T001-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(B.[BUDAT] AS DATE),'19000101') AS [Fecha_Contable] --  -Fecha de contabilización en el documento-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(A.[GJAHR] AS INT),0)  AS [Ejercicio_Id] -- X-Ejercicio-Check:-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(B.[MONAT] AS SMALLINT),0)  AS [Mes_Contable] -- X-Ejercicio Mes-Check:-Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[SHKZG] AS VARCHAR(1)) ,'') COLLATE DATABASE_DEFAULT AS [Indicador_Debe_Haber] --  -Indicador debe/haber-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[GSBER] AS VARCHAR(4)) ,'') COLLATE DATABASE_DEFAULT AS [Division] --  -División-Check:TGSB-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[KOKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [Sociedad_CO] --   -Sociedad CO-Check:TKA01-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[KOSTL] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Centro_Costo_Id] --  -Centro de coste-Check:*-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[PRCTR] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Centro_Beneficio_Id] --  -Centro de beneficio-Check:*-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[SAKNR] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Cuenta_Id] --  -Número de la cuenta de mayor-Check:SKB1-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[HKONT] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Cuenta_Principal_Id] --  -Cuenta de mayor de la contabilidad principal-Check:SKB1-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[XBILK] AS VARCHAR(1)) ,'') COLLATE DATABASE_DEFAULT AS [Indicador_Cuenta_Balance] --  -Indicador: ¿Es la cuenta una cuenta de balance?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[KKBER] AS VARCHAR(4)) ,'') COLLATE DATABASE_DEFAULT AS [Area_Control_Creditos_Id] --  -Área de control de créditos-Check:T014-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[KSTAR] AS VARCHAR(10)),'') COLLATE DATABASE_DEFAULT AS [Clase_Costo_Id] --  -Clase de coste-Check:*-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[DMBTR] AS DECIMAL(13,2)),0) AS [Importe_Moneda_Local] --  -Importe en moneda local-Check: -Datatype:CURR-Len:(13,2)
	
FROM {{ source('DL_FARINTER', 'DL_SAP_BSEG') }} A --Documentos contables por posición
INNER JOIN {{ source('DL_FARINTER', 'DL_SAP_BKPF') }} B --Cabecera de documento contable
	ON A.BELNR = B.BELNR
		AND A.BUKRS = B.BUKRS
		AND A.GJAHR = B.GJAHR
		AND A.AnioMes_Id = B.AnioMesCreado_Id
{% if is_incremental() %}
INNER JOIN incremental INC 
	ON B.AnioMesCreado_Id = INC.AnioMesCreado_Id
		AND B.BUKRS = INC.BUKRS
		AND B.GJAHR = INC.GJAHR
		AND B.BUDAT = INC.BUDAT
{% else %}
WHERE A.AnioMes_Id >= (YEAR(GETDATE())-2) * 100 + 01 --AND A.BUKRS IN ('1400','1301','5000')
{% endif %}

)
select A.Fecha_Contable
	, A.Sociedad_Id
	, A.Ejercicio_Id
	, A.Mes_Contable
	, A.Indicador_Debe_Haber
	, A.Division
	, A.Sociedad_CO
	, A.[Centro_Costo_Id]
	, A.[Centro_Beneficio_Id]
	, A.[Cuenta_Id] 
	, A.[Cuenta_Principal_Id] 
	, A.[Indicador_Cuenta_Balance]
	, A.[Area_Control_Creditos_Id] 
	, A.[Clase_Costo_Id] 
	, SUM(A.[Importe_Moneda_Local]) AS [Importe_Moneda_Local]
	, MAX(CP.PlanCuenta_Id) as PlanCuenta_Id_Principal
	, MAX(CSP.SociedadCuenta_Id) as SociedadCuenta_Id_Principal
	, MAX(CC.CentroCosto_SocCo_Id) CentroCosto_SocCo_Id
	, MAX(CB.CentroBeneficio_SocCo_Id) CentroBeneficio_SocCo_Id
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
from staging A
INNER JOIN {{ref('BI_SAP_Dim_Sociedad')}} S --Sociedades
	ON A.Sociedad_Id = S.Sociedad_Id
LEFT JOIN {{ref('BI_SAP_Dim_CuentaContable')}} CP -- Cuentas contables
	ON S.PlanCuentas_Id = CP.PlanCuentas_Id
		AND A.Cuenta_Principal_Id = CP.Cuenta_Id
LEFT JOIN {{ref('BI_SAP_Dim_CuentaContableSociedad')}} CSP --Configuración: Cuentas contables Sociedades
	ON  A.Cuenta_Principal_Id = CSP.Cuenta_Id
		AND A.Sociedad_Id = CSP.Sociedad_Id
LEFT JOIN {{ref('BI_SAP_Dim_CentroCosto')}} CC --
	ON A.Centro_Costo_Id = CC.Centro_Costo_Id
	AND A.Sociedad_CO = CC.Sociedad_CO
LEFT JOIN {{ref('BI_SAP_Dim_CentroBeneficio')}} CB --
	ON A.Centro_Beneficio_Id = CB.Centro_Beneficio_Id
	AND A.Sociedad_CO = CB.Sociedad_CO
GROUP BY A.Fecha_Contable
	, A.Sociedad_Id
	, A.Ejercicio_Id
	, A.Mes_Contable
	, A.Indicador_Debe_Haber
	, A.Division
	, A.Sociedad_CO
	, A.[Centro_Costo_Id]
	, A.[Centro_Beneficio_Id]
	, A.[Cuenta_Id] 
	, A.[Cuenta_Principal_Id] 
	, A.[Indicador_Cuenta_Balance]
	, A.[Area_Control_Creditos_Id] 
	, A.[Clase_Costo_Id] 
