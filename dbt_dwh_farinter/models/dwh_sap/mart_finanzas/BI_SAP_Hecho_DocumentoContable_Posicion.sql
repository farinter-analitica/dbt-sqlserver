
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
{% set unique_key_list = ['Sociedad_Id','Documento_Id', 'Ejercicio_Id', 'Posicion_Id', 'Fecha_Contable'] %}
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
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -3, max(Fecha_Actualizado)), 112), '20240101')  from  """ ~ this, relation_not_found_value='20240101') %}
	{% set last_date2 = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Contable)), 112), '20240101')  from  """ ~ this, relation_not_found_value='20240101') %}
    {% set last_date = last_date if last_date < last_date2 else last_date2 %}
{% else %}
    {% set last_date = '20240101' %}
{% endif %}


WITH
staging as
(
SELECT --TOP 10 
	  ISNULL(CAST(A.[BUKRS] AS VARCHAR(4)) ,'') COLLATE DATABASE_DEFAULT AS [Sociedad_Id] -- X-Sociedad-Check:T001-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[BELNR] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Documento_Id] -- X-Número de un documento contable-Check: -Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[GJAHR] AS INT),0)  AS [Ejercicio_Id] -- X-Ejercicio-Check:-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(B.[MONAT] AS SMALLINT),0)  AS [Mes_Contable] -- X-Ejercicio Mes-Check:-Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[BUZEI] AS VARCHAR(3)),'') COLLATE DATABASE_DEFAULT AS [Posicion_Id] -- X-Posición-Check:T004-Datatype:CHAR-Len:(3,0)
	--, ISNULL(CAST(A.[AnioMes_Id] as INT ),0)  AS [AnioMes_Id] 
	, ISNULL(CAST(B.[BUDAT] AS DATE),'19000101') AS [Fecha_Contable] --  -Fecha de contabilización en el documento-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(A.[CPUDT] AS DATE),'19000101') AS [Fecha_Creado] --  -Fecha de la Creado-Check: -Datatype:DATS-Len:(8,0)
	--, ISNULL(TRY_CAST(A.[AEDAT] AS DATE),'19000101') AS [Fecha_Modificado] --  -Fecha de la Modificado-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(TRY_CAST(A.[AUGDT] AS DATE),'19000101') AS [Fecha_Compensado] --  -Fecha de la compensación-Check: -Datatype:DATS-Len:(8,0)
	--, ISNULL(TRY_CAST(A.[AUGCP] AS DATE),'19000101') AS [FechaRegistro_Compensado]  --  -Día de registro de la compensación-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(A.[AUGBL] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Documento_Compensacion_Id] --  -Número del documento de compensación-Check: -Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[BSCHL] AS VARCHAR(2)) ,'') COLLATE DATABASE_DEFAULT AS [Clave_Contabilizacion] --  -Clave de contabilización-Check:TBSL-Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[KOART] AS VARCHAR(1)) ,'') COLLATE DATABASE_DEFAULT AS [Clase_Cuenta] --  -Clase de cuenta-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[SHKZG] AS VARCHAR(1)) ,'') COLLATE DATABASE_DEFAULT AS [Indicador_Debe_Haber] --  -Indicador debe/haber-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[GSBER] AS VARCHAR(4)) ,'') COLLATE DATABASE_DEFAULT AS [Division] --  -División-Check:TGSB-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[MWSKZ] AS VARCHAR(2)) ,'') COLLATE DATABASE_DEFAULT AS [Indicador_IVA] --  -Indicador IVA-Check:T007A-Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[QSSKZ] AS VARCHAR(2)) ,'') COLLATE DATABASE_DEFAULT AS [Indicador_Retencion] --  -Indicador de retención-Check:T059Q-Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[DMBTR] AS DECIMAL(13,2)),0) AS [Importe_Moneda_Local] --  -Importe en moneda local-Check: -Datatype:CURR-Len:(13,2)
	, ISNULL(CAST(A.[KZBTR] AS DECIMAL(13,2)),0) AS [Importe_Reduccion_Original_Moneda_Local] --  -Importe de reducción original en moneda local-Check: -Datatype:CURR-Len:(13,2)
	, ISNULL(CAST(A.[PSWBT] AS DECIMAL(13,2)),0) AS [Importe_Actualizacion_Libro_Mayor] --  -Importe de actualización en el libro mayor-Check: -Datatype:CURR-Len:(13,2)
	, ISNULL(CAST(A.[PSWSL] AS  VARCHAR(5)),'') COLLATE DATABASE_DEFAULT AS [Moneda_Actualizacion_Libro_Mayor] --  -Moneda actualización para cifras movimientos libro mayor-Check:TCURC-Datatype:CUKY-Len:(5,0)
	, ISNULL(CAST(A.[TXBHW] AS DECIMAL(13,2)),0) AS [Base_Imponible_Original_Moneda_Local] --  -Base imponible original en moneda local-Check: -Datatype:CURR-Len:(13,2)
	, ISNULL(CAST(A.[MWSTS] AS DECIMAL(13,2)),0) AS [Importe_Impuesto_Moneda_Local] --  -Importe del impuesto en moneda local-Check: -Datatype:CURR-Len:(13,2)
	, ISNULL(CAST(A.[HWBAS] AS DECIMAL(13,2)),0) AS [Importe_Base_Impuesto_Moneda_Local] --  -Importe base del impuesto en moneda local-Check: -Datatype:CURR-Len:(13,2)
	, ISNULL(CAST(A.[HWZUZ] AS DECIMAL(13,2)),0) AS [Importe_Provision_Moneda_Local] --  -Importe de provisión en moneda local-Check: -Datatype:CURR-Len:(13,2)
	--, ISNULL(CAST(A.[SHZUZ] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Dato_Adicional_Debe_Haber_Descuento] --  -Dato adicional Debe/Haber para descuento-Check: -Datatype:CHAR-Len:(1,0)
	--, ISNULL(CAST(A.[MWART] AS VARCHAR(1)),'') COLLATE DATABASE_DEFAULT AS [Clase_Impuesto] --  -Clase de impuesto-Check: -Datatype:CHAR-Len:(1,0)
	--, ISNULL(CAST(A.[TXGRP] AS VARCHAR(3)),'') COLLATE DATABASE_DEFAULT AS [Indicador_Grupo_Documentos_Impuesto] --  -Indicador de grupo para documentos de impuesto-Check: -Datatype:NUMC-Len:(3,0)
	, ISNULL(CAST(A.[KTOSL] AS VARCHAR(3)),'') COLLATE DATABASE_DEFAULT AS [Clave_Operacion] --  -Clave de operación-Check: -Datatype:CHAR-Len:(3,0)
	, ISNULL(CAST(A.[QSSHB] AS DECIMAL(13,2)),0) AS [Importe_Base_Retencion] --  -Importe base para retención-Check: -Datatype:CURR-Len:(13,2)
	--, ISNULL(CAST(A.[ZUONR] AS VARCHAR(18)),'')  COLLATE DATABASE_DEFAULT AS [Numero_Asignacion] --  -Número de asignación-Check: -Datatype:CHAR-Len:(18,0)
	--, ISNULL(CAST(A.[SGTXT] AS VARCHAR(50)),'')  COLLATE DATABASE_DEFAULT AS [Texto_Posicion] --  -Texto posición-Check: -Datatype:CHAR-Len:(50,0)
	, ISNULL(CAST(A.[BEWAR] AS VARCHAR(3)) ,'') COLLATE DATABASE_DEFAULT AS [Clase_Movimiento] --  -Cl.movimiento-Check:T856-Datatype:CHAR-Len:(3,0)
	--, ISNULL(CAST(A.[VORGN] AS VARCHAR(4)) ,'') COLLATE DATABASE_DEFAULT AS [Clase_Operacion_GL] --  -Clase de operación para General Ledger-Check: -Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[KOKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [Sociedad_CO] --   -Sociedad CO-Check:TKA01-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[FDLEV] AS VARCHAR(2)) ,'') COLLATE DATABASE_DEFAULT AS [Nivel_Gestion_Tesoreria] --  -Nivel gest.tesorería-Check:T036-Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[FDGRP] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Grupo_Tesoreria_Id] --  -Grupo de tesorería-Check:T035-Datatype:CHAR-Len:(10,0)
	, ISNULL(TRY_CAST(A.[FDTAG] AS DATE),'19000101') AS [Fecha_Tesoreria] --  -Fecha de tesorería-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(A.[KOSTL] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Centro_Costo_Id] --  -Centro de coste-Check:*-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[AUFNR] AS VARCHAR(12)),'')  COLLATE DATABASE_DEFAULT AS [Numero_Orden_Id] --  -Número de orden-Check:AUFK-Datatype:CHAR-Len:(12,0)
	, ISNULL(CAST(A.[VBELN] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Factura_Id] --  -Factura-Check:VBUK-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[VBEL2] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Documento_Ventas_Id] --  -Documento de ventas-Check:VBUK-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[POSN2] AS VARCHAR(6)) ,'') COLLATE DATABASE_DEFAULT AS [Posicion_Documento_Ventas] --  -Posición documento ventas-Check:VBUP-Datatype:NUMC-Len:(6,0)
	, ISNULL(CAST(A.[ANLN1] AS VARCHAR(12)),'')  COLLATE DATABASE_DEFAULT AS [Activo_Fijo_Principal_Id] --  -Número principal de activo fijo-Check:ANLH-Datatype:CHAR-Len:(12,0)
	, ISNULL(CAST(A.[ANLN2] AS VARCHAR(4)) ,'') COLLATE DATABASE_DEFAULT AS [Subnumero_Activo_Fijo] --  -Subnúmero de activo fijo-Check:ANLA-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[ANBWA] AS VARCHAR(3)) ,'') COLLATE DATABASE_DEFAULT AS [Clase_Movimiento_Activos_Fijo_Id] --  -Clase de movimiento de activos fijos-Check:TABW-Datatype:CHAR-Len:(3,0)
	, ISNULL(CAST(A.[XUMSW] AS VARCHAR(1)) ,'') COLLATE DATABASE_DEFAULT AS [Indicador_Posicion_Efecto_Ventas] --  -Indicador: ¿Posición con efecto sobre las ventas?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[SAKNR] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Cuenta_Id] --  -Número de la cuenta de mayor-Check:SKB1-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[HKONT] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Cuenta_Principal_Id] --  -Cuenta de mayor de la contabilidad principal-Check:SKB1-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[KUNNR] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Deudor_Id] --  -Número de deudor-Check:KNA1-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[LIFNR] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Acreedor_Proveedor_Id] --  -Número de cuenta del proveedor o acreedor-Check:LFA1-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[XBILK] AS VARCHAR(1)) ,'') COLLATE DATABASE_DEFAULT AS [Indicador_Cuenta_Balance] --  -Indicador: ¿Es la cuenta una cuenta de balance?-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[GVTYP] AS VARCHAR(2)) ,'') COLLATE DATABASE_DEFAULT AS [Tipo_Cuenta_Beneficios] --  -Tp.cta.beneficios-Check: -Datatype:CHAR-Len:(2,0)
	, ISNULL(CAST(A.[HZUON] AS VARCHAR(18)),'')  COLLATE DATABASE_DEFAULT AS [Numero_Asignacion_Cuentas_Especiales] --  -Número de asignación para cuentas de mayor especiales-Check: -Datatype:CHAR-Len:(18,0)
	, ISNULL(TRY_CAST(A.[ZFBDT] AS DATE),'19000101') AS [Fecha_Base_Calculo_Vencimiento] --  -Fecha base para cálculo del vencimiento-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(A.[ZTERM] AS VARCHAR(4)),'')  COLLATE DATABASE_DEFAULT AS [Condiciones_Pago_Id] --  -Clave de condiciones de pago-Check: -Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[SKNTO] AS DECIMAL(13,2)),0) AS [Importe_Descuento_Moneda_Local] --  -Importe del descuento en moneda local-Check: -Datatype:CURR-Len:(13,2)
	, ISNULL(CAST(A.[NEBTR] AS DECIMAL(13,2)),0) AS [Importe_Neto_Pago] --  -Importe neto del pago-Check: -Datatype:CURR-Len:(13,2)
	, ISNULL(CAST(A.[ABPER] AS INT),0) AS [Periodo_Liquidacion] --  -Período de liquidación-Check: -Datatype:ACCP-Len:(6,0)
	, ISNULL(CAST(A.[EBELN] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Documento_Compras_Id] --  -Número del documento de compras-Check:EKKO-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[EBELP] AS VARCHAR(5)) ,'') COLLATE DATABASE_DEFAULT AS [Posicion_Documento_Compras_Id] --  -Número de posición del documento de compras-Check:EKPO-Datatype:NUMC-Len:(5,0)
	, ISNULL(CAST(A.[PRCTR] AS VARCHAR(10)),'')  COLLATE DATABASE_DEFAULT AS [Centro_Beneficio_Id] --  -Centro de beneficio-Check:*-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[XRAGL] AS VARCHAR(1)) ,'') COLLATE DATABASE_DEFAULT AS [Indicador_Compensacion_Anulada] --  -Indicador: La compensación ha sido anulada-Check: -Datatype:CHAR-Len:(1,0)
	, ISNULL(CAST(A.[KKBER] AS VARCHAR(4)) ,'') COLLATE DATABASE_DEFAULT AS [Area_Control_Creditos_Id] --  -Área de control de créditos-Check:T014-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(A.[KIDNO] AS VARCHAR(30)),'')  COLLATE DATABASE_DEFAULT AS [Referencia_Pago] --  -Referencia de pago-Check: -Datatype:CHAR-Len:(30,0)
	, ISNULL(CAST(A.[AGZEI] AS VARCHAR(5)),'') AS [Posicion_Compensacion] --  -Posición de compensación-Check: -Datatype:DEC-Len:(5,0)
	, ISNULL(CAST(A.[AUGGJ] AS INT),0) AS [Ejercicio_Doc_Compensacion] --  -Ejercicio de doc.compensación-Check: -Datatype:NUMC-Len:(4,0)
	, ISNULL(CAST(A.[KSTAR] AS VARCHAR(10)),'') COLLATE DATABASE_DEFAULT AS [Clase_Costo_Id] --  -Clase de coste-Check:*-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
	
FROM {{ source('DL_FARINTER', 'DL_SAP_BSEG') }} A --Documentos contables por posición
INNER JOIN {{ source('DL_FARINTER', 'DL_SAP_BKPF') }} B --Cabecera de documento contable
	ON A.BELNR = B.BELNR
		AND A.BUKRS = B.BUKRS
		AND A.GJAHR = B.GJAHR
		AND A.AnioMes_Id = B.AnioMesCreado_Id
WHERE
{% if is_incremental() %}
   A.AEDAT >= '{{last_date}}'
{% else %}
   A.AnioMes_Id >= 202301 AND A.BUKRS IN ('1400','1301','5000')
{% endif %}

)
select A.*
	, C.PlanCuenta_Id
	, CS.SociedadCuenta_Id
	, CP.PlanCuenta_Id as PlanCuenta_Id_Principal
	, CSP.SociedadCuenta_Id as SociedadCuenta_Id_Principal
	, CC.CentroCosto_SocCo_Id
	, CB.CentroBeneficio_SocCo_Id
from staging A
INNER JOIN {{ref('BI_SAP_Dim_Sociedad')}} S --Sociedades
	ON A.Sociedad_Id = S.Sociedad_Id
LEFT JOIN {{ref('BI_SAP_Dim_CuentaContable')}} C -- Cuentas contables
	ON S.PlanCuentas_Id = C.PlanCuentas_Id
		AND A.Cuenta_Id = C.Cuenta_Id
LEFT JOIN {{ref('BI_SAP_Dim_CuentaContableSociedad')}} CS --Configuración: Cuentas contables Sociedades
	ON  A.Cuenta_Id = CS.Cuenta_Id
		AND A.Sociedad_Id = CS.Sociedad_Id
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


