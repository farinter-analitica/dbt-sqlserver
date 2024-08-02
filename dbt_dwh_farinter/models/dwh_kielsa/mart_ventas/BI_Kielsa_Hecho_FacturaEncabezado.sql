

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
			@p_fecha_base = '2018-01-01'
	{% endset -%}
{%- endif -%}
{%- set on_clause = nombre_esquema_particion ~ "([Factura_Fecha])" -%}
{%- set unique_key_list = ["Factura_Id","Suc_Id","Emp_Id","TipoDoc_Id","Caja_Id","Factura_Fecha"] -%}

{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		on_clause_filegroup = on_clause,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		pre_hook=[sql_inicializar_particion],
		post_hook=[
		"{{ dwh_farinter_remove_incremental_temp_table() }}",
		after_commit("{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(),
			if_another_exists_drop_it=true) }}"),
		after_commit("{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", 
			create_clustered=false, 
			is_incremental=is_incremental(), 
			if_another_exists_drop_it=true) }}"),
        after_commit("{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado'], included_columns=['Factura_Fecha']) }}"),
        after_commit("{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Factura_Fecha']) }}"),
		after_commit("EXEC ADM_FARINTER.dbo.pa_comprimir_indices_particiones_anteriores 
			@p_base_datos = '{{this.database}}',
		 	@p_esquema_tabla = '{{this.schema}}', 
			@p_nombre_tabla = '{{this.identifier}}', 
			@p_tipo_datos = 'Fecha';")		
			]
		
) }}

{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -0, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{% else %}
	{% set last_date = '19000101' %}
{% endif %}

WITH Facturas AS
(
	SELECT --TOP 1000
		ISNULL(CAST(FE.[Factura_Fecha] AS DATE), '19000101') Factura_Fecha
		, ISNULL(FE.[Emp_Id], 0) [Emp_Id]
		, ISNULL(FE.[Suc_Id], 0) [Suc_Id] --, FE.[Suc_Id]
		, ISNULL(FE.[Bodega_Id], 0) [Bodega_Id] --, FE.[Bodega_Id]
		, ISNULL(FE.[Caja_Id], 0) [Caja_Id] --, FE.[Caja_Id]
		, ISNULL(FE.[TipoDoc_Id], 0) [TipoDoc_Id] --, FE.[TipoDoc_Id]
		, ISNULL(FE.[Factura_Id], 0) [Factura_Id] --, FE.[Factura_Id]
		, ISNULL(FE.[SubDoc_Id], 0) [SubDoc_Id] --, FE.[SubDoc_Id]
		, FE.[Consecutivo] Consecutivo_Factura
		, CAST(FE.[Factura_Fecha] AS TIME(0)) Factura_FechaHora
		, FE.[MonederoTarj_Id]
		, FE.[MonederoTarj_Id_Limpio] as [Monedero_Id]
		, FE.[Cliente_Id]
		, FE.[Vendedor_Id]
		, FE.[Preventa_Id]
		, FE.[PreFactura_id]
		, FE.Factura_Numero_CAI AS Factura_Clave_Tributaria
		, FE.[Factura_Estado]
		, FE.[Factura_Origen]
		, FE.[AnioMes_Id]
		, ISNULL(FE.Factura_Costo, 0) Valor_Costo
		, ISNULL(FE.Factura_Costo_Bonificacion, 0) Valor_Costo_Bonificacion
		, ISNULL(FE.Factura_Subtotal, 0) Valor_Subtotal
		, ISNULL(FE.Factura_Descuento, 0) Valor_Descuento
		, ISNULL(FE.Factura_Impuesto, 0) Valor_Impuesto
		, ISNULL(FE.Factura_Total, 0) Valor_Total
		, SUC.HashStr_SucEmp
		, DOC.HashStr_SubDDocEmp
		, CLI.HashStr_CliEmp
		, MON.HashStr_MonEmp
		, ISNULL(SAM.Tipo_Id, 0) AS Same_Id
	FROM  {{source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado')}} FE
	INNER JOIN {{source('BI_FARINTER', 'BI_Dim_Calendario')}} CAL 
		ON CAL.Fecha_Id = CAST(FE.Factura_Fecha AS DATE) AND CAL.AnioMes_Id = FE.AnioMes_Id
	LEFT JOIN {{ref ('BI_Kielsa_Dim_Monedero')}} MON
		ON MON.Monedero_Id = FE.MonederoTarj_Id_Limpio
		AND MON.Emp_Id = FE.Emp_Id
	LEFT JOIN {{source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa')}} E
		ON E.Empresa_Id = FE.Emp_Id
	LEFT JOIN {{ref ('BI_Kielsa_Dim_Sucursal')}} SUC
		ON SUC.Sucursal_Id = FE.Suc_Id
		AND SUC.Emp_Id = FE.Emp_Id
	LEFT JOIN {{source ('BI_FARINTER', 'BI_Hecho_SameSucursales_Kielsa')}} SAM
		ON SAM.Sucursal_Id_Solo = FE.Suc_Id
		AND SAM.Pais_Id = FE.Emp_Id
		AND SAM.Anio_Id = CAL.Anio_Calendario
		AND SAM.Mes_Id = CAL.Mes_Calendario
	LEFT JOIN {{ref ('BI_Kielsa_Dim_TipoDocumentoSub')}} DOC
		ON DOC.Documento_Id = FE.TipoDoc_Id
		AND DOC.SubDocumento_Id = FE.SubDoc_Id
		AND DOC.Emp_Id = FE.Emp_Id
	LEFT JOIN {{ref ('BI_Kielsa_Dim_Cliente')}} CLI
		ON CLI.Cliente_Id = FE.Cliente_Id
		AND CLI.Emp_Id = FE.Emp_Id
	{% if is_incremental() and last_date != '19000101' %} 
	WHERE FE.Fecha_Actualizado >= '{{ last_date }}' AND FE.Factura_Fecha >= DATEADD(MONTH, -1, GETDATE())
	{% else %}
	WHERE FE.Factura_Fecha >= DATEADD(YEAR, -3, GETDATE()) AND FE.AnioMes_Id >= YEAR(DATEADD(YEAR, -3, GETDATE()))*100 + 1
	{% endif %}
) 
SELECT *
    , ISNULL({{ dwh_farinter_hash_column( columns = ["Factura_Id","Suc_Id","Emp_Id","TipoDoc_Id","Caja_Id"], table_alias="") }},'') AS [HashStr_FacSucEmpDocCaj]   
    , ISNULL(GETDATE(),'19000101') AS [Fecha_Actualizado]

FROM Facturas
