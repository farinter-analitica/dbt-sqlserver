{%- set nombre_esquema_particion = "ps_" + this.identifier + "_fecha" -%}
{%- if is_incremental() -%}
	{% set sql_inicializar_particion= '' %}
{%- else -%}
	{%- set sql_inicializar_particion %}
		EXEC ADM_FARINTER.dbo.pa_inicializar_particiones
			@p_base_datos = '{{this.database}}',
			@p_nombre_esquema_particion = '{{nombre_esquema_particion}}',
			@p_nombre_funcion_particion = '{{"pf_" + this.identifier + "_fecha"}}',
			@p_periodo_tipo = 'Mensual', --'Anual' o 'Mensual'
			@p_tipo_datos = 'Fecha', --'Fecha' o 'AnioMes'
			@p_fecha_base = '2018-01-01'
	{% endset -%}
{%- endif -%}
{%- set on_clause = nombre_esquema_particion ~ "([Factura_Fecha])" -%}
{%- set unique_key_list = ["Factura_Id","Suc_Id","Emp_Id","TipoDoc_Id","Caja_Id","Factura_Fecha","Articulo_Id"] -%}

{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario","automation/periodo_por_hora"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		on_clause_filegroup = on_clause,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		pre_hook=[sql_inicializar_particion],
		post_hook=[
		"{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(),
			if_another_exists_drop_it=true) }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", 
			create_clustered=false, 
			is_incremental=is_incremental(), 
			if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado'], included_columns=['Factura_Fecha']) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Factura_Fecha']) }}",
		"EXEC ADM_FARINTER.dbo.pa_comprimir_indices_particiones_anteriores 
			@p_base_datos = '{{this.database}}',
		 	@p_esquema_tabla = '{{this.schema}}', 
			@p_nombre_tabla = '{{this.identifier}}', 
			@p_tipo_datos = 'Fecha';"		
			]
		
) }}

{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{% else %}
	{% set last_date = '19000101' %}
{% endif %}
--20230927: Axell Padilla > Vista de las posiciones de facturas para crear el hecho.
--20240901: Axell Padilla > Migracion a dbt
--20241018: Axell Padilla > Agregado historico de la arboleda

	{% if is_incremental() and last_date != '19000101' %} 
WITH Resumen_Fecha_Desde AS
	(
		SELECT AnioMes_Id, CAST(MIN(Factura_Fecha) AS DATE) AS Fecha_Desde
		FROM {{source ('DL_FARINTER', 'DL_Kielsa_FacturasPosiciones')}} FP
		WHERE FP.DWH_Fecha_Actualizado >= '{{ last_date }}' 
		AND FP.Factura_Fecha >= DATEADD(MONTH, -1, GETDATE()) 
		AND FP.AnioMes_Id >= YEAR(DATEADD(MONTH, -1, GETDATE()))*100 + MONTH(DATEADD(MONTH, -1, GETDATE()))
		GROUP BY AnioMes_Id
	)
	{% else %}
	---FULL REFRESH
	{% endif %}
SELECT
	FE.[Factura_Fecha] Factura_Fecha
	, DATEPART(HOUR,FE.[Factura_FechaHora]) Hora_Id
	, FP.[Detalle_Fecha]
	, CAST(FE.[Emp_Id] AS SMALLINT) AS [Emp_Id]
	, FE.[Suc_Id]
	, FE.[Bodega_Id]
	, FE.[Caja_Id]
	, FE.[TipoDoc_Id]
	, FE.[Factura_Id]
	, FP.[Detalle_Id]
	, FP.[Articulo_Id]
	, FP.[Detalle_Cantidad]
	, FP.[Cantidad_Padre]
	, FP.[Valor_Bruto]
	, FP.[Valor_Neto]
	, FP.[Valor_Utilidad]
	, FP.[Valor_Costo]
	, FP.[Valor_Descuento]
	, FP.[Valor_Impuesto]
	, FP.[Valor_Descuento_Financiero]
	, FP.[Valor_Acum_Monedero]
	, FP.[Valor_Descuento_Cupon]
	, FP.[Valor_Descuento_Monedero]
	, DTE.[Valor_Descuento_Tercera_Edad]
	, FP.[Detalle_Precio_Unitario]
	, FP.[Detalle_Descuento_Monto]
	, FP.[Detalle_Costo_Unitario]
	, FP.[Detalle_Impuesto_Monto]
	, FP.[Detalle_AcumMonedero]
	, FP.[Detalle_Saldo]
	, FP.[Detalle_Costo_Unitario_Dolar]
	, FP.[Detalle_Descuento_Porc]
	, FP.[Detalle_Impuesto_Porc]
	, FP.[Detalle_Total]
	, FP.[Detalle_PorcMonedero]
	, FP.[Detalle_Regalo]
	, FP.[Detalle_Gasto_Monto]
	, FP.[Detalle_Manejo_Costo]
	, FP.[Detalle_Costo_Fact]
	, FP.[Detalle_Unidad_Simbolo]
	, FP.[Detalle_Precio_Original]
	, FP.[Detalle_Desc_Financiero_Porc]
	, FE.[SubDoc_Id]
	, FE.[Consecutivo_Factura]
	, FE.[MonederoTarj_Id]
	, FE.Monedero_Id [MonederoTarj_Id_Limpio] --borrar cuando sea posible
	, FE.[Monedero_Id]
	, FE.[Cliente_Id]
	, FE.[Vendedor_Id]
	, FE.[Factura_Estado]
	, FE.[Factura_Origen]
	, FE.[AnioMes_Id]
	, CAL.Dia_de_la_Semana
	, E.Pais_Id
    , ART.Hash_ArticuloEmp --borrar cuando sea posible
	, ART.HashStr_ArtEmp
    , C.Hash_CasaEmp --borrar cuando sea posible
	, CASE
			WHEN FEXP.Factura_Id IS NOT NULL
				THEN 5 --eCommerce
			ELSE CASE FE.Factura_Origen
						WHEN 'FA'
							THEN 1	--Venta de sucursal
						WHEN 'EX'
							THEN 2	--Domicilio
						WHEN 'PU'
							THEN 3	--Recoger en sucursal
						WHEN 'PR'
							THEN 4	--Preventa
						ELSE 0
					END --Origen desconocido
		END 
		AS CanalVenta_Id
	, FE.HashStr_CliEmp
	, FE.HashStr_MonEmp
	, FE.EmpMon_Id
	, FE.HashStr_SubDDocEmp
	, FE.HashStr_SucEmp
	, FE.HashStr_SucBodEmp
	, FE.HashStr_EmplEmp
	, C.HashStr_CasaEmp 
    , SUC.Hash_SucursalEmp --borrar cuando sea posible
	, DOC.Hash_SubDocEmp --borrar cuando sea posible
	, CAST(CAST(CLI.HashStr_CliEmp AS varbinary) AS BIGINT)  Hash_ClienteEmp --borrar cuando sea posible
	, MON.Hash_MonederoEmp --borrar cuando sea posible
	, ISNULL(SAM.Tipo_Id, 0) AS Same_Id --borrar cuando sea posible
	, CASE
		WHEN FP.Valor_Utilidad >= 0
			THEN 1
		ELSE 0
	END AS TipoUtilidad_Id
	, (FP.Detalle_Cantidad * FP.Detalle_Precio_Unitario * (ISNULL(FPD.Descuento_Porcentaje, 0) / 100.0)) AS [Descuento_Proveedor]
	, {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'TipoDoc_Id', 'Caja_Id', 'Factura_Id'], input_length=49, table_alias='FE')}} [EmpSucDocCajFac_Id]
	{% if is_incremental() and last_date != '19000101' %} 
	, ISNULL(GETDATE(), '19000101') AS Fecha_Actualizado
	{% else %}
	, CASE WHEN FE.[Fecha_Actualizado] > GETDATE()-7 THEN GETDATE() ELSE ISNULL(FE.[Fecha_Actualizado],'19000101') END AS Fecha_Actualizado
	{% endif %}
FROM {{ref ('BI_Kielsa_Hecho_FacturaEncabezado')}} FE
{% if is_incremental() and last_date != '19000101' %} 
INNER JOIN  Resumen_Fecha_Desde Res
	ON FE.AnioMes_Id = Res.AnioMes_Id 
	AND FE.Factura_Fecha >= Res.Fecha_Desde 
{% endif %}
INNER JOIN (
		SELECT  [Emp_Id], [Suc_Id], [TipoDoc_id], [Caja_Id], [Factura_Id], FP.[AnioMes_Id], [Articulo_Id]
		, MAX([Detalle_Fecha] ) AS [Detalle_Fecha]
		, MAX(FP.[Detalle_Id]) AS [Detalle_Id]
		, SUM(FP.[Detalle_Cantidad]) AS [Detalle_Cantidad]
		, SUM(FP.[Cantidad_Padre]) AS [Cantidad_Padre]
		, SUM(FP.[Valor_Bruto]) 	AS [Valor_Bruto]
		, SUM(FP.[Valor_Neto]) 	AS [Valor_Neto]
		, SUM(FP.[Valor_Utilidad]) AS [Valor_Utilidad]
		, SUM(FP.[Valor_Costo]) 	AS [Valor_Costo]
		, SUM(FP.[Valor_Descuento]) AS [Valor_Descuento]
		, SUM(FP.[Valor_Impuesto]) AS [Valor_Impuesto]
		, SUM(FP.[Valor_Descuento_Financiero]) AS [Valor_Descuento_Financiero]
		, SUM(FP.[Valor_Acum_Monedero]) AS [Valor_Acum_Monedero]
		, SUM(FP.[Valor_Descuento_Cupon]) AS [Valor_Descuento_Cupon]
		, SUM(FP.[Valor_Descuento_Monedero]) AS [Valor_Descuento_Monedero]
		, MAX(FP.[Detalle_Precio_Unitario]) AS [Detalle_Precio_Unitario]
		, MAX(FP.[Detalle_Descuento_Monto]) AS [Detalle_Descuento_Monto]
		, MAX(FP.[Detalle_Costo_Unitario]) AS [Detalle_Costo_Unitario]
		, MAX(FP.[Detalle_Impuesto_Monto]) AS [Detalle_Impuesto_Monto]
		, MAX(FP.[Detalle_AcumMonedero]) AS [Detalle_AcumMonedero]
		, MAX(FP.[Detalle_Saldo]) AS [Detalle_Saldo]
		, MAX(FP.[Detalle_Costo_Unitario_Dolar]) AS [Detalle_Costo_Unitario_Dolar]
		, MAX(FP.[Detalle_Descuento_Porc]) AS [Detalle_Descuento_Porc]
		, MAX(FP.[Detalle_Impuesto_Porc]) AS [Detalle_Impuesto_Porc]
		, MAX(FP.[Detalle_Total]) AS [Detalle_Total]
		, MAX(FP.[Detalle_PorcMonedero]) AS [Detalle_PorcMonedero]
		, MAX(CAST(FP.[Detalle_Regalo] AS SMALLINT)) AS [Detalle_Regalo]
		, MAX(FP.[Detalle_Gasto_Monto]) AS [Detalle_Gasto_Monto]
		, MAX(FP.[Detalle_Manejo_Costo]) AS [Detalle_Manejo_Costo]
		, MAX(FP.[Detalle_Costo_Fact]) AS [Detalle_Costo_Fact]
		, MAX(FP.[Detalle_Unidad_Simbolo]) AS [Detalle_Unidad_Simbolo]
		, MAX(FP.[Detalle_Precio_Original]) AS [Detalle_Precio_Original]
		, MAX(FP.[Detalle_Desc_Financiero_Porc]) 	AS [Detalle_Desc_Financiero_Porc]

		FROM {{source ('DL_FARINTER', 'DL_Kielsa_FacturasPosiciones')}} FP
		{% if is_incremental() and last_date != '19000101' %} 
		--Esto es por posicion, delimitando encabezado a un mes
		INNER JOIN  Resumen_Fecha_Desde Res
			ON FP.AnioMes_Id = Res.AnioMes_Id 
			AND FP.Factura_Fecha >= Res.Fecha_Desde 
		{% else %}
		WHERE FP.Factura_Fecha >= DATEADD(YEAR, -3, GETDATE()) AND FP.AnioMes_Id >= YEAR(DATEADD(YEAR, -3, GETDATE()))*100 + 1
		{% endif %}
		GROUP BY FP.[AnioMes_Id], [Emp_Id], [Suc_Id], [TipoDoc_id], [Caja_Id], [Factura_Id],  [Articulo_Id]
	) FP
	ON FE.Emp_Id = FP.Emp_Id
	AND FE.Suc_Id = FP.Suc_Id
	AND FE.TipoDoc_id = FP.TipoDoc_id
	AND FE.Caja_Id = FP.Caja_Id
	AND FE.Factura_Id = FP.Factura_Id
	AND FE.AnioMes_Id = FP.AnioMes_Id
INNER JOIN {{source ('BI_FARINTER', 'BI_Dim_Calendario')}} CAL --Optimizacion por tema de particiones
	ON CAL.Fecha_Id = FE.Factura_Fecha AND CAL.AnioMes_Id = FE.AnioMes_Id
LEFT JOIN {{ref ('BI_Kielsa_Dim_Monedero')}} MON
	ON MON.Monedero_Id = FE.Monedero_Id
	AND MON.Emp_Id = FP.Emp_Id
LEFT JOIN (SELECT Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id, AnioMes_Id, Articulo_Id, SUM(Descuento_Porcentaje) AS Descuento_Porcentaje 
		FROM {{source ('DL_FARINTER', 'DL_Kielsa_FacturaPosicionDescuento')}} 
		WHERE Descuento_Origen = 6
		GROUP BY AnioMes_Id,Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id,  Articulo_Id) FPD
	ON FPD.Emp_Id = FP.Emp_Id
	AND FPD.TipoDoc_Id = FP.TipoDoc_Id
	AND FPD.Suc_Id = FP.Suc_Id
	AND FPD.Caja_Id = FP.Caja_Id
	AND FPD.Factura_Id = FP.Factura_Id
	AND FPD.Articulo_Id = FP.Articulo_Id
	AND FPD.AnioMes_Id = FP.AnioMes_Id	
LEFT JOIN (SELECT Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id, AnioMes_Id, Articulo_Id, SUM(Descuento_Monto) AS Valor_Descuento_Tercera_Edad 
		FROM {{source ('DL_FARINTER', 'DL_Kielsa_FacturaPosicionDescuento')}} 
		WHERE Descuento_Origen = 1 AND Descuento_Nombre LIKE 'TERCERA%EDAD%'
		GROUP BY AnioMes_Id,Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id,  Articulo_Id) DTE
	ON DTE.Emp_Id = FP.Emp_Id
	AND DTE.TipoDoc_Id = FP.TipoDoc_Id
	AND DTE.Suc_Id = FP.Suc_Id
	AND DTE.Caja_Id = FP.Caja_Id
	AND DTE.Factura_Id = FP.Factura_Id
	AND DTE.Articulo_Id = FP.Articulo_Id
	AND DTE.AnioMes_Id = FP.AnioMes_Id	
LEFT JOIN {{ref ('BI_Kielsa_Dim_Articulo')}} ART
	ON ART.Articulo_Id = FP.Articulo_Id
	AND ART.Emp_Id = FP.Emp_Id
LEFT JOIN {{source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa')}} E
	ON E.Empresa_Id = FE.Emp_Id
LEFT JOIN {{ref ('BI_Kielsa_Dim_Casa')}} C
	ON C.Casa_Id = ART.Casa_Id
	AND C.Emp_Id = ART.Emp_Id
LEFT JOIN {{ref ('BI_Kielsa_Dim_Sucursal')}} SUC
    ON SUC.Sucursal_Id = FE.Suc_Id
	AND SUC.Emp_Id = FE.Emp_Id
LEFT JOIN {{source ('BI_FARINTER', 'BI_Hecho_SameSucursales_Kielsa')}} SAM
	ON SAM.Sucursal_Id_Solo = FE.Suc_Id
	AND SAM.Pais_Id = FE.Emp_Id
	AND SAM.Anio_Id = CAL.Anio_Calendario
	AND SAM.Mes_Id = CAL.Mes_Calendario
LEFT JOIN {{ref('BI_Kielsa_Dim_TipoDocumentoSub')}} DOC
    ON DOC.Documento_Id = FE.TipoDoc_Id
	AND DOC.SubDocumento_Id = FE.SubDoc_Id
	AND DOC.Emp_Id = FE.Emp_Id
LEFT JOIN {{ref('BI_Kielsa_Dim_Cliente')}} CLI
	ON CLI.Cliente_Id = FE.Cliente_Id
	AND CLI.Emp_Id = FE.Emp_Id
LEFT JOIN (SELECT DISTINCT A.Emp_Id, A.TipoDoc_Id, A.Suc_Id, A.Caja_Id, A.Factura_Id 
		FROM {{ref('DL_Kielsa_Exp_Factura_Express')}} A
		WHERE A.Orden_Usuario_Registro = 'WEB'
				AND A.Estatus_Id = 'T'
		)	FEXP
	ON FE.Emp_Id = FEXP.Emp_Id
	AND FE.TipoDoc_Id = FEXP.TipoDoc_Id
	AND FE.Suc_Id = FEXP.Suc_Id
	AND FE.Caja_Id = FEXP.Caja_Id
	AND FE.Factura_Id = FEXP.Factura_Id 
{% if is_incremental() and last_date != '19000101' %} 
--Esto es por posicion, delimitando encabezado a un mes
WHERE FE.Factura_Fecha >= DATEADD(MONTH, -1, GETDATE()) AND FE.AnioMes_Id >= YEAR(DATEADD(MONTH, -1, GETDATE()))*100 + 1
{% else %}
WHERE FE.Factura_Fecha >= DATEADD(YEAR, -3, GETDATE()) AND FE.AnioMes_Id >= YEAR(DATEADD(YEAR, -3, GETDATE()))*100 + 1
{% endif %}

--OPTION (FORCE ORDER);
	    
--SELECT TOP 1000 * FROM BI_Kielsa_Hecho_FacturaPosicion
{% if not is_incremental() and (modules.datetime.datetime.now() - modules.datetime.timedelta(days=5*365)) < modules.datetime.datetime(2024, 10, 1) %}
UNION ALL
--Carga de historico sistema anterior arboleda
SELECT
	FE.[Factura_Fecha] Factura_Fecha
	, DATEPART(HOUR,FE.[Factura_FechaHora]) Hora_Id
	, FP.[Detalle_Fecha]
	, CAST(444 AS SMALLINT) AS [Emp_Id] --Nuevo ID
	, FE.[Suc_Id]
	, FE.[Bodega_Id]
	, FE.[Caja_Id]
	, FE.[TipoDoc_Id]
	, FE.[Factura_Id]
	, FP.[Detalle_Id]
	, FP.[Articulo_Id]
	, FP.[Detalle_Cantidad]
	, FP.[Cantidad_Padre]
	, FP.[Valor_Bruto]
	, FP.[Valor_Neto]
	, FP.[Valor_Utilidad]
	, FP.[Valor_Costo]
	, FP.[Valor_Descuento]
	, FP.[Valor_Impuesto]
	, FP.[Valor_Descuento_Financiero]
	, FP.[Valor_Acum_Monedero]
	, FP.[Valor_Descuento_Cupon]
	, FP.[Valor_Descuento_Monedero]
	, DTE.[Valor_Descuento_Tercera_Edad]
	, FP.[Detalle_Precio_Unitario]
	, FP.[Detalle_Descuento_Monto]
	, FP.[Detalle_Costo_Unitario]
	, FP.[Detalle_Impuesto_Monto]
	, FP.[Detalle_AcumMonedero]
	, FP.[Detalle_Saldo]
	, FP.[Detalle_Costo_Unitario_Dolar]
	, FP.[Detalle_Descuento_Porc]
	, FP.[Detalle_Impuesto_Porc]
	, FP.[Detalle_Total]
	, FP.[Detalle_PorcMonedero]
	, FP.[Detalle_Regalo]
	, FP.[Detalle_Gasto_Monto]
	, FP.[Detalle_Manejo_Costo]
	, FP.[Detalle_Costo_Fact]
	, FP.[Detalle_Unidad_Simbolo]
	, FP.[Detalle_Precio_Original]
	, FP.[Detalle_Desc_Financiero_Porc]
	, FE.[SubDoc_Id]
	, FE.[Consecutivo_Factura]
	, FE.[MonederoTarj_Id]
	, FE.Monedero_Id [MonederoTarj_Id_Limpio] --borrar cuando sea posible
	, FE.[Monedero_Id]
	, FE.[Cliente_Id]
	, FE.[Vendedor_Id]
	, FE.[Factura_Estado]
	, FE.[Factura_Origen]
	, FE.[AnioMes_Id]
	, CAL.Dia_de_la_Semana
	, E.Pais_Id
    , ART.Hash_ArticuloEmp --borrar cuando sea posible
	, ART.HashStr_ArtEmp
    , C.Hash_CasaEmp --borrar cuando sea posible
	, CASE
			WHEN FEXP.Factura_Id IS NOT NULL
				THEN 5 --eCommerce
			ELSE CASE FE.Factura_Origen
						WHEN 'FA'
							THEN 1	--Venta de sucursal
						WHEN 'EX'
							THEN 2	--Domicilio
						WHEN 'PU'
							THEN 3	--Recoger en sucursal
						WHEN 'PR'
							THEN 4	--Preventa
						ELSE 0
					END --Origen desconocido
		END 
		AS CanalVenta_Id
	, FE.HashStr_CliEmp
	, FE.HashStr_MonEmp
	, FE.EmpMon_Id
	, FE.HashStr_SubDDocEmp
	, FE.HashStr_SucEmp
	, FE.HashStr_SucBodEmp
	, FE.HashStr_EmplEmp
	, C.HashStr_CasaEmp 
    , SUC.Hash_SucursalEmp --borrar cuando sea posible
	, DOC.Hash_SubDocEmp --borrar cuando sea posible
	, CAST(CAST(CLI.HashStr_CliEmp AS varbinary) AS BIGINT)  Hash_ClienteEmp --borrar cuando sea posible
	, MON.Hash_MonederoEmp --borrar cuando sea posible
	, ISNULL(SAM.Tipo_Id, 0) AS Same_Id --borrar cuando sea posible
	, CASE
		WHEN FP.Valor_Utilidad >= 0
			THEN 1
		ELSE 0
	END AS TipoUtilidad_Id
	, (FP.Detalle_Cantidad * FP.Detalle_Precio_Unitario * (ISNULL(FPD.Descuento_Porcentaje, 0) / 100.0)) AS [Descuento_Proveedor]
	, {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'TipoDoc_Id', 'Caja_Id', 'Factura_Id'], input_length=49, table_alias='FE')}} [EmpSucDocCajFac_Id]
	, ISNULL(FE.[Factura_Fecha],'19000101') AS Fecha_Actualizado
FROM [dbo].[BI_Kielsa_Hecho_FacturaEncabezado] FE -- {{ref ('BI_Kielsa_Hecho_FacturaEncabezado')}} FE
INNER JOIN (
		SELECT  [Emp_Id], [Suc_Id], [TipoDoc_id], [Caja_Id], [Factura_Id], FP.[AnioMes_Id], [Articulo_Id]
		, MAX([Detalle_Fecha] ) AS [Detalle_Fecha]
		, MAX(FP.[Detalle_Id]) AS [Detalle_Id]
		, SUM(FP.[Detalle_Cantidad]) AS [Detalle_Cantidad]
		, SUM(FP.[Cantidad_Padre]) AS [Cantidad_Padre]
		, SUM(FP.[Valor_Bruto]) 	AS [Valor_Bruto]
		, SUM(FP.[Valor_Neto]) 	AS [Valor_Neto]
		, SUM(FP.[Valor_Utilidad]) AS [Valor_Utilidad]
		, SUM(FP.[Valor_Costo]) 	AS [Valor_Costo]
		, SUM(FP.[Valor_Descuento]) AS [Valor_Descuento]
		, SUM(FP.[Valor_Impuesto]) AS [Valor_Impuesto]
		, SUM(FP.[Valor_Descuento_Financiero]) AS [Valor_Descuento_Financiero]
		, SUM(FP.[Valor_Acum_Monedero]) AS [Valor_Acum_Monedero]
		, SUM(FP.[Valor_Descuento_Cupon]) AS [Valor_Descuento_Cupon]
		, SUM(FP.[Valor_Descuento_Monedero]) AS [Valor_Descuento_Monedero]
		, MAX(FP.[Detalle_Precio_Unitario]) AS [Detalle_Precio_Unitario]
		, MAX(FP.[Detalle_Descuento_Monto]) AS [Detalle_Descuento_Monto]
		, MAX(FP.[Detalle_Costo_Unitario]) AS [Detalle_Costo_Unitario]
		, MAX(FP.[Detalle_Impuesto_Monto]) AS [Detalle_Impuesto_Monto]
		, MAX(FP.[Detalle_AcumMonedero]) AS [Detalle_AcumMonedero]
		, MAX(FP.[Detalle_Saldo]) AS [Detalle_Saldo]
		, MAX(FP.[Detalle_Costo_Unitario_Dolar]) AS [Detalle_Costo_Unitario_Dolar]
		, MAX(FP.[Detalle_Descuento_Porc]) AS [Detalle_Descuento_Porc]
		, MAX(FP.[Detalle_Impuesto_Porc]) AS [Detalle_Impuesto_Porc]
		, MAX(FP.[Detalle_Total]) AS [Detalle_Total]
		, MAX(FP.[Detalle_PorcMonedero]) AS [Detalle_PorcMonedero]
		, MAX(CAST(FP.[Detalle_Regalo] AS SMALLINT)) AS [Detalle_Regalo]
		, MAX(FP.[Detalle_Gasto_Monto]) AS [Detalle_Gasto_Monto]
		, MAX(FP.[Detalle_Manejo_Costo]) AS [Detalle_Manejo_Costo]
		, MAX(FP.[Detalle_Costo_Fact]) AS [Detalle_Costo_Fact]
		, MAX(FP.[Detalle_Unidad_Simbolo]) AS [Detalle_Unidad_Simbolo]
		, MAX(FP.[Detalle_Precio_Original]) AS [Detalle_Precio_Original]
		, MAX(FP.[Detalle_Desc_Financiero_Porc]) 	AS [Detalle_Desc_Financiero_Porc]
		FROM [DL_FARINTER].[dbo].[DL_Kielsa_FacturasPosiciones_ArboledaOld] FP --{{source ('DL_FARINTER', 'DL_Kielsa_FacturasPosiciones_ArboledaOld')}} FP
		WHERE FP.Factura_Fecha >= DATEADD(YEAR, -3, GETDATE()) AND FP.AnioMes_Id >= YEAR(DATEADD(YEAR, -3, GETDATE()))*100 + 1
		GROUP BY FP.[AnioMes_Id], [Emp_Id], [Suc_Id], [TipoDoc_id], [Caja_Id], [Factura_Id],  [Articulo_Id]
	) FP
	ON FE.Emp_Id = FP.Emp_Id
	AND FE.Suc_Id = FP.Suc_Id
	AND FE.TipoDoc_id = FP.TipoDoc_id
	AND FE.Caja_Id = FP.Caja_Id
	AND FE.Factura_Id = FP.Factura_Id
	AND FE.AnioMes_Id = FP.AnioMes_Id
INNER JOIN {{source ('BI_FARINTER', 'BI_Dim_Calendario')}} CAL --Optimizacion por tema de particiones
	ON CAL.Fecha_Id = FE.Factura_Fecha AND CAL.AnioMes_Id = FE.AnioMes_Id
LEFT JOIN {{ref ('BI_Kielsa_Dim_Monedero')}} MON
	ON MON.Monedero_Id = FE.Monedero_Id
	AND MON.Emp_Id = FP.Emp_Id
LEFT JOIN (SELECT Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id, AnioMes_Id, Articulo_Id, SUM(Descuento_Porcentaje) AS Descuento_Porcentaje 
		FROM {{source ('DL_FARINTER', 'DL_Kielsa_FacturaPosicionDescuento')}} 
		WHERE Descuento_Origen = 6
		GROUP BY AnioMes_Id,Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id,  Articulo_Id) FPD
	ON FPD.Emp_Id = FP.Emp_Id
	AND FPD.TipoDoc_Id = FP.TipoDoc_Id
	AND FPD.Suc_Id = FP.Suc_Id
	AND FPD.Caja_Id = FP.Caja_Id
	AND FPD.Factura_Id = FP.Factura_Id
	AND FPD.Articulo_Id = FP.Articulo_Id
	AND FPD.AnioMes_Id = FP.AnioMes_Id	
LEFT JOIN (SELECT Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id, AnioMes_Id, Articulo_Id, SUM(Descuento_Monto) AS Valor_Descuento_Tercera_Edad 
		FROM {{source ('DL_FARINTER', 'DL_Kielsa_FacturaPosicionDescuento')}} 
		WHERE Descuento_Origen = 1 AND Descuento_Nombre LIKE 'TERCERA%EDAD%'
		GROUP BY AnioMes_Id,Emp_Id, TipoDoc_Id, Suc_Id, Caja_Id, Factura_Id,  Articulo_Id) DTE
	ON DTE.Emp_Id = FP.Emp_Id
	AND DTE.TipoDoc_Id = FP.TipoDoc_Id
	AND DTE.Suc_Id = FP.Suc_Id
	AND DTE.Caja_Id = FP.Caja_Id
	AND DTE.Factura_Id = FP.Factura_Id
	AND DTE.Articulo_Id = FP.Articulo_Id
	AND DTE.AnioMes_Id = FP.AnioMes_Id	
LEFT JOIN {{ref ('BI_Kielsa_Dim_Articulo')}} ART
	ON ART.Articulo_Id = FP.Articulo_Id
	AND ART.Emp_Id = FP.Emp_Id
LEFT JOIN {{source ('BI_FARINTER', 'BI_Kielsa_Dim_Empresa')}} E
	ON E.Empresa_Id = FE.Emp_Id
LEFT JOIN {{ref ('BI_Kielsa_Dim_Casa')}} C
	ON C.Casa_Id = ART.Casa_Id
	AND C.Emp_Id = ART.Emp_Id
LEFT JOIN {{ref ('BI_Kielsa_Dim_Sucursal')}} SUC
    ON SUC.Sucursal_Id = FE.Suc_Id
	AND SUC.Emp_Id = FE.Emp_Id
LEFT JOIN {{source ('BI_FARINTER', 'BI_Hecho_SameSucursales_Kielsa')}} SAM
	ON SAM.Sucursal_Id_Solo = FE.Suc_Id
	AND SAM.Pais_Id = FE.Emp_Id
	AND SAM.Anio_Id = CAL.Anio_Calendario
	AND SAM.Mes_Id = CAL.Mes_Calendario
LEFT JOIN {{ref('BI_Kielsa_Dim_TipoDocumentoSub')}} DOC
    ON DOC.Documento_Id = FE.TipoDoc_Id
	AND DOC.SubDocumento_Id = FE.SubDoc_Id
	AND DOC.Emp_Id = FE.Emp_Id
LEFT JOIN {{ref('BI_Kielsa_Dim_Cliente')}} CLI
	ON CLI.Cliente_Id = FE.Cliente_Id
	AND CLI.Emp_Id = FE.Emp_Id
LEFT JOIN (SELECT DISTINCT A.Emp_Id, A.TipoDoc_Id, A.Suc_Id, A.Caja_Id, A.Factura_Id 
		FROM {{ref('DL_Kielsa_Exp_Factura_Express')}} A
		WHERE A.Orden_Usuario_Registro = 'WEB'
				AND A.Estatus_Id = 'T'
		)	FEXP
	ON FE.Emp_Id = FEXP.Emp_Id
	AND FE.TipoDoc_Id = FEXP.TipoDoc_Id
	AND FE.Suc_Id = FEXP.Suc_Id
	AND FE.Caja_Id = FEXP.Caja_Id
	AND FE.Factura_Id = FEXP.Factura_Id
WHERE FE.Factura_Fecha >= DATEADD(YEAR, -3, GETDATE()) AND FE.AnioMes_Id >= YEAR(DATEADD(YEAR, -3, GETDATE()))*100 + 1

{% endif %}