{% set unique_key_list = ["Emp_Id","Sucursal_Id","Articulo_Id","Orden_Id","Fecha_Id"] -%}

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
        ]
	) 
}}

{%- if is_incremental() %}
	{%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -120, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}

WITH
Alertas AS
	(SELECT
		MAX(Alerta.Alerta_Id) AS PV_Alerta_Id, Alerta.Articulo_Id AS Articulo_Id, Alerta.Emp_Id
	FROM	DL_FARINTER.dbo.DL_Kielsa_Articulo_Alerta Alerta --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo_Alerta')}}
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_PV_Alerta PVAlerta --{{ source('DL_FARINTER', 'DL_Kielsa_PV_Alerta')}}
		ON Alerta.Alerta_Id = PVAlerta.Alerta_Id AND Alerta.Emp_Id = PVAlerta.Emp_Id
	WHERE Bit_Cronico = 0
		AND Bit_Recomendacion = 1
		AND (PVAlerta.Alerta_Id BETWEEN 1 AND 12)
		AND GETDATE() BETWEEN PVAlerta.Fecha_Inicio AND PVAlerta.Fecha_Final
	GROUP BY Alerta.Emp_Id, Alerta.Articulo_Id)
,
Ingresos AS
	(SELECT
		Ingresos.Emp_Id
		, Ingresos.Sucursal_Id
		, Ingresos.Articulo_Id
		, Ingresos.Orden_Id AS Orden_Id
		, Ingresos.Anio_Id
		, Ingresos.Mes_Id
		, Ingresos.Boleta_Id
		, SUM(Ingresos.Ingreso_Facturada) AS Facturada
	FROM	[BI_FARINTER].[dbo].[BI_Kielsa_Hecho_IngresoHist] Ingresos	--{{ ref('BI_Kielsa_Hecho_IngresoHist')}}
		{% if is_incremental() -%} 
		WHERE Ingresos.Fecha_Id > '{{ last_date }}' 
		{% else -%}
		WHERE Ingresos.Fecha_Id >= '{{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=360*3)).replace(day=1, month=1, hour=0, minute=0, second=0).strftime('%Y%m%d')  }}'
		{% endif -%}
	GROUP BY Ingresos.Emp_Id
			, Ingresos.Sucursal_Id
			, Ingresos.Articulo_Id
			, Ingresos.Orden_Id
			, Ingresos.Anio_Id
			, Ingresos.Mes_Id
			, Ingresos.Boleta_Id)
,
ComprasLocales AS
	(SELECT
		Suc.Emp_Id
		, Suc.Sucursal_Id AS Sucursal_Id
		, MAX(Suc.Zona_Id) AS Zona_Id
		, MAX(Suc.Departamento_Id) AS Departamento_Id
		, MAX(Suc.Municipio_Id) AS Municipio_Id
		, MAX(Suc.Ciudad_Id) AS Ciudad_Id
		, MAX(Suc.TipoSucursal_id) AS TipoSucursal_Id
		, MAX(OrdenDetalle.Articulo_Id) AS Articulo_Id
		, MAX(ISNULL(Art.Casa_Id, ArtInfo.Casa_Id)) AS Casa_Id
		, MAX(ISNULL(Art.Marca_Id, ArtInfo.Marca_Id)) AS Marca1_Id
		, MAX(ISNULL(Art.Categoria_Id, ArtInfo.Categoria_Id)) AS CategoriaArt_Id
		, MAX(ISNULL(Art.DeptoArt_Id, ArtInfo.Depto_Id)) AS DeptoArt_Id
		, MAX(ISNULL(Art.SubCategoria1Art_Id, ArtInfo.SubCategoria_Id)) AS SubCategoria1Art_Id
		, MAX(ISNULL(Art.SubCategoria2Art_Id, ArtInfo.SubCategoria2_Id)) AS SubCategoria2Art_Id
		, MAX(ISNULL(Art.SubCategoria3Art_Id, ArtInfo.SubCategoria3_Id)) AS SubCategoria3Art_Id
		, MAX(ISNULL(Art.SubCategoria4Art_Id, ArtInfo.SubCategoria4_Id)) AS SubCategoria4Art_Id
		, MAX(OrdenEnc.Prov_Id) AS Proveedor_Id
		, MAX(ISNULL(CASE
						WHEN Alertas.Emp_Id = 2
							THEN Alertas.PV_Alerta_Id - 1
						WHEN Alertas.Emp_Id = 3
							THEN Alertas.PV_Alerta_Id - 3
						WHEN Alertas.Emp_Id = 5
							THEN Alertas.PV_Alerta_Id - 5
						ELSE Alertas.PV_Alerta_Id
					END
					, 0)) AS Cuadro_Id
		, MAX(CAST(ISNULL(CAST(ArtxMec.MecanicaCanje_Id AS VARCHAR), 'x') AS VARCHAR)) AS Mecanica_Id
		, MAX(CASE Art.ABC_Cadena WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 WHEN 'D' THEN 4 WHEN 'E' THEN 5 ELSE
																												0 END) AS ABCCadena_Id
		, MAX(CASE OrdenEnc.Orden_Estado WHEN 'GU' THEN 1 WHEN 'AN' THEN 2 WHEN 'RE' THEN 3 WHEN 'AP' THEN 4 ELSE 0 END) AS CompraEstado_Id
		, 1 AS TipoCompra_Id
		, MAX(CASE
				WHEN YEAR(Estado.Fecha_Id) = YEAR(GETDATE())
					AND Estado.Mes_Id = MONTH(GETDATE())
					AND CONVERT(DATE, OrdenEnc.Orden_Fecha_Crea) > DATEADD(month, -3, GETDATE())
					THEN Estado.Estado1
				ELSE 0
			END) AS DiasInv_Id
		, MAX(CASE
				WHEN YEAR(Estado.Fecha_Id) = YEAR(GETDATE())
					AND Estado.Mes_Id = MONTH(GETDATE())
					AND CONVERT(DATE, OrdenEnc.Orden_Fecha_Crea) > DATEADD(month, -3, GETDATE())
					THEN Estado.Estado2
				ELSE 0
			END) AS AlertaInv_Id
		, MAX(CASE WHEN OrdenEnc.Orden_Usuario_Crea = 0 THEN 'x' ELSE CAST(OrdenEnc.Orden_Usuario_Crea AS VARCHAR)END) AS Usuario_Id
		, OrdenDetalle.Orden_Id AS Orden_Id
		, MAX(CAST(ISNULL(CAST(Ingresos.Boleta_Id AS VARCHAR), 'x') AS VARCHAR)) AS Boleta_Id
		, CASE
			WHEN SUM(OrdenDetalle.Detalle_Recibido) = MAX(Ingresos.Facturada)
				THEN 1
			ELSE CASE
					WHEN SUM(OrdenDetalle.Detalle_Recibido) < MAX(Ingresos.Facturada)
						THEN 3
					ELSE 2
				END
		END AS Entrega_Id
		, MAX(CAST(OrdenDetalle.Detalle_Transito AS INT)) AS Transito_Id
		, MAX(OrdenEnc.Orden_Fecha_Entrada_Estimada) AS Fecha_Entrega
		, CONVERT(DATE, OrdenEnc.Orden_Fecha_Crea) AS Fecha_Id
		, YEAR(OrdenEnc.Orden_Fecha_Crea) AS Anio_Id
		, DATEPART(quarter, OrdenEnc.Orden_Fecha_Crea) AS Trimestre_Id
		, MONTH(OrdenEnc.Orden_Fecha_Crea) AS Mes_Id
		, DATEPART(weekday, OrdenEnc.Orden_Fecha_Crea) AS Dias_Id
		, DAY(OrdenEnc.Orden_Fecha_Crea) AS Dia_Id
		, SUM(OrdenDetalle.Detalle_Existencia) AS Compra_Existencia
		, SUM(OrdenDetalle.Detalle_Cantidad_Transito) AS Compra_Transito
		, SUM(OrdenDetalle.Detalle_Cantidad) AS Compra_Cantidad
		, SUM(OrdenDetalle.Detalle_Cancelado) AS Compra_Cancelado
		, SUM(OrdenDetalle.Detalle_Recibido) AS Compra_Recibido
		, SUM(OrdenDetalle.Detalle_Sugerido) AS Compra_Sugerido
		, SUM(OrdenDetalle.Detalle_Bonifica) AS Compra_Bonifica
		, SUM(OrdenDetalle.Detalle_Recibido_Bonifica) AS Compra_Recibido_Bonifica
		, SUM(OrdenDetalle.Detalle_Descuento) AS Compra_Descuento
		, SUM(OrdenDetalle.Detalle_Impuesto) AS Compra_Impuesto
		, SUM(OrdenDetalle.Detalle_Costo_Bruto) / COUNT(OrdenDetalle.Articulo_Id) AS Compra_Costo_Bruto
		, SUM(OrdenDetalle.Detalle_Costo_Neto) / COUNT(OrdenDetalle.Articulo_Id) AS Compra_Costo_Neto
		, MAX(Suc.EmpSuc_Id) AS Emp_Suc_Id
		, MAX(Art.EmpArt_Id) AS EmpArt_Id
	FROM	DL_FARINTER.dbo.DL_Kielsa_Orden_local_Detalle OrdenDetalle --{{ source('DL_FARINTER', 'DL_Kielsa_Orden_Local_Detalle')}}
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Orden_Local_Encabezado OrdenEnc --{{ source('DL_FARINTER', 'DL_Kielsa_Orden_Local_Encabezado')}}
		ON OrdenDetalle.Emp_Id = OrdenEnc.Emp_Id
		AND OrdenDetalle.Suc_Id = OrdenEnc.Suc_Id
		AND OrdenDetalle.Orden_Id = OrdenEnc.Orden_Id
	INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal Suc -- {{ref ('BI_Kielsa_Dim_Sucursal')}}
		ON OrdenDetalle.Emp_Id = Suc.Emp_Id AND OrdenDetalle.Suc_Id = Suc.Sucursal_Id
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Articulo_Info ArtInfo --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo_Info')}}
		ON OrdenDetalle.Emp_Id = ArtInfo.Emp_Id AND OrdenDetalle.Articulo_Id = ArtInfo.Articulo_Id
	INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo Art -- {{ref ('BI_Kielsa_Dim_Articulo')}} 
		ON OrdenDetalle.Emp_Id = Art.Emp_Id AND OrdenDetalle.Articulo_Id = Art.Articulo_Id
	LEFT OUTER JOIN Alertas
		ON OrdenDetalle.Emp_Id = Alertas.Emp_Id AND OrdenDetalle.Articulo_Id = Alertas.Articulo_Id
	LEFT OUTER JOIN DL_FARINTER.[dbo].[DL_TC_ArticuloXMecanica_Kielsa] ArtxMec --{{ ref('DL_TC_ArticuloXMecanica_Kielsa')}}
		ON OrdenDetalle.Emp_Id = ArtxMec.Emp_Id
		AND OrdenDetalle.Articulo_Id = ArtxMec.Articulo_Id
		AND OrdenEnc.Orden_Fecha_Crea BETWEEN ArtxMec.Inicio AND ArtxMec.Final
	LEFT OUTER JOIN AN_FARINTER.[dbo].[AN_Cal_ArticulosEstado_Kielsa] Estado --{{ source('AN_FARINTER', 'AN_Cal_ArticulosEstado_Kielsa')}}
		ON OrdenDetalle.Emp_Id = Estado.Pais_Id
		AND OrdenDetalle.Articulo_Id = Estado.Articulo_Id_Solo
		AND YEAR(OrdenEnc.Orden_Fecha_Crea) = YEAR(Estado.Fecha_Id)
		AND MONTH(OrdenEnc.Orden_Fecha_Crea) = Estado.Mes_Id
	LEFT OUTER JOIN Ingresos Ingresos
		ON OrdenDetalle.Emp_Id = Ingresos.Emp_Id
		AND OrdenDetalle.Suc_Id = Ingresos.Sucursal_Id
		AND OrdenDetalle.Articulo_Id = Ingresos.Articulo_Id
		AND OrdenDetalle.Orden_Id = Ingresos.Orden_Id
		{% if is_incremental() -%} 
		WHERE OrdenEnc.Orden_Fecha_Crea > '{{ last_date }}' 
		{% else -%}
		WHERE OrdenEnc.Orden_Fecha_Crea >= '{{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=360*3)).replace(day=1, month=1, hour=0, minute=0, second=0).strftime('%Y%m%d')  }}'
		{% endif -%}
	GROUP BY Suc.Emp_Id, Suc.Sucursal_Id, OrdenDetalle.Articulo_Id, OrdenDetalle.Orden_Id, OrdenEnc.Orden_Fecha_Crea)
,
ComprasExteriores AS
	(SELECT
		ExtDet.Emp_Id AS Emp_Id
		, ExtDet.Suc_Id AS Sucursal_Id
		, MAX(Suc.Zona_Id) AS Zona_Id
		, MAX(Suc.Departamento_Id) AS Departamento_Id
		, MAX(Suc.Municipio_Id) AS Municipio_Id
		, MAX(Suc.Ciudad_Id) AS Ciudad_Id
		, MAX(Suc.TipoSucursal_id) AS TipoSucursal_Id
		, ExtDet.Articulo_Id AS Articulo_Id
		, MAX(ISNULL(Art.Casa_Id, ArtInfo.Casa_Id)) AS Casa_Id
		, MAX(ISNULL(Art.Marca_Id, ArtInfo.Marca_Id)) AS Marca1_Id
		, MAX(ISNULL(Art.Categoria_Id, ArtInfo.Categoria_Id)) AS CategoriaArt_Id
		, MAX(ISNULL(Art.DeptoArt_Id, ArtInfo.Depto_Id)) AS DeptoArt_Id
		, MAX(ISNULL(Art.SubCategoria1Art_Id, ArtInfo.SubCategoria_Id)) AS SubCategoria1Art_Id
		, MAX(ISNULL(Art.SubCategoria2Art_Id, ArtInfo.SubCategoria2_Id)) AS SubCategoria2Art_Id
		, MAX(ISNULL(Art.SubCategoria3Art_Id, ArtInfo.SubCategoria3_Id)) AS SubCategoria3Art_Id
		, MAX(ISNULL(Art.SubCategoria4Art_Id, ArtInfo.SubCategoria4_Id)) AS SubCategoria4Art_Id
		, MAX(ExtEnc.Prov_Id) AS Proveedor_Id
		, MAX(ISNULL(CASE
						WHEN Alerta.Emp_Id = 2
							THEN Alerta.PV_Alerta_Id - 1
						WHEN Alerta.Emp_Id = 3
							THEN Alerta.PV_Alerta_Id - 3
						WHEN Alerta.Emp_Id = 5
							THEN Alerta.PV_Alerta_Id - 5
						ELSE Alerta.PV_Alerta_Id
					END
					, 0)) AS Cuadro_Id
		, MAX(CAST(ISNULL(CAST(ArtxMec.MecanicaCanje_Id AS VARCHAR), 'x') AS VARCHAR)) AS Mecanica_Id
		, MAX(CASE Art.ABC_Cadena WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 WHEN 'D' THEN 4 WHEN 'E' THEN 5 ELSE
																												0 END) AS ABCCadena_Id
		, MAX(CASE ExtEnc.Orden_Estado WHEN 'GU' THEN 1 WHEN 'AN' THEN 2 WHEN 'RE' THEN 3 WHEN 'AP' THEN 4 ELSE 0 END) AS CompraEstado_Id
		, 2 AS TipoCompra_Id
		, MAX(CASE
				WHEN YEAR(Estado.Fecha_Id) = YEAR(GETDATE())
					AND Estado.Mes_Id = MONTH(GETDATE())
					AND CONVERT(DATE, ExtEnc.Orden_Fecha_Crea) > DATEADD(month, -3, GETDATE())
					THEN Estado.Estado1
				ELSE 0
			END) AS DiasInv_Id
		, MAX(CASE
				WHEN YEAR(Estado.Fecha_Id) = YEAR(GETDATE())
					AND Estado.Mes_Id = MONTH(GETDATE())
					AND CONVERT(DATE, ExtEnc.Orden_Fecha_Crea) > DATEADD(month, -3, GETDATE())
					THEN Estado.Estado2
				ELSE 0
			END) AS AlertaInv_Id
		, MAX(CASE WHEN ExtEnc.Orden_Usuario_Crea = 0 THEN 'x' ELSE CAST(ExtEnc.Orden_Usuario_Crea AS VARCHAR)END) AS Usuario_Id
		, MAX(ExtDet.Orden_Id) AS Orden_Id
		, MAX(CAST(ISNULL(CAST(Ingresos.Boleta_Id AS VARCHAR), 'x') AS VARCHAR)) AS Boleta_Id
		, CASE
			WHEN SUM(ExtDet.Detalle_Recibido) = MAX(Ingresos.Facturada)
				THEN 1
			ELSE CASE
					WHEN SUM(ExtDet.Detalle_Recibido) < MAX(Ingresos.Facturada)
						THEN 3
					ELSE 2
				END
		END AS Entrega_Id
		, MAX(CAST(ExtDet.Detalle_Transito AS INT)) AS Transito_Id
		, MAX(ExtEnc.Orden_Fecha_Entrega) AS Fecha_Entrega
		, MAX(CONVERT(DATE, ExtEnc.Orden_Fecha_Crea)) AS Fecha_Id
		, MAX(YEAR(ExtEnc.Orden_Fecha_Crea)) AS Anio_Id
		, MAX(DATEPART(quarter, ExtEnc.Orden_Fecha_Crea)) AS Trimestre_Id
		, MAX(MONTH(ExtEnc.Orden_Fecha_Crea)) AS Mes_Id
		, MAX(DATEPART(weekday, ExtEnc.Orden_Fecha_Crea)) AS Dias_Id
		, MAX(DAY(ExtEnc.Orden_Fecha_Crea)) AS Dia_Id
		, 0 AS Compra_Existencia
		, 0 AS Compra_Transito
		, SUM(ExtDet.Detalle_Cantidad) AS Compra_Cantidad
		, SUM(ExtDet.Detalle_Cancelado) AS Compra_Cancelado
		, SUM(ExtDet.Detalle_Recibido) AS Compra_Recibido
		, SUM(ExtDet.Detalle_Sugerido) AS Compra_Sugerido
		, SUM(ExtDet.Detalle_Bonifica) AS Compra_Bonifica
		, 0 AS Compra_Recibido_Bonifica
		, SUM(ExtDet.Detalle_Descuento) AS Compra_Descuento
		, SUM(ExtDet.Detalle_Impuesto) AS Compra_Impuesto
		, MAX(ExtDet.Detalle_Costo_Bruto) AS Compra_Costo_Bruto
		, MAX(ExtDet.Detalle_Costo_Neto) AS Compra_Costo_Neto
		, MAX(Suc.EmpSuc_Id) AS EmpSuc_Id
		, MAX(Art.EmpArt_Id) AS EmpArt_Id
	FROM	[DL_FARINTER].dbo.DL_Kielsa_Orden_Exterior_Detalle ExtDet --{{ source('DL_FARINTER', 'DL_Kielsa_Orden_Exterior_Detalle')}}
	INNER JOIN [DL_FARINTER].dbo.DL_Kielsa_Orden_Exterior_Encabezado ExtEnc --{{ source('DL_FARINTER', 'DL_Kielsa_Orden_Exterior_Encabezado')}}
		ON ExtDet.Emp_Id = ExtEnc.Emp_Id AND ExtDet.Suc_Id = ExtEnc.Suc_Id AND ExtDet.Orden_Id = ExtEnc.Orden_Id
	INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal Suc -- {{ref ('BI_Kielsa_Dim_Sucursal')}}')}}
		ON ExtDet.Emp_Id = Suc.Emp_Id AND ExtDet.Suc_Id = Suc.Sucursal_Id
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Articulo_Info ArtInfo --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo_Info')}}
		ON ExtDet.Emp_Id = ArtInfo.Emp_Id AND ExtDet.Articulo_Id = ArtInfo.Articulo_Id
	INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo Art -- {{ref ('BI_Kielsa_Dim_Articulo')}}')}}
		ON ExtDet.Emp_Id = Art.Emp_Id AND ExtDet.Articulo_Id = Art.Articulo_Id
	INNER JOIN AN_FARINTER.[dbo].[AN_Cal_ArticulosEstado_Kielsa] Estado --{{ source('AN_FARINTER', 'AN_Cal_ArticulosEstado_Kielsa')}}
		ON ExtDet.Emp_Id = Estado.Pais_Id
		AND ExtDet.Articulo_Id = Estado.Articulo_Id_Solo
		AND YEAR(ExtEnc.Orden_Fecha_Crea) = YEAR(Estado.Fecha_Id)
		AND MONTH(ExtEnc.Orden_Fecha_Crea) = Estado.Mes_Id
	LEFT OUTER JOIN Alertas Alerta
		ON ExtDet.Emp_Id = Alerta.Emp_Id AND ExtDet.Articulo_Id = Alerta.Articulo_Id
	LEFT OUTER JOIN DL_FARINTER.[dbo].[DL_TC_ArticuloXMecanica_Kielsa] ArtxMec --{{ ref ('DL_TC_ArticuloXMecanica_Kielsa')}}
		ON ExtDet.Emp_Id = ArtxMec.Emp_Id
		AND ExtDet.Articulo_Id = ArtxMec.Articulo_Id
		AND ExtEnc.Orden_Fecha_Crea BETWEEN ArtxMec.Inicio AND ArtxMec.Final
	LEFT OUTER JOIN Ingresos Ingresos
		ON ExtDet.Emp_Id = Ingresos.Emp_Id
		AND ExtDet.Suc_Id = Ingresos.Sucursal_Id
		AND ExtDet.Articulo_Id = Ingresos.Articulo_Id
		AND ExtDet.Orden_Id = Ingresos.Orden_Id
		{% if is_incremental() -%} 
		WHERE ExtEnc.Orden_Fecha_Crea > '{{ last_date }}' 
		{% else -%}
		WHERE ExtEnc.Orden_Fecha_Crea >= '{{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=360*3)).replace(day=1, month=1, hour=0, minute=0, second=0).strftime('%Y%m%d')  }}'
		{% endif -%}
	GROUP BY ExtDet.Emp_Id, ExtDet.Suc_Id, ExtDet.Articulo_Id, ExtDet.Orden_Id, ExtEnc.Orden_Fecha_Crea)
,
comprasGenerales AS (SELECT * FROM ComprasLocales UNION ALL SELECT * FROM ComprasExteriores)
SELECT
	ISNULL(Emp_Id, 0) AS Emp_Id
	, ISNULL(Sucursal_Id, 0) AS Sucursal_Id
	, Zona_Id
	, Departamento_Id
	, Municipio_Id
	, Ciudad_Id
	, TipoSucursal_Id
	, ISNULL(Articulo_Id, '') AS Articulo_Id
	, Casa_Id
	, Marca1_Id
	, CategoriaArt_Id
	, DeptoArt_Id
	, SubCategoria1Art_Id
	, SubCategoria2Art_Id
	, SubCategoria3Art_Id
	, SubCategoria4Art_Id
	, Proveedor_Id
	, Cuadro_Id
	, Mecanica_Id
	, ABCCadena_Id
	, CompraEstado_Id
	, TipoCompra_Id
	, DiasInv_Id
	, AlertaInv_Id
	, Usuario_Id
	, ISNULL(Orden_Id, 0) AS Orden_Id
	, Boleta_Id
	, Entrega_Id
	, Transito_Id
	, Fecha_Entrega
	, ISNULL(Fecha_Id, '19000101') AS Fecha_Id
	, Anio_Id
	, Trimestre_Id
	, Mes_Id
	, Dias_Id
	, Dia_Id
	, CAST(Compra_Existencia AS INT) Compra_Existencia
	, CAST(Compra_Transito AS INT) Compra_Transito
	, CAST(Compra_Cantidad AS INT) Compra_Cantidad
	, CAST(Compra_Cancelado AS INT) Compra_Cancelado
	, CAST(Compra_Recibido AS INT) Compra_Recibido
	, Compra_Sugerido
	, CAST(Compra_Bonifica AS INT) Compra_Bonifica
	, CAST(Compra_Recibido_Bonifica AS INT) Compra_Recibido_Bonifica
	, Compra_Descuento
	, Compra_Impuesto
	, Compra_Costo_Bruto
	, Compra_Costo_Neto
	, Emp_Suc_Id
	, EmpArt_Id
	, GETDATE() AS Fecha_Actualizado
FROM	comprasGenerales
--WHERE TipoCompra_Id = 1

/*SELECT * INTO #ComprasPrueba
FROM comprasGenerales

SELECT * FROM #ComprasPrueba*/