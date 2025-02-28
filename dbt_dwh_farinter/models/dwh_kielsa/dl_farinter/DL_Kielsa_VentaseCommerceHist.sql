
{%- set unique_key_list = [] -%}
{{ 
    config(
		tags=["periodo/diario"],
		materialized="view",
) }}

-- Solo editable en DBT DAGSTER

SELECT A.Emp_Id AS Pais_Id,
	CASE
		WHEN B.Factura_Id IS NULL THEN 'x'
		ELSE '1-' + CONVERT(NVARCHAR, B.Suc_Id) + '-' + CONVERT(NVARCHAR, B.TipoDoc_id) + '-' + CONVERT(NVARCHAR, B.Caja_Id) + '-' + CONVERT(NVARCHAR, B.Factura_Id)
	END AS Factura_Id,
	A.Orden_Id,
	A.OrdenExp_Id,
	isnull(B.Orden_Usuario_Registro, 'x') AS Usuario_Registro,
	A.CanalVenta_Id,
	A.conFactura_Id,
	isnull(C.Origen_Id, 1) AS Origen_Id,
	isnull(D.FormaPago_Id, 1) AS FormaPago_Id,
	isnull(E.TipoPago_Id, 1) AS TipoPago_Id,
	isnull('1-' + CONVERT(NVARCHAR, B.Suc_Id), 'x') AS Sucursal_Id,
	'1-' + CONVERT(nvarchar, A.Articulo_Id) AS Articulo_Id,
	CONVERT(date, A.fecha) AS Fecha_Id,
	A.Cantidad AS Cantidad,
	A.Cantidad *(A.Precio_Unitario - isnull(A.Descuento, 0)) AS Venta_Neta,
	isnull(A.Descuento, 0) AS Descuento,
	A.Cantidad * A.Costo_Unitario AS Costo,
	A.Cantidad *(
		A.Precio_Unitario - isnull(A.Descuento, 0) - A.Costo_Unitario
	) AS Utilidad
FROM(
		SELECT 1 AS Emp_Id,
			1 AS CC_Id,
			dateadd(
				HOUR,
				-6,
				try_parse(left(A.created_at, 19) AS datetime)
			) AS fecha,
			A._id AS Orden_Id,
			CASE
				WHEN A.type_order = 'EX' THEN 2
				WHEN A.type_order = 'PU' THEN 3
				ELSE 0
			END AS CanalVenta_Id,
			A.origin AS SubCanalVenta_Id,
			CONVERT(int, isnull(A.Order_number, 0)) AS OrdenExp_Id,
			A.type_payment AS FormaPago_Id,
			A.type_pay_card AS TipoPago_Id,
			CASE
				WHEN len(A.bill_number_dgi) = 19 THEN 1
				ELSE 0
			END AS conFactura_Id,
			A.[user_id] AS Usuario_Id,
			isnull(A.bill_number_dgi, 'x') AS Factura_Id,
			B.other_id_product AS Articulo_Id,
			B.amount AS Cantidad,
			B.normal_price AS Precio_Unitario,
			(B.normal_price - B.price_product) AS Descuento,
			(B.descuento_tercera_edad / 100.0) * B.price_product AS Descuento_Tercera,
			(B.porcentaje_isv / 100.0) * B.price_product AS Impuesto,
			B.costo_producto AS Costo_Unitario
		FROM DL_FARINTER.mdb_ecommerce_hn.orders AS A -- {{ source('DL_FARINTER_mdb_ecommerce_hn', 'orders') }}
			INNER JOIN DL_FARINTER.mdb_ecommerce_hn.orders__shopping_cart AS B ON A._dlt_id = B._dlt_parent_id -- {{ source('DL_FARINTER_mdb_ecommerce_hn', 'orders__shopping_cart') }}
	) AS A
	LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Exp_Factura_Express AS B -- {{ ref('DL_Kielsa_Exp_Factura_Express') }}
	ON A.Emp_Id = B.Emp_Id
	AND A.CC_Id = B.CC_Id
	AND A.OrdenExp_Id = B.Orden_Id
	LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_eCommerceOrigen AS C -- {{ source('DL_FARINTER', 'DL_Kielsa_eCommerceOrigen') }}
	ON A.SubCanalVenta_Id = C.Origen_Nombre
	LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_eCommerceFormaPago AS D -- {{ source('DL_FARINTER', 'DL_Kielsa_eCommerceFormaPago') }}
	ON A.FormaPago_Id = D.FormaPago_Nombre
	LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_eCommerceTipoPago AS E  -- {{ source('DL_FARINTER','DL_Kielsa_eCommerceTipoPago') }}
	ON A.TipoPago_Id = E.TipoPago_Nombre
WHERE A.Articulo_Id IS NOT NULL
	AND A.fecha < CONVERT(date, getdate())