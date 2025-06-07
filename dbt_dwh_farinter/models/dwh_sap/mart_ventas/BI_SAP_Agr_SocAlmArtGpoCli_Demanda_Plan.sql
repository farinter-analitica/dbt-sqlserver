{% set unique_key_list = ["Material_Id","Fecha_Id","Centro_Almacen_Id","Sociedad_Id","Gpo_Cliente"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["automation/periodo_mensual_inicio", "periodo_unico/si", "automation_only"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental()) }}",
        ]
	) 
}}
{% set v_semanas_muestra = 12 %}
{% set v_dias_muestra = v_semanas_muestra * 7 %}
{% set v_fecha_inicio = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=v_dias_muestra)).strftime('%Y%m%d') %}
{% set v_fecha_fin = modules.datetime.datetime.now().strftime('%Y%m%d')  %}
{% set v_anio_mes_inicio =  v_fecha_inicio[:6]  %}

SELECT --top 100
		ISNULL(S.Sociedad_Id, 'OTROS') as Sociedad_Id
		, ISNULL(E1.Almacen_Id_Mapeado, 'X') AS Centro_Almacen_Id
		, ISNULL(C.Material_Id, 'X') AS Material_Id
		, ISNULL(A.GrupoClientes_Nombre, 'OTROS') AS Gpo_Cliente
		, CONVERT(INT, A.Anio_Id) AS Anio_Id
		, CONVERT(INT, A.Mes_Id) AS Mes_Id
        , ISNULL(DATEFROMPARTS(A.Anio_Id, A.Mes_Id, 1), '19000101') AS Fecha_Id
		, COALESCE(SUM(CASE WHEN A.TipoDocumento_Id IN ( 'M', 'P' ) THEN A.Cantidad_SKU ELSE 0 END), 0) AS Demanda_Positiva
            --Deberian ser pedidos de los clientes, sin embargo, los pedidos es lo mismo que la factura practicamente en Farinter
			--incluyendo facturas M, y notas de debito P
		, COALESCE(SUM(CASE WHEN A.TipoDocumento_Id IN ( 'O') THEN A.Cantidad_SKU ELSE 0 END), 0) AS Demanda_Negativa
            --Deberian ser solo devoluciones de facturas recientes de los clientes, sin embargo, incluyendo notas de credito O
        , COALESCE(SUM(CASE WHEN A.TipoDocumento_Id IN ( 'O') THEN A.Cantidad_SKU ELSE 0 END), 0)  AS Vencidos_Entrada
            --Duplicado mientras se encuentra la logica para separarlo
		, MAX(B.Gpo_Obs_Nombre_Corto) AS Gpo_Obs_Nombre_Corto
		--, S.Sociedad_Id
		, B.Gpo_Obs_Id
        , B.Gpo_Plan_Id COLLATE DATABASE_DEFAULT AS Gpo_Plan
		, MAX(C.Sector_Id) AS Sector
		, MAX(C.Articulo_Nombre) AS Material_Nombre
		, C.Articulo_Id
		, SUM(A.Valor_Bruto) AS Valor_Bruto
		, SUM(A.Valor_Neto) AS Valor_Neto
		, SUM(A.Valor_Costo) AS Valor_Costo
		, SUM(A.Valor_Utilidad) AS Valor_Utilidad
	FROM
		(SELECT
			V.Anio_Calendario AS Anio_Id
			, V.Mes_Calendario Mes_Id
            , V.TipoDocumento_Id
			, V.Sociedad_Id
			, V.Articulo_Id
			, V.Almacen_Id
			, V.Centro_Id
			, COALESCE(CASE WHEN LEFT(V.Cliente_Id,5) = '00003' THEN 'INST' ELSE C.GrupoClientes_Nombre END, 'OTROS') AS GrupoClientes_Nombre
			, (CONVERT(DECIMAL, V.Cantidad_SKU)) AS Cantidad_SKU
			, (CONVERT(DECIMAL, V.Venta)) AS Valor_Bruto
			, (CONVERT(DECIMAL, V.Venta - V.Descuento)) AS Valor_Neto
			, (CONVERT(DECIMAL, V.Costo)) AS Valor_Costo
			, (CONVERT(DECIMAL, V.Venta - V.Descuento - V.Costo)) AS Valor_Utilidad
		FROM	dbo.BI_SAP_Mixto_Facturas V -- {{ ref('BI_SAP_Mixto_Facturas') }}
		LEFT JOIN DL_FARINTER.dbo.DL_Edit_GrupoClientes_SAP C -- {{ ref('DL_Edit_GrupoClientes_SAP') }}
			ON V.Cliente_Id = C.Cliente_Id
		WHERE V.Anio_Calendario >= YEAR(EOMONTH(DATEADD(YEAR, -5, GETDATE()), -1))
			AND V.Fecha_Id > EOMONTH(DATEADD(YEAR, -5, GETDATE()), -1)
			AND V.Fecha_Id <= EOMONTH(GETDATE(), -1)
			AND (V.Indicador_Anulado = '' OR V.Indicador_Anulado IS NULL) --Solo lo que no fue anulado
			AND V.TipoDocumento_Id IN ( 'M',  'O', 'P' ) --Solo las categorias tipo en uso
		) A
	INNER JOIN dbo.BI_Dim_Articulo_SAP C -- {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }}
		ON A.Articulo_Id = C.Articulo_Id
	INNER JOIN dbo.BI_Dim_Sociedad_SAP S -- {{ source('BI_FARINTER', 'BI_Dim_Sociedad_SAP') }}
		ON A.Sociedad_Id = S.Sociedad_Id
	INNER JOIN DL_FARINTER.dbo.DL_Planning_ParamSocMat B -- {{ ref('DL_Planning_ParamSocMat') }}
		ON S.Sociedad_Id = B.Sociedad_Id AND C.Articulo_Id = B.Articulo_Id
	INNER JOIN dbo.BI_Dim_Centro_SAP D -- {{ source('BI_FARINTER', 'BI_Dim_Centro_SAP') }}
		ON A.Centro_Id = D.Centro_Id
	INNER JOIN DL_FARINTER.[dbo].[DL_Edit_AlmacenFP_SAP] E1 -- {{ ref('DL_Edit_AlmacenFP_SAP') }}
		ON A.Almacen_Id = E1.Almacen_Id
	WHERE E1.Planificado = 'S'	--Solo lo planificado
	--and B.Gpo_Obs_Id = 'COINS' --and A.Sociedad_Id = '1200' 
        AND A.Sociedad_Id IN ( '1200', '1300', '1301', '1700', '2500' )
    --AND  A.Sociedad_Id = '1200' and B.Gpo_Obs_Id = 'FIC' --and B.Gpo_Obs_Id = 'COINS'  --AND A.Sociedad_Id = '1200'   --
    GROUP BY S.Sociedad_Id
				, C.Articulo_Id
				, E1.Almacen_Id_Mapeado
				, A.Anio_Id
				, A.Mes_Id
                , C.Material_Id
                , C.Sector_Id
                , B.Gpo_Plan_Id
				, A.GrupoClientes_Nombre
                , B.Gpo_Obs_Id


--AND MARA.Articulo_Id = '000000000010029708'


/*
SELECT TOP 100 *		FROM	dbo.BI_SAP_Mixto_Facturas V
WHERE V.TipoDocumento_Id IN ( 'M', 'O' )
AND Cantidad_SKU<0



SELECT distinct v.TipoDocumento_Id	
FROM	dbo.BI_SAP_Mixto_Facturas V
WHERE V.TipoDocumento_Id IN ( 'M', 'O' )
AND Cantidad_SKU<0

SELECT TOP 100 *
FROM	dbo.BI_SAP_Dim_Facturas_Actual V
WHERE V.TipoDocumento_Id IN ( 'M', 'N', 'O','P' )
AND Cantidad_SKU<0

*/
/*
		
	A Inquiry

B Quotation

C Order

D Item proposal

E Scheduling agreement

F Scheduling agreement with external service agent

G Contract

H Returns

I Order w/o charge

J Delivery

K Credit memo request

L Debit memo request

M Invoice

N Invoice cancellation

O Credit memo

P Debit memo

Q WMS transfer order

R Goods movement

S Credit memo cancellation

T Returns delivery for order

U Pro forma invoice

V Purchase order

W Independent reqts plan

X Handling unit

0 Master contract

1 Sales activities (CAS)

2 External transaction

3 Invoice list

4 Credit memo list

5 Intercompany invoice

6 Intercompany credit memo

7 Delivery/shipping notification

8 Shipment

a Shipment costs

e Allocation table

g Rough Goods Receipt (only IS-Retail)

h Cancel goods issue

i Goods receipt

j JIT call

r TD Shipment (IS-Oil Only)

s Loading Confirmation, Reposting (IS-Oil Only)

t Gain/Loss (IS-Oil Only)

u Placing Back in Stock (IS-Oil Only)

v Two-Step Goods Receipt (IS-Oil Only)

w Reservation (IS-Oil Only)

x Loading Confirmation, Goods Receipt (IS-Oil Only)
*/