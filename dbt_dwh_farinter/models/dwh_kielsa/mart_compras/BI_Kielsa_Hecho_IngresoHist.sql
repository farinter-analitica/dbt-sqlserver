{% set unique_key_list = ["Emp_Id","Sucursal_Id","Articulo_Id", "Boleta_Id", "Orden_Id", "Fecha_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado'] ) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Id'] ) }}",
        ]
	) 
}}
{% if is_incremental() %}
	{% set v_last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{% else %}
	{% set v_last_date = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=365*4)).replace(day=1,month=1).strftime('%Y%m%d') %}
{% endif %}
{% set v_anio_mes_inicio =  v_last_date[:6]  %}
--20230927: Axell Padilla > Vista de las posiciones de facturas para crear el hecho.

WITH Ingresos AS (
SELECT
			A.Emp_Id AS Emp_Id
			, Suc.Sucursal_Id as Sucursal_Id
			, MAX(Suc.Zona_Id) AS Zona_Id
			, MAX(Suc.Departamento_Id) AS Departamento_Id
			, MAX(Suc.Municipio_Id) AS Municipio_Id
			, MAX(Suc.Ciudad_Id) AS Ciudad_Id
			, MAX(Suc.TipoSucursal_Id) AS TipoSucursal_Id
			, B.Boleta_Id AS Boleta_Id
			, B.Orden_Id AS Orden_Id
			, MAX(CASE B.Boleta_Estado
				WHEN 'AP' THEN 4
				WHEN 'CA' THEN 5
				WHEN 'GU' THEN 1
				ELSE 0
			END) AS Compra_Estado
			, 1 AS TipoCompra_Id
			, MAX(B.Usuario_Id) AS Usuario_Id
			, MAX(CAST(B.Prov_distribuye as INT)) AS Prov_Distribuye
			, Art.Articulo_Id AS Articulo_Id
			, MAX(Art.Casa_Id) AS Casa_Id
			, MAX(Art.Marca_Id) AS Marca1_Id
			, MAX(Art.Categoria_Id) AS CategoriaArt_Id
			, MAX(Art.DeptoArt_Id) AS DeptoArt_Id
			, MAX(Art.SubCategoria1Art_Id) AS SubCategoria1Art_Id
			, MAX(Art.SubCategoria2Art_Id) AS SubCategoria2Art_Id
			, MAX(Art.SubCategoria3Art_Id) AS SubCategoria3Art_Id
			, MAX(Art.SubCategoria4Art_Id) AS SubCategoria4Art_Id
			, MAX(B.Prov_Id) AS Proveedor_Id
			, MAX(ISNULL(E.MecanicaCanje_Id, 'x')) AS Mecanica_Id
			, MAX(F.Alerta_Id_Recomendacion) AS Cuadro_Id
			, MAX(ISNULL(ARTEST.Estado2,0)) AS AlertaInv_Id
			, MAX(ISNULL(ARTEST.Estado1,0)) AS DiasInv_Id
			, CONVERT(DATE, B.Boleta_FechaSola) AS Fecha_Id
			, B.AnioMes_Id AnioMes_Id
			, YEAR(B.Boleta_FechaSola) AS Anio_Id
			, DATEPART(quarter, B.Boleta_FechaSola) AS Trimestre_Id
			, MONTH(B.Boleta_FechaSola) AS Mes_Id
			, DATEPART(weekday, B.Boleta_FechaSola) AS Dias_Id
			, DAY(B.Boleta_FechaSola) AS Dia_Id
			, CONVERT(INT,SUM(A.Detalle_Cantidad_Pedida)) AS Ingreso_Pedido
			, CONVERT(INT,SUM(A.Detalle_Cantidad_Entrada)) AS Ingreso_Entrada
			, CONVERT(INT,SUM(A.Detalle_Cantidad_Facturada)) AS Ingreso_Facturada
			, CONVERT(INT,SUM(A.Detalle_Cantidad_Scaneo)) AS Ingreso_Scaneada
			, CONVERT(INT,SUM(A.Detalle_Bonificacion)) AS Ingreso_Bonificacion
			, CONVERT(DECIMAL(10, 2),AVG(A.Detalle_Costo_Bruto),2) AS Ingreso_Costo_Bruto
			, CONVERT(DECIMAL(10, 2),AVG(A.Detalle_Costo_Bruto_Orden),2) AS Compra_Costo_Bruto
			, CONVERT(DECIMAL(10, 2),AVG(A.Detalle_Descuento),2) AS Ingreso_Descuento
			, CONVERT(DECIMAL(10, 2),AVG(A.Detalle_Descuento_Orden),2) AS Compra_Descuento
			, CONVERT(DECIMAL(10, 3),AVG(A.Detalle_Impuesto),2) AS Ingreso_Impuesto
			, CONVERT(DECIMAL(10, 2),AVG(A.Detalle_Impuesto_Orden),2) AS Compra_Impuesto
			, MAX(Art.EmpArt_Id) AS EmpArt_Id
			, MAX(Suc.EmpSuc_Id)  AS EmpSuc_Id
		FROM	DL_FARINTER.[dbo].[DL_Kielsa_BoletaLocal_Detalle] A --{{ source('DL_FARINTER', 'DL_Kielsa_BoletaLocal_Detalle')}} A
		INNER JOIN DL_FARINTER.[dbo].[DL_Kielsa_BoletaLocal_Encabezado] B --{{ source('DL_FARINTER', 'DL_Kielsa_BoletaLocal_Encabezado')}} B
			ON A.Emp_Id = B.Emp_Id AND A.Suc_Id = B.Suc_Id AND A.Bodega_Id = B.Bodega_Id AND A.Boleta_Id = B.Boleta_Id AND A.AnioMes_Id = B.AnioMes_Id
		INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal Suc -- {{ref ('BI_Kielsa_Dim_Sucursal')}} Suc
			ON A.Emp_Id = Suc.Emp_Id AND A.Suc_Id = Suc.Sucursal_Id
		INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo Art -- {{ref ('BI_Kielsa_Dim_Articulo')}} Art
			ON A.Emp_Id = Art.Emp_Id AND A.Articulo_Id = Art.Articulo_Id
		LEFT OUTER JOIN DL_FARINTER.[dbo].[DL_TC_ArticuloXMecanica_Kielsa] E --{{ source('DL_FARINTER', 'DL_TC_ArticuloXMecanica_Kielsa')}} E
			ON A.Emp_Id = CONVERT(INT, LEFT(E.MecanicaCanje_Id, 1))
			AND A.Articulo_Id = E.Articulo_Id 
			AND B.Boleta_Fecha BETWEEN E.Inicio AND E.Final
		LEFT OUTER JOIN [DL_FARINTER].[dbo].[DL_Kielsa_Articulo_Calc] F --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo_Calc')}} F
			ON A.Articulo_Id = F.Articulo_Id AND A.Emp_Id = F.Emp_Id 
		LEFT OUTER JOIN AN_FARINTER.[dbo].[AN_Cal_ArticulosEstado_Kielsa] ARTEST --{{ source('AN_FARINTER', 'AN_Cal_ArticulosEstado_Kielsa')}} ARTEST
			ON A.Emp_Id = ARTEST.Pais_Id AND A.Articulo_Id = ARTEST.Articulo_Id_Solo AND A.AnioMes_Id = ARTEST.AnioMes_Id
		WHERE A.Indicador_Borrado = 0 AND B.Indicador_Borrado = 0 
		{% if is_incremental() %}
		AND B.AnioMes_Id >= '{{ v_anio_mes_inicio }}' AND B.Boleta_FechaSola >= '{{ v_last_date }}'
		{% endif %}
		GROUP BY 
				A.Emp_Id
		 		, Suc.Sucursal_Id
				, Art.Articulo_Id
				, B.Boleta_Id
				, B.Orden_Id
				, B.AnioMes_Id
				, B.Boleta_FechaSola
)

SELECT 			ISNULL(Emp_Id, 0) AS Emp_Id
			,ISNULL(Sucursal_Id, 0) AS Sucursal_Id
			,Zona_Id
			,Departamento_Id
			,Municipio_Id
			,Ciudad_Id
			,TipoSucursal_Id
			,ISNULL(Boleta_Id, 0) AS Boleta_Id
			,ISNULL(Orden_Id, 0) AS Orden_Id
			,Compra_Estado
			, TipoCompra_Id
			, Usuario_Id
			, Prov_Distribuye
			, ISNULL(Articulo_Id, 'X') AS Articulo_Id
			, Casa_Id
			, Marca1_Id
			, CategoriaArt_Id
			, DeptoArt_Id
			, SubCategoria1Art_Id
			, SubCategoria2Art_Id
			, SubCategoria3Art_Id
			, SubCategoria4Art_Id
			, Proveedor_Id
			, Mecanica_Id
			, Cuadro_Id
			, AlertaInv_Id
			, DiasInv_Id
			, ISNULL(Fecha_Id, '19000101') AS Fecha_Id
			, Anio_Id
			, Trimestre_Id
			, Mes_Id
			, Dias_Id
			, Dia_Id
			, Ingreso_Pedido
			, Ingreso_Entrada
			, Ingreso_Facturada
			, Ingreso_Scaneada
			, Ingreso_Bonificacion
			, Ingreso_Costo_Bruto
			, Compra_Costo_Bruto
			, Ingreso_Descuento
			, Compra_Descuento
			, Ingreso_Impuesto
			, Compra_Impuesto
			, EmpArt_Id
			, EmpSuc_Id
			, GETDATE() AS Fecha_Actualizado

FROM Ingresos
--WHERE Fecha_Id >= '20240901' 


		--SELECT TOP 100 * FROM DL_FARINTER.[dbo].[DL_Kielsa_BoletaLocal_Encabezado] B --{{ source('DL_FARINTER', 'DL_Kielsa_BoletaLocal_Encabezado')}} B

--WHERE Articulo_Id = '1110000819'  AND Boleta_Id= '277' AND Orden_Id ='311' AND Usuario_Id='2220'

--SELECT * FROM #TempIngresos

 /*select top 10 A.AnioMes_Id, ARTEST.AnioMes_Id, A.Articulo_Id, ARTEST.Articulo_Id_Solo FROM	DL_FARINTER.[dbo].[DL_Kielsa_BoletaLocal_Detalle] A
 LEFT OUTER JOIN AN_FARINTER.[dbo].[AN_Cal_ArticulosEstado_Kielsa] ARTEST
			ON A.Emp_Id = ARTEST.Pais_Id AND A.Articulo_Id = ARTEST.Articulo_Id_Solo AND A.AnioMes_Id = ARTEST.AnioMes_Id
			WHERE ARTEST.AnioMes_Id IS NOT NULL*/
