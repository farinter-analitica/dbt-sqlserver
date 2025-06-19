{% set unique_key_list = ["Emp_Id","Sucursal_Id","ArticuloPadre_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      	"{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

WITH
DatosBase AS
	(SELECT
		AxB.Emp_Id
		, AxB.Sucursal_Id
		, AxB.ArticuloPadre_Id
		, AxB.Existencia
		, AxC.Transito
		, Venta_180.Cantidad AS Venta_180
		, (Venta_180.Cantidad / 180.0) AS Promedio_Venta_Diario
		, Suc.Marca
		, Suc.Ciudad_Id
		, Suc.Departamento_Id
		, Suc.Zona_Id
		, Suc.HashStr_SucEmp
		, Art.HashStr_ArtEmp
	FROM
		(SELECT --TOP 100
			A.Emp_Id
			, A.Suc_Id AS Sucursal_Id
			, B.Articulo_Codigo_Padre AS ArticuloPadre_Id
			, SUM(CASE
					WHEN B.Indicador_PadreHijo = 'H'
						THEN (A.AxB_Existencia * B.Factor_Numerador) / ISNULL(B.Factor_Denominador, 1)
					ELSE A.AxB_Existencia
				END) AS Existencia
		FROM	{{ source('DL_FARINTER', 'DL_Kielsa_Articulo_x_Bodega') }} A
		LEFT JOIN {{ref('BI_Kielsa_Dim_Articulo')}} B
			ON A.Articulo_Id = B.Articulo_Id AND A.Emp_Id = B.Emp_Id
		WHERE A.Bodega_Id = 1
		GROUP BY A.Emp_Id, A.Suc_Id, B.Articulo_Codigo_Padre) AxB
	LEFT JOIN
		(SELECT
			A.Emp_Id
			, A.Suc_Id AS Sucursal_Id
			, B.Articulo_Codigo_Padre AS ArticuloPadre_Id
			, SUM(CASE
					WHEN B.Indicador_PadreHijo = 'H'
						THEN (A.AxC_Transito * B.Factor_Numerador) / ISNULL(B.Factor_Denominador, 1)
					ELSE A.AxC_Transito
				END) AS Transito
		FROM {{ source('DL_FARINTER', 'DL_Kielsa_Articulo_x_Compra')}} A
		LEFT JOIN {{ref('BI_Kielsa_Dim_Articulo')}} B
			ON A.Emp_Id = B.Emp_Id AND A.Articulo_Id = B.Articulo_Id
		WHERE A.Bodega_Id = 1 AND A.AxC_Transito <> 0
		GROUP BY A.Emp_Id, A.Suc_Id, B.Articulo_Codigo_Padre) AxC
		ON AxC.Sucursal_Id = AxB.Sucursal_Id
		AND AxC.ArticuloPadre_Id = AxB.ArticuloPadre_Id
		AND AxC.Emp_Id = AxB.Emp_Id
	LEFT JOIN {{ref('BI_Kielsa_Dim_Articulo')}} Art
		ON AxB.ArticuloPadre_Id = Art.Articulo_Id AND AxB.Emp_Id = Art.Emp_Id
	LEFT JOIN
		(SELECT
			Emp_Id, Suc_Id, Articulo_Id, SUM(Cantidad_Padre) AS Cantidad
		FROM {{ source('DL_FARINTER', 'DL_Kielsa_FacturasPosiciones')}}	
		WHERE Factura_Fecha BETWEEN DATEADD(day, -180, GETDATE()) AND DATEADD(day, -1, GETDATE())
			AND AnioMes_Id >= (YEAR(DATEADD(day, -180, GETDATE())) * 100) + MONTH(DATEADD(day, -180, GETDATE()))
		GROUP BY Emp_Id, Suc_Id, Articulo_Id) Venta_180
		ON AxB.Emp_Id = Venta_180.Emp_Id
		AND AxB.Sucursal_Id = Venta_180.Suc_Id
		AND AxB.ArticuloPadre_Id = Venta_180.Articulo_Id
	LEFT JOIN {{ref('BI_Kielsa_Dim_Sucursal')}} Suc
		ON AxB.Sucursal_Id = Suc.Sucursal_Id AND AxB.Emp_Id = Suc.Emp_Id)
,
Existencias_Generales AS
	(SELECT
		*
		, (Existencia / NULLIF(Promedio_Venta_Diario,0)) AS Cobertura_Sucursal_Dias
		, SUM(Existencia) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id) AS Existencia_Cadena
		, SUM(Existencia) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id, Marca) AS Existencia_Marca
		, SUM(Existencia) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id, Ciudad_Id) AS Existencia_Ciudad
		, SUM(Existencia) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id, Departamento_Id) AS Existencia_Departamento
		, SUM(Existencia) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id, Zona_Id) AS Existencia_Zona
		, SUM(Promedio_Venta_Diario) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id) AS Promedio_Venta_Diario_Cadena
		, SUM(Promedio_Venta_Diario) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id, Marca) AS Promedio_Venta_Diario_Marca
		, SUM(Promedio_Venta_Diario) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id, Ciudad_Id) AS Promedio_Venta_Diario_Ciudad
		, SUM(Promedio_Venta_Diario) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id, Departamento_Id) AS Promedio_Venta_Diario_Departamento
		, SUM(Promedio_Venta_Diario) OVER (PARTITION BY Emp_Id, ArticuloPadre_Id, Zona_Id) AS Promedio_Venta_Diario_Zona
	FROM	DatosBase
	WHERE (Existencia > 0 OR Transito > 0))
,
Coberturas_Dias AS
	(SELECT
		*
		, (Existencia_cadena / NULLIF(Promedio_Venta_Diario_Cadena, 0)) AS Cobertura_Cadena_Dias
		, (Existencia_Marca / NULLIF(Promedio_Venta_Diario_Marca, 0)) AS Cobertura_Marca_Dias
		, (Existencia_Ciudad / NULLIF(Promedio_Venta_Diario_Ciudad, 0)) AS Cobertura_Ciudad_Dias
		, (Existencia_Departamento / NULLIF(Promedio_Venta_Diario_Departamento, 0)) AS Cobertura_Departamento_Dias
		, (Existencia_Zona / NULLIF(Promedio_Venta_Diario_Zona, 0)) AS Cobertura_Zona_Dias
	FROM	Existencias_Generales)
,
Coberturas_Id AS
	(SELECT
		*
		, CASE
			WHEN Cobertura_Cadena_Dias = 0
				THEN 1
			WHEN Cobertura_Cadena_Dias > 0 AND	Cobertura_Cadena_Dias <= 30
				THEN 2
			WHEN Cobertura_Cadena_Dias > 30 AND Cobertura_Cadena_Dias <= 45
				THEN 3
			WHEN Cobertura_Cadena_Dias > 45 AND Cobertura_Cadena_Dias <= 60
				THEN 4
			WHEN Cobertura_Cadena_Dias > 60 AND Cobertura_Cadena_Dias <= 90
				THEN 5
			WHEN Cobertura_Cadena_Dias > 90 AND Cobertura_Cadena_Dias <= 120
				THEN 6
			WHEN Cobertura_Cadena_Dias > 120 AND Cobertura_Cadena_Dias <= 180
				THEN 7
			WHEN Cobertura_Cadena_Dias > 180
				THEN 8
			WHEN Cobertura_Cadena_Dias IS NULL
				THEN 99
			ELSE 0
		END AS Cobertura_Cadena_Dias_Id
		, CASE
			WHEN Cobertura_Marca_Dias = 0
				THEN 1
			WHEN Cobertura_Marca_Dias > 0 AND	Cobertura_Marca_Dias <= 30
				THEN 2
			WHEN Cobertura_Marca_Dias > 30 AND	Cobertura_Marca_Dias <= 45
				THEN 3
			WHEN Cobertura_Marca_Dias > 45 AND	Cobertura_Marca_Dias <= 60
				THEN 4
			WHEN Cobertura_Marca_Dias > 60 AND	Cobertura_Marca_Dias <= 90
				THEN 5
			WHEN Cobertura_Marca_Dias > 90 AND	Cobertura_Marca_Dias <= 120
				THEN 6
			WHEN Cobertura_Marca_Dias > 120 AND Cobertura_Marca_Dias <= 180
				THEN 7
			WHEN Cobertura_Marca_Dias > 180
				THEN 8
			WHEN Cobertura_Marca_Dias IS NULL
				THEN 99
			ELSE 0
		END AS Cobertura_Marca_Dias_Id
		, CASE
			WHEN Cobertura_Zona_Dias = 0
				THEN 1
			WHEN Cobertura_Zona_Dias > 0 AND Cobertura_Zona_Dias <= 30
				THEN 2
			WHEN Cobertura_Zona_Dias > 30 AND	Cobertura_Zona_Dias <= 45
				THEN 3
			WHEN Cobertura_Zona_Dias > 45 AND	Cobertura_Zona_Dias <= 60
				THEN 4
			WHEN Cobertura_Zona_Dias > 60 AND	Cobertura_Zona_Dias <= 90
				THEN 5
			WHEN Cobertura_Zona_Dias > 90 AND	Cobertura_Zona_Dias <= 120
				THEN 6
			WHEN Cobertura_Zona_Dias > 120 AND	Cobertura_Zona_Dias <= 180
				THEN 7
			WHEN Cobertura_Zona_Dias > 180
				THEN 8
			WHEN Cobertura_Zona_Dias IS NULL
				THEN 99
			ELSE 0
		END AS Cobertura_Zona_Dias_Id
		, CASE
			WHEN Cobertura_Departamento_Dias = 0
				THEN 1
			WHEN Cobertura_Departamento_Dias > 0 AND Cobertura_Departamento_Dias <= 30
				THEN 2
			WHEN Cobertura_Departamento_Dias > 30 AND	Cobertura_Departamento_Dias <= 45
				THEN 3
			WHEN Cobertura_Departamento_Dias > 45 AND	Cobertura_Departamento_Dias <= 60
				THEN 4
			WHEN Cobertura_Departamento_Dias > 60 AND	Cobertura_Departamento_Dias <= 90
				THEN 5
			WHEN Cobertura_Departamento_Dias > 90 AND	Cobertura_Departamento_Dias <= 120
				THEN 6
			WHEN Cobertura_Departamento_Dias > 120 AND	Cobertura_Departamento_Dias <= 180
				THEN 7
			WHEN Cobertura_Departamento_Dias > 180
				THEN 8
			WHEN Cobertura_Departamento_Dias IS NULL
				THEN 99
			ELSE 0
		END AS Cobertura_Departamento_Dias_Id
		, CASE
			WHEN Cobertura_Ciudad_Dias = 0
				THEN 1
			WHEN Cobertura_Ciudad_Dias > 0 AND	Cobertura_Ciudad_Dias <= 30
				THEN 2
			WHEN Cobertura_Ciudad_Dias > 30 AND Cobertura_Ciudad_Dias <= 45
				THEN 3
			WHEN Cobertura_Ciudad_Dias > 45 AND Cobertura_Ciudad_Dias <= 60
				THEN 4
			WHEN Cobertura_Ciudad_Dias > 60 AND Cobertura_Ciudad_Dias <= 90
				THEN 5
			WHEN Cobertura_Ciudad_Dias > 90 AND Cobertura_Ciudad_Dias <= 120
				THEN 6
			WHEN Cobertura_Ciudad_Dias > 120 AND Cobertura_Ciudad_Dias <= 180
				THEN 7
			WHEN Cobertura_Ciudad_Dias > 180
				THEN 8
			WHEN Cobertura_Ciudad_Dias IS NULL
				THEN 99
			ELSE 0
		END AS Cobertura_Ciudad_Dias_Id
		, CASE
			WHEN Cobertura_Sucursal_Dias = 0
				THEN 1
			WHEN Cobertura_Sucursal_Dias > 0 AND Cobertura_Sucursal_Dias <= 30
				THEN 2
			WHEN Cobertura_Sucursal_Dias > 30 AND	Cobertura_Sucursal_Dias <= 45
				THEN 3
			WHEN Cobertura_Sucursal_Dias > 45 AND	Cobertura_Sucursal_Dias <= 60
				THEN 4
			WHEN Cobertura_Sucursal_Dias > 60 AND	Cobertura_Sucursal_Dias <= 90
				THEN 5
			WHEN Cobertura_Sucursal_Dias > 90 AND	Cobertura_Sucursal_Dias <= 120
				THEN 6
			WHEN Cobertura_Sucursal_Dias > 120 AND	Cobertura_Sucursal_Dias <= 180
				THEN 7
			WHEN Cobertura_Sucursal_Dias > 180
				THEN 8
			WHEN Cobertura_Sucursal_Dias IS NULL
				THEN 99
			ELSE 0
		END AS Cobertura_Sucursal_Dias_Id
	FROM	Coberturas_Dias)
SELECT
	ISNULL(Emp_Id,0) Emp_Id
	, ISNULL(Sucursal_Id,0) Sucursal_Id
	, ISNULL(ArticuloPadre_Id,'X') ArticuloPadre_Id
	, CONVERT(DECIMAL(16, 4), Cobertura_Cadena_Dias) Cobertura_Cadena_Dias
	, CONVERT(DECIMAL(16, 4), Cobertura_Marca_Dias) Cobertura_Marca_Dias
	, CONVERT(DECIMAL(16, 4), Cobertura_Zona_Dias) Cobertura_Zona_Dias
	, CONVERT(DECIMAL(16, 4), Cobertura_Departamento_Dias) Cobertura_Departamento_Dias
	, CONVERT(DECIMAL(16, 4), Cobertura_Ciudad_Dias) Cobertura_Ciudad_Dias
	, CONVERT(DECIMAL(16, 4), Cobertura_Sucursal_Dias) Cobertura_Sucursal_Dias
	, Cobertura_Cadena_Dias_Id
	, Cobertura_Marca_Dias_Id
	, Cobertura_Zona_Dias_Id
	, Cobertura_Departamento_Dias_Id
	, Cobertura_Ciudad_Dias_Id
	, Cobertura_Sucursal_Dias_Id
	, ISNULL(CONVERT(DECIMAL(16, 4), Existencia),0) Existencia
	, ISNULL(CONVERT(DECIMAL(16, 4), Transito),0) Transito
	, HashStr_SucEmp
	, HashStr_ArtEmp
	, GETDATE() AS Fecha_Carga
	, GETDATE() AS Fecha_Actualizacion
FROM	Coberturas_Id