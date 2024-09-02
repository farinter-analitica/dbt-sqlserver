{% set unique_key_list = ["Emp_Id","Sucursal_Id","ArticuloPadre_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ],
		owners = ["cleymer.mendoza@farinter.com"]
	) 
}}

-- Hijos segundo nivel
WITH TBL_MOVIMIENTOS AS (
	SELECT A.Emp_Id, 
		A.Suc_Destino AS Suc_Id, 
		A.Bodega_Destino AS Bodega_Id,
		--, COUNT(DISTINCT B.Suc_Id) AS Suc_Origen
		--, COUNT(DISTINCT B.Mov_Id) AS Documento
		--, MAX(B.Mov_Fecha_Aplicado) AS Fecha
		A.Articulo_Id, 
		SUM(A.Detalle_Cantidad) AS Cantidad
	FROM {{source('DL_FARINTER', 'DL_Kielsa_MovimientosHist')}} A
		INNER JOIN 
			{{source('DL_FARINTER', 'DL_Kielsa_Tipo_Mov_Inventario')}} C
				ON A.Emp_Id = C.Emp_Id AND A.Tipo_Id = C.Tipo_Id
	WHERE 
		A.Mov_Estado = 'AP' 
		AND C.Tipo_Tipo_Movimiento = 2 
		AND A.Bodega_Destino = 1
	GROUP BY 
		A.Articulo_Id, A.Emp_Id, A.Suc_Destino, A.Bodega_Destino
),

TBL_VENCIDOS AS (
	SELECT --top 10 
		B.Emp_Id,
		A.Suc_Id AS Suc_Id, 
		C.Articulo_Codigo_Padre AS Articulo_Id, 
		CASE WHEN C.Indicador_PadreHijo = 'H' THEN SUM(A.Detalle_Cantidad/ISNULL(C.Factor_Denominador, 1))
		ELSE 
		SUM(A.Detalle_Cantidad) 
		END 
		AS Vencido 
		
	FROM {{source('DL_FARINTER', 'DL_Kielsa_MovimientosHist')}} A
	INNER JOIN {{ref ('BI_Kielsa_Dim_Sucursal') }}  B
	ON A.Emp_Id = B.Emp_Id AND A.Suc_Destino = B.Sucursal_Id
		LEFT OUTER JOIN {{ref ('BI_Kielsa_Dim_Articulo') }} C
			ON A.Emp_Id = C.Emp_Id 
			AND A.Articulo_Id = C.Articulo_Id
	WHERE 
		A.Bodega_Destino = 2 

		AND B.Indicador_CEDI= 1
		AND A.Mov_Estado = 'RE' 
		AND A.Tipo_Id = 3	
	GROUP BY 
		B.Emp_Id ,A.Suc_Id, C.Articulo_Codigo_Padre, C.Indicador_PadreHijo

),
TBL_VENCIDOS_HISTORICOS AS (

	SELECT 
		V.Emp_Id,
		V.Suc_Id, 
		V.Articulo_Id, 
		SUM(V.Vencido) AS Vencido
	FROM TBL_VENCIDOS V
	GROUP BY V.Suc_Id, V.Articulo_Id, V.Emp_Id
),

TBL_KIELSA_INV_DEMANDA AS (
	SELECT
		A.Emp_Id,
		A.Suc_Id, 
		B.Articulo_Codigo_Padre AS Articulo_Id, 
		CASE WHEN B.Indicador_PadreHijo = 'H' THEN SUM(A.Demanda_Cantidad/ISNULL(B.Factor_Denominador, 1))
		ELSE 
		SUM(A.Demanda_Cantidad) 
		END AS Cantidad
	FROM 
		 {{ source ('DL_FARINTER', 'DL_Kielsa_INV_Demanda')}} A
	LEFT JOIN {{ref ('BI_Kielsa_Dim_Articulo') }} B 
		ON A.Emp_Id = B.Emp_Id AND A.Articulo_Id = B.Articulo_Id
	WHERE LEN(A.Articulo_Id) > 9	
	GROUP BY A.Suc_Id, B.Articulo_Codigo_Padre, B.Indicador_PadreHijo, A.Emp_Id
),

TBL_KIELSA_INV_HISTORICO AS (
	SELECT --top 10 
		A.Emp_Id,
		A.Suc_Id, 
		B.Articulo_Codigo_Padre AS Articulo_Id, 
		CASE WHEN B.Indicador_PadreHijo = 'H' THEN SUM(A.Demanda_Cantidad/ISNULL(B.Factor_Denominador, 1))
		ELSE 
		SUM(A.Demanda_Cantidad) 
		END AS Cantidad
	FROM {{ source ('DL_FARINTER', 'DL_Kielsa_INV_Demanda_Histo')}} A
		LEFT JOIN {{ref ('BI_Kielsa_Dim_Articulo') }} B 
		ON A.Emp_Id = B.Emp_Id AND A.Articulo_Id = B.Articulo_Id
	WHERE LEN(A.Articulo_Id) > 9	
	GROUP BY A.Suc_Id, B.Articulo_Codigo_Padre,B.Indicador_PadreHijo,A.Emp_Id
),

TBL_DEMANDA AS (

	SELECT
		A.Emp_Id,
		A.Suc_Id, 
		A.Articulo_Id, 
		SUM(A.Cantidad) AS Cantidad
	FROM (
		SELECT
			A.Emp_Id,
			A.Suc_Id, 
			A.Articulo_Id, 
			A.Cantidad
		FROM TBL_KIELSA_INV_DEMANDA A
		UNION
		SELECT 
			A.Emp_Id,
			A.Suc_Id, 
			A.Articulo_Id, 
			A.Cantidad
		FROM TBL_KIELSA_INV_HISTORICO A
	)  A GROUP BY A.Suc_Id, A.Articulo_Id, A.Emp_Id
	

),

-- Hijos primer nivel

TBL_ARTICULOS AS (
	SELECT 
		A.Emp_Id AS Emp_Id, 
		A.Suc_Id AS Suc_Id, 
		C.Articulo_Codigo_Padre AS Articulo_Id, 
		SUM(CASE WHEN C.Indicador_PadreHijo = 'P' THEN A.AxC_Promedio_Diario * 30 ELSE 0 END) PV_Mensual, 
		SUM(CASE WHEN C.Indicador_PadreHijo = 'P' THEN A.AxC_Rotacion_Dias ELSE 0 END) AS Politica,  
		CASE WHEN C.Indicador_PadreHijo = 'H'THEN SUM(A.AxC_Existencia_Total / ISNULL(C.Factor_Denominador, 1))
		ELSE SUM(A.AxC_Existencia_Total) END AS Existencia, 
		CASE WHEN C.Indicador_PadreHijo = 'H' THEN SUM(A.AxC_Transito / ISNULL(C.Factor_Denominador, 1)) 
		ELSE SUM(A.AxC_Transito )END AS Transito,
		
		--Maximo definido por el sistema
		SUM(CASE WHEN A.AxC_Rotacion_Dias = 1
				THEN --Politica dias
					CASE WHEN C.Indicador_PadreHijo = 'P' 
						THEN A.AxC_Rotacion_Maximo * A.AxC_Promedio_Diario
						ELSE 0 END
				ELSE --Politica cantidad
					CASE WHEN C.Indicador_PadreHijo = 'P' 
						THEN A.AxC_Rotacion_Maximo
						ELSE 0 END
				END) AS Maximo,
		--Minimo definido por el sistema
		SUM(CASE WHEN A.AxC_Rotacion_Dias = 1
				THEN --Politica dias
					CASE WHEN C.Indicador_PadreHijo = 'P' 
						THEN A.AxC_Rotacion_Minimo * A.AxC_Promedio_Diario
					ELSE 0 END
				ELSE --Politica cantidad
					CASE WHEN C.Indicador_PadreHijo = 'P' 
						THEN A.AxC_Rotacion_Minimo
						ELSE 0 END
				END) AS Minimo,
		SUM(CASE WHEN C.Indicador_PadreHijo = 'P'  THEN A.AxC_Cantidad_Minima ELSE 0 END) Cantidad_minima,
		SUM(CASE WHEN C.Indicador_PadreHijo = 'P' THEN B.AxS_Costo_Actual ELSE 0 END) Costo,
		CASE WHEN C.Indicador_PadreHijo = 'H' THEN SUM(ISNULL(ET.Cantidad,0) / ISNULL(C.Factor_Denominador, 1))
		ELSE SUM(ISNULL(ET.Cantidad,0)) END AS Existencia_Transito
		--SUM(ISNULL(ET.Cantidad,0) / ISNULL(C.Factor_Numerador, 1)) AS Existencia_Transito
	FROM {{ source('DL_FARINTER', 'DL_Kielsa_Articulo_x_Compra')}} A 
		INNER JOIN {{ source('DL_FARINTER', 'DL_Kielsa_Articulo_x_Sucursal')}} B 
			ON A.Emp_Id = B.Emp_Id 
			AND A.Suc_Id = B.Suc_Id 
			AND A.Articulo_Id = B.Articulo_Id
		LEFT JOIN TBL_MOVIMIENTOS ET
			ON ET.Emp_Id = A.Emp_Id AND ET.Suc_Id = A.Suc_Id AND ET.Articulo_Id = A.Articulo_Id
		LEFT OUTER JOIN {{ref ('BI_Kielsa_Dim_Articulo') }} C --Buscando padres de hijos, para su factor unitario
				   ON A.Emp_Id = C.Emp_Id AND A.Articulo_Id = C.Articulo_Id
	WHERE 
		A.Bodega_Id = 1	
		AND C.Indicador_PadreHijo = 'P'
	GROUP BY A.Emp_Id, A.Suc_Id, C.Articulo_Codigo_Padre, C.Indicador_PadreHijo

)

-- Query principal
SELECT --top 10
	ISNULL(T.Emp_Id,0) AS Emp_Id,
	ISNULL(T.Suc_Id,0) AS Sucursal_Id,
	ISNULL(T.Articulo_Id,0) AS ArticuloPadre_Id,
	Matriz_Id = CASE WHEN SUM(T.PV_Mensual) > 0 THEN 1 ELSE 0 END,
	SUM(T.Politica) AS Politica,
	SUM(T.PV_Mensual) AS PV_Mensual,
	SUM(T.Cantidad_Minima) AS Cantidad_Minima,
	SUM(T.Minimo) AS Minimo,
	SUM(T.Maximo) AS Maximo,
	SUM(T.Existencia) AS Existencia,
	SUM(T.Transito) AS Transito,
	SUM(ISNULL(C.Vencido, 0)) AS Vencido,
	SUM(ISNULL(D.Cantidad, 0)) AS Perdida,
	SUM(T.Costo) AS Costo,
	SUM(T.Existencia_Transito) AS Existencia_Transito  ,
	GETDATE() AS Fecha_Actualizado
FROM TBL_ARTICULOS T
	LEFT JOIN TBL_VENCIDOS_HISTORICOS C
		ON T.Emp_Id = C.Emp_Id AND T.Suc_Id = C.Suc_Id AND T.Articulo_Id = C.Articulo_Id
	LEFT JOIN TBL_DEMANDA D
		ON T.Emp_Id = D.Emp_Id AND T.Suc_Id = D.Suc_Id AND T.Articulo_Id = D.Articulo_Id
		--WHERE T.Articulo_Id = '1110000488'
GROUP BY T.Emp_Id, T.Suc_Id, T.Articulo_Id