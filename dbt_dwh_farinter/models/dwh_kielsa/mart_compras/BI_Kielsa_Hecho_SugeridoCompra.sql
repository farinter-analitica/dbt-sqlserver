{% set unique_key_list = ["Emp_Id","Sucursal_Id","ArticuloPadre_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","detener_carga/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}
{% set v_semanas_muestra = 12 %}
{% set v_dias_muestra = v_semanas_muestra * 7 %}
{% set v_fecha_inicio = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=v_dias_muestra)).strftime('%Y%m%d') %}
{% set v_fecha_fin = modules.datetime.datetime.now().strftime('%Y%m%d')  %}
{% set v_anio_mes_inicio =  v_fecha_inicio[:6]  %}

/*
DECLARE @Inicio AS DATE = GETDATE()
DECLARE @SemanasPonderacion AS INT = 12
DECLARE @DiasPonderacion AS INT = @SemanasPonderacion*7 --Historia para ponderar
DROP TABLE IF EXISTS #Temp
;*/
WITH
BoletaDetalle AS
	(SELECT -- top 10
		BoletaLDetalle.Emp_Id
		, BoletaLDetalle.Suc_Id
		, BoletaLDetalle.Bodega_Id
		, BoletaLDetalle.Articulo_Id
		, MAX(BoletaLEncabezado.Boleta_Fecha) AS Fecha_Entrada
	FROM	DL_FARINTER.dbo.DL_Kielsa_BoletaLocal_Detalle BoletaLDetalle --{{ source('DL_FARINTER', 'DL_Kielsa_BoletaLocal_Detalle')}}
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_BoletaLocal_Encabezado BoletaLEncabezado --{{ source('DL_FARINTER', 'DL_Kielsa_BoletaLocal_Encabezado') }}
		ON BoletaLDetalle.Emp_Id = BoletaLEncabezado.Emp_Id
		AND BoletaLDetalle.Suc_Id = BoletaLEncabezado.Suc_Id
		AND BoletaLDetalle.Bodega_Id = BoletaLEncabezado.Bodega_Id
		AND BoletaLDetalle.Boleta_Id = BoletaLEncabezado.Boleta_Id
	GROUP BY BoletaLDetalle.Emp_Id, BoletaLDetalle.Suc_Id, BoletaLDetalle.Bodega_Id, BoletaLDetalle.Articulo_Id)
,
Boleta_Exterior AS
	(SELECT --top 10
		BoletaExterior.Emp_Id
		, BoletaExterior.Suc_Id
		, BoletaExterior.Bodega_Id
		, BoletaExterior.Articulo_Id
		, MAX(BoletaLEncabezado.Boleta_Fecha) AS Fecha_Entrada
	FROM	DL_FARINTER.dbo.DL_Kielsa_Boleta_Exterior_Hist BoletaExterior --{{ source('DL_FARINTER', 'DL_Kielsa_Boleta_Exterior_Hist')}}
	INNER JOIN DL_FARINTER.dbo.DL_Kielsa_BoletaLocal_Encabezado BoletaLEncabezado --{{ source('DL_FARINTER', 'DL_Kielsa_BoletaLocal_Encabezado') }}
		ON BoletaExterior.Emp_Id = BoletaLEncabezado.Emp_Id
		AND BoletaExterior.Suc_Id = BoletaLEncabezado.Suc_Id
		AND BoletaExterior.Bodega_Id = BoletaLEncabezado.Bodega_Id
		AND BoletaExterior.Boleta_Id = BoletaLEncabezado.Boleta_Id
	GROUP BY BoletaExterior.Emp_Id, BoletaExterior.Suc_Id, BoletaExterior.Bodega_Id, BoletaExterior.Articulo_Id)
,
Boleta AS
	(SELECT
		Boletas.Emp_Id
		, (Boletas.Suc_Id) AS Suc_Id
		, Boletas.Articulo_Id AS Articulo_Id
		, MAX(Boletas.Fecha_Entrada) AS Fecha_Entrada
	FROM
		(SELECT
			BoletaDetalle.Emp_Id
			, BoletaDetalle.Suc_Id
			, BoletaDetalle.Bodega_Id
			, BoletaDetalle.Articulo_Id
			, MAX(BoletaDetalle.Fecha_Entrada) AS Fecha_Entrada
		FROM	BoletaDetalle BoletaDetalle
		GROUP BY BoletaDetalle.Emp_Id, BoletaDetalle.Suc_Id, BoletaDetalle.Bodega_Id, BoletaDetalle.Articulo_Id
		UNION ALL
		SELECT
			Boleta_Exterior.Emp_Id
			, Boleta_Exterior.Suc_Id
			, Boleta_Exterior.Bodega_Id
			, Boleta_Exterior.Articulo_Id
			, MAX(Boleta_Exterior.Fecha_Entrada) AS Fecha_Entrada
		FROM	Boleta_Exterior Boleta_Exterior
		GROUP BY Boleta_Exterior.Emp_Id, Boleta_Exterior.Suc_Id, Boleta_Exterior.Bodega_Id, Boleta_Exterior.Articulo_Id) Boletas
	GROUP BY Boletas.Emp_Id, Boletas.Suc_Id, Boletas.Articulo_Id, Boletas.Fecha_Entrada)
,
Compras AS
	(SELECT
		Compra.Emp_Id AS Emp_Id
		, Compra.Sucursal_Id AS Sucursal_Id
		, Compra.ArticuloPadre_Id AS ArticuloPadre_Id
		, Compra.Politica AS Politica_Id
		, CASE
			WHEN Boleta.Fecha_Entrada IS NULL
				THEN CONVERT(DATETIME, '')
			ELSE Boleta.Fecha_Entrada
		END AS Fecha_Entrada
		, Compra.PV_Mensual AS PV_Mensual
		, Compra.Cantidad_Minima AS Cantidad_Minima
		, Compra.Minimo AS Minimo
		, Compra.Maximo AS Maximo
		, Compra.Existencia AS Existencia_cantidad
		, Compra.Transito AS Transito_cantidad

		--Politica dias con restricción de cantidad minima mayor al maximo sugerido
		, CASE
			WHEN Compra.Politica = 1
				THEN CASE
						WHEN Compra.Transito > 0
							THEN CASE
									WHEN Compra.Existencia + Compra.Transito <= Compra.Minimo
										THEN CASE
												WHEN Compra.Cantidad_Minima <> 0
													AND Compra.Cantidad_Minima > Compra.Maximo
													AND (Compra.Existencia + Compra.Transito) <= compra.Cantidad_Minima
													THEN Compra.Cantidad_Minima - (Compra.Existencia + Compra.Transito)
												ELSE Compra.Maximo - (Compra.Existencia + Compra.Transito)
											END
									ELSE
										CASE
											WHEN Compra.Cantidad_Minima <> 0
												AND Compra.Cantidad_Minima > Compra.Maximo
												AND (Compra.Existencia + Compra.Transito) <= Compra.Cantidad_Minima
												THEN Compra.Cantidad_Minima - (Compra.Existencia + Compra.Transito)
											ELSE 0
										END
								END
						ELSE
							CASE
								WHEN Compra.Existencia <= Compra.Minimo
									THEN CASE
											WHEN Compra.Cantidad_Minima <> 0
												AND Compra.Cantidad_Minima > Compra.Maximo
												AND Compra.Existencia <= compra.Cantidad_Minima
												THEN Compra.Cantidad_Minima - Compra.Existencia
											ELSE Compra.Maximo - Compra.Existencia
										END
								ELSE
									CASE
										WHEN Compra.Cantidad_Minima <> 0
											AND Compra.Cantidad_Minima > Compra.Maximo
											AND Compra.Existencia <= Compra.Cantidad_Minima
											THEN Compra.Cantidad_Minima - Compra.Existencia
										ELSE 0
									END
							END
					END
			ELSE -- Politica cantidad
				CASE
					WHEN Compra.Transito > 0
						THEN CASE
								WHEN Compra.Existencia + Compra.Transito <= Compra.Minimo
									THEN Compra.Maximo - (Compra.Existencia + Compra.Transito)
								ELSE 0
							END
					ELSE CASE
							WHEN Compra.Existencia <= Compra.Minimo
								THEN Compra.Maximo - Compra.Existencia
							ELSE 0
						END
				END
		END Sugerido_Cantidad
		, CASE
			WHEN Compra.PV_Mensual <> 0 AND Compra.Emp_Id IN ( 1, 5 )
				THEN Compra.Existencia / (Compra.PV_Mensual / 30)
			ELSE CASE
					WHEN Compra.PV_Mensual <> 0 AND Compra.Emp_Id NOT IN ( 1, 5 )
						THEN Compra.Existencia / (Compra.PV_Mensual)
					ELSE CASE
								WHEN Compra.Existencia <> 0
									THEN 9999
								ELSE 0
							END
				END
		END AS Dias

		--Politica dias con restricción de cantidad minima mayor al maximo sugerido
		, CASE
			WHEN Compra.Politica = 1
				THEN CASE
						WHEN Compra.Transito > 0
							THEN CASE
									WHEN Compra.Cantidad_Minima <> 0 AND Compra.Cantidad_Minima > Compra.Maximo
										THEN --Compra.Existencia + Compra.Transito < Compra.Minimo
		CASE
			WHEN Compra.Existencia > Compra.Cantidad_Minima
				AND (CASE WHEN Boleta.Fecha_Entrada IS NULL THEN CONVERT(DATETIME, '')ELSE Boleta.Fecha_Entrada END) > GETDATE()
																														- 120
				THEN Compra.Existencia - Compra.Cantidad_Minima
			ELSE 0
		END
									ELSE
										CASE
											WHEN Compra.Existencia > Compra.Maximo
												AND (CASE WHEN Boleta.Fecha_Entrada IS NULL THEN CONVERT(DATETIME, '')ELSE
																													Boleta.Fecha_Entrada END) > GETDATE()
																																				- 120
												THEN Compra.Existencia - Compra.Maximo
											ELSE 0
										END
								END
						ELSE
							CASE
								WHEN Compra.Cantidad_Minima <> 0 AND Compra.Cantidad_Minima > Compra.Maximo
									THEN --Compra.Existencia > Compra.Maximo
		CASE
			WHEN Compra.Existencia > Compra.Cantidad_Minima
				AND (CASE WHEN Boleta.Fecha_Entrada IS NULL THEN CONVERT(DATETIME, '')ELSE Boleta.Fecha_Entrada END) > GETDATE()
																														- 120
				THEN Compra.Existencia - Compra.Cantidad_Minima
			ELSE 0
		END
								ELSE
									CASE
										WHEN Compra.Existencia > Compra.Maximo
											AND (CASE WHEN Boleta.Fecha_Entrada IS NULL THEN CONVERT(DATETIME, '')ELSE
																												Boleta.Fecha_Entrada END) > GETDATE()
																																			- 120
											THEN Compra.Existencia - Compra.Maximo
										ELSE 0
									END
							END
					END
			ELSE -- Politica cantidad
				CASE
					WHEN Compra.Transito > 0
						THEN CASE
								WHEN Compra.Existencia > Compra.Maximo
									AND (CASE WHEN Boleta.Fecha_Entrada IS NULL THEN CONVERT(DATETIME, '')ELSE
																										Boleta.Fecha_Entrada END) > GETDATE()
																																	- 120
									THEN Compra.Existencia - Compra.Maximo
								ELSE 0
							END
					ELSE
						CASE
							WHEN Compra.Existencia > Compra.Maximo
								AND (CASE WHEN Boleta.Fecha_Entrada IS NULL THEN CONVERT(DATETIME, '')ELSE
																									Boleta.Fecha_Entrada END) > GETDATE()
																																- 120
								THEN Compra.Existencia - Compra.Maximo
							ELSE 0
						END
				END
		END Exceso_Cantidad
		, Compra.Vencido AS Vencido_cantidad
		, Compra.Perdida AS Perdida_Cantidad
		, Compra.Costo AS Costo
		, Compra.Existencia_Transito AS Existencia_Transito_Cantidad
	FROM	DL_FARINTER.[dbo].[DL_Kielsa_TC_Sugerido] Compra --{{ ref('DL_Kielsa_TC_Sugerido') }}
	LEFT OUTER JOIN Boleta Boleta
		ON Compra.Emp_Id = Boleta.Emp_Id
		AND Compra.Sucursal_Id = Boleta.Suc_Id
		AND Compra.ArticuloPadre_Id = Boleta.Articulo_Id
        )

---Query Principal
SELECT --top 20
	ISNULL(Compras.Emp_Id,0) AS Emp_Id
	, ISNULL(Compras.Sucursal_Id,0) AS Sucursal_Id
	, ISNULL(Compras.ArticuloPadre_Id,0) AS ArticuloPadre_Id
	, Compras.Politica_Id
	, CASE
		WHEN Compras.Dias <= 0.001
			THEN 1
		WHEN Compras.Dias < 7.001
			THEN 2
		WHEN Compras.Dias < 15.001
			THEN 3
		WHEN Compras.Dias < 30.001
			THEN 4
		WHEN Compras.Dias < 60.011
			THEN 5
		WHEN Compras.Dias < 90.011
			THEN 6
		WHEN Compras.Dias < 121.011
			THEN 7
		WHEN Compras.Dias < 151.011
			THEN 8
		WHEN Compras.Dias < 181.011
			THEN 9
		ELSE 10 -- More than 180 days
	END AS DiasInv_Id
	, CASE
		WHEN Compras.Dias < 30.001
			THEN 1	-- Urgente
		WHEN Compras.Dias < 60.011
			THEN 2	-- Comprar urgente
		WHEN Compras.Dias < 90.011
			THEN 3	-- Compra mensual
		WHEN Compras.Dias < 121.011
			THEN 4	-- Compra dentro 1 mes
		WHEN Compras.Dias < 151.011
			THEN 5	-- Compra dentro 2 meses
		WHEN Compras.Dias < 181.011
			THEN 6	-- Compra dentro 3 meses
		ELSE 7		-- Compra dentro 4 meses or No comprar Exceso en CD
	END AS AlertaInv_Id
	, CASE
		WHEN Compras.Vencido_cantidad >= 0.001
			THEN 1
		ELSE 0
	END AS AlertaVenc_Id
	, CASE
		WHEN Compras.Perdida_Cantidad >= 0.001
			THEN 1
		ELSE 0
	END AS AlertaPerdida_Id
	, Compras.Fecha_Entrada

	--Redondeos en base a reglas definidas por Eduardo Torres
	, Compras.PV_Mensual AS PV_Mensual
	, Compras.Cantidad_Minima AS Cantidad_Minima
	, CAST(Compras.Minimo AS INT) AS Minimo
	, CAST(Compras.Maximo AS INT) AS Maximo
	, ROUND(Compras.Dias, 4) AS Dias
	, CASE
		WHEN Compras.PV_Mensual = 0 AND Compras.Minimo = 0 AND	Compras.Maximo = 0
			THEN 0
		ELSE 1
	END AS Matriz_Id
	, Compras.Existencia_cantidad AS Existencia_cantidad
	, Compras.Transito_cantidad AS Transito_cantidad
	, BI_FARINTER.[dbo].[fnc_RedondeoSugerido_Kielsa](Compras.Sugerido_Cantidad) AS Sugerido_Cantidad
	, BI_FARINTER.[dbo].[fnc_RedondeoSugerido_Kielsa](Compras.Exceso_cantidad) AS Exceso_cantidad
	, Compras.Vencido_cantidad AS Vencido_cantidad
	, Compras.Perdida_cantidad AS Perdida_cantidad
	, Compras.Costo AS Costo
	, Compras.Existencia_Transito_Cantidad
	, Existencia_valor = Existencia_cantidad * Costo
	, Transito_valor = Transito_cantidad * Costo
	, Sugerido_valor = Sugerido_cantidad * Costo
	, Exceso_valor = Exceso_cantidad * Costo
	, Vencido_valor = Vencido_cantidad * Costo
	, Perdida_valor = Perdida_cantidad * Costo
	, Art.EmpArt_Id
	, Suc.EmpSuc_Id
    , GETDATE() AS Fecha_Actualizado
FROM	Compras
LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo Art -- {{ref ('BI_Kielsa_Dim_Articulo')}} Art
	ON Compras.Emp_Id = Art.Emp_Id AND Compras.ArticuloPadre_Id = Art.Articulo_Id
LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal Suc -- {{ref ('BI_Kielsa_Dim_Sucursal')}} Suc
	ON Compras.Emp_Id = Suc.Emp_Id AND Compras.Sucursal_Id = Suc.Sucursal_Id

--WHERE Compras.ArticuloPadre_Id = '1110032419' AND Compras.Sucursal_Id = 1 AND Compras.Emp_Id = 2
