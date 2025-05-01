{% set unique_key_list = ["Material_Id", "Fecha_Id","Centro_Almacen_Id","Sociedad_Id"] %}
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

SELECT --top 100
		ISNULL(S.Sociedad_Id, 'OTROS') as Sociedad_Id
		, ISNULL(E1.Almacen_Id_Mapeado, 'X') AS Centro_Almacen_Id
		, ISNULL(C.Material_Id, 'X') AS Material_Id
		, CAL.Anio_Calendario AS Anio_Id
		, CAL.Mes_Calendario Mes_Id
        , ISNULL(MAX(CAL.Mes_Inicio), '19000101') AS Fecha_Id
        , SUM(CONVERT(DECIMAL, V.Libre_Cantidad + V.Calidad_Cantidad + V.TransitoAlm_Cantidad+ V.TransitoCentro_Cantidad)) AS Stock_Cierre
		, MAX(V.DiasSin_Stock) AS DiasSin_Stock
		, MAX(B.Gpo_Obs_Nombre_Corto) AS Gpo_Obs_Nombre_Corto
		, B.Gpo_Obs_Id
        , B.Gpo_Plan_Id COLLATE DATABASE_DEFAULT AS Gpo_Plan
		, MAX(C.Sector_Id) AS Sector
		, MAX(C.Articulo_Nombre) AS Material_Nombre
		, C.Articulo_Id
	FROM 	dbo.BI_SAP_Hecho_ExistenciasHist V -- {{ ref('BI_SAP_Hecho_ExistenciasHist') }}
		INNER JOIN dbo.BI_Dim_Articulo_SAP C -- {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }}
			ON V.Articulo_Id = C.Articulo_Id
		INNER JOIN dbo.BI_Dim_Sociedad_SAP S -- {{ source('BI_FARINTER', 'BI_Dim_Sociedad_SAP') }}
			ON V.Sociedad_Id = S.Sociedad_Id
		INNER JOIN DL_FARINTER.dbo.DL_Planning_ParamSocMat B -- {{ ref('DL_Planning_ParamSocMat') }}
			ON S.Sociedad_Id = B.Sociedad_Id AND C.Articulo_Id = B.Articulo_Id
		INNER JOIN dbo.BI_Dim_Centro_SAP D -- {{ source('BI_FARINTER', 'BI_Dim_Centro_SAP') }}
			ON V.Centro_Id = D.Centro_Id
		INNER JOIN DL_FARINTER.[dbo].[DL_Edit_AlmacenFP_SAP] E1 -- {{ ref('DL_Edit_AlmacenFP_SAP') }}
			ON V.Almacen_Id = E1.Almacen_Id 
		INNER JOIN dbo.BI_Dim_Calendario CAL -- {{ source('BI_FARINTER', 'BI_Dim_Calendario') }}
			ON V.AnioMes_Id = CAL.AnioMes_Id
			AND V.Fecha_Id = CAL.Fecha_Id
		WHERE CAL.Anio_Calendario >= YEAR(DATEADD(year, -5, DATEADD(MONTH, -1, EOMONTH(GETDATE()))))
			AND CAL.Fecha_Id > DATEADD(year, -5, DATEADD(MONTH, -1, EOMONTH(GETDATE())))
			AND CAL.Fecha_Id <= DATEADD(MONTH, -1, EOMONTH(GETDATE()))
			AND V.Libre_Cantidad + V.Calidad_Cantidad + V.TransitoAlm_Cantidad+ V.TransitoCentro_Cantidad > 0 
			AND V.Stock_id = 1 AND 
			E1.Planificado = 'S'	--and B.Gpo_Obs_Id = 'COINS' --and A.Sociedad_Id = '1200' 
			AND S.Sociedad_Id IN ( '1200', '1300', '1301', '1700', '2500' )
GROUP BY S.Sociedad_Id
				, C.Articulo_Id
				, E1.Almacen_Id_Mapeado
				, CAL.Anio_Calendario
				, CAL.Mes_Calendario
                , C.Material_Id
                , C.Sector_Id
                , B.Gpo_Plan_Id
                , B.Gpo_Obs_Id
