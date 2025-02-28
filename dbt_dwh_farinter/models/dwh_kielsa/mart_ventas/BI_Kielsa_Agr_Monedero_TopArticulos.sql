 
{% set unique_key_list = ["Monedero_Id","Emp_Id", "Articulo_Id"] %}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns= unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns= unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
		]	
) 
}}
WITH last_6_months AS (
	SELECT FP.[Emp_Id],
		A.Articulo_Codigo_Padre AS Articulo_Id,
		FP.[MonederoTarj_Id_Limpio] AS Monedero_Id,
		FP.[Cantidad_Padre],
		FP.[Valor_Neto],
		FP.[EmpSucDocCajFac_Id]
	FROM [BI_FARINTER].[dbo].[BI_Kielsa_Hecho_FacturaPosicion] FP
		INNER JOIN [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Articulo] A ON A.Emp_Id = FP.Emp_Id
		AND A.Articulo_Id = FP.Articulo_Id
	WHERE FP.[Factura_Fecha] >= DATEADD(MONTH, -6, GETDATE())
),
metrics AS (
	SELECT Emp_Id,
		Monedero_Id,
		Articulo_Id,
		COUNT(DISTINCT EmpSucDocCajFac_Id) AS Frecuencia,
		SUM(Valor_Neto) AS Valor_Neto,
		SUM(Cantidad_Padre) AS Cantidad_Total
	FROM last_6_months
	GROUP BY Emp_Id,
		Monedero_Id,
		Articulo_Id
),
normalized AS (
	SELECT m.*,
		Frecuencia / AVG(Frecuencia) OVER (PARTITION BY Emp_Id, Monedero_Id) AS Frecuencia_Norm,
		Valor_Neto / AVG(Valor_Neto) OVER (PARTITION BY Emp_Id, Monedero_Id) AS Valor_Neto_Norm,
		Cantidad_Total / AVG(Cantidad_Total) OVER (PARTITION BY Emp_Id, Monedero_Id) AS Cantidad_Total_Norm
	FROM metrics m
	WHERE Frecuencia > 0
	AND Valor_Neto > 0
	AND Cantidad_Total > 0
),
ranked AS (
	SELECT Emp_Id,
		Monedero_Id,
		Articulo_Id,
		Frecuencia,
		Valor_Neto,
		Cantidad_Total,
		(
			Frecuencia_Norm + Valor_Neto_Norm + Cantidad_Total_Norm
		) / 3 AS Combined_Score,
		ROW_NUMBER() OVER (
			PARTITION BY Emp_Id
			ORDER BY (
					Frecuencia_Norm + Valor_Neto_Norm + Cantidad_Total_Norm
				) / 3 DESC
		) AS rank
	FROM normalized
)
SELECT Emp_Id,
	Monedero_Id,
	Articulo_Id,
	Frecuencia,
	Valor_Neto,
	Cantidad_Total,
	Combined_Score,
	Rank
FROM ranked
WHERE rank <= 5