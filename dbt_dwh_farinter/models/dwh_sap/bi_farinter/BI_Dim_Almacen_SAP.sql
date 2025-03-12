{% set unique_key_list = ["Almacen_Id"] %}
{{ 
    config(
		tags=["periodo/diario"],
		materialized="view",
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}
SELECT CenAlm_Id AS Almacen_Id,
	Almacen_Nombre,
	Centro_Id AS C1,
	Centro_Nombre AS C2,
	Sociedad_Id AS C3,
	Sociedad_Nombre AS C4,
	'Usar BI_SAP_Dim_Almacen' AS Descontinuado
FROM {{ ref('BI_SAP_Dim_Almacen') }}
