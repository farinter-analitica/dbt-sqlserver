{{ 
    config(
		materialized="view",
		tags=["periodo/diario"],
	) 
}}
-- Solo editable en DBT DAGSTER
SELECT CanalVenta_Id, CanalVenta_Nombre
FROM {{source ('DL_FARINTER','DL_Kielsa_CanalVenta')}}
UNION ALL
select 5 AS CanalVenta_Id, 'eCommerce' AS CanalVenta_Nombre