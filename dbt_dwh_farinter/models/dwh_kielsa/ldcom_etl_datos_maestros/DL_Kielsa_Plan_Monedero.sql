{{ 
    config(
		materialized="view",
		tags=["periodo/diario"],
	) 
}}
--dbt dagster

SELECT Emp_Id, 
    Monedero_Id as MonederoPlan_Id, 
    Monedero_Nombre as MonederoPlan_Nombre, 
    ABS(CAST(HASHBYTES('SHA2_256', CONCAT(Monedero_Id, '-', Emp_Id)) AS int)) AS Hash_MonederoPlanEmp
FROM {{ref ('DL_Kielsa_Monedero_Plan')}}