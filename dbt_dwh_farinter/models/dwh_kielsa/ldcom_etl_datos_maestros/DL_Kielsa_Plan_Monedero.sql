{{ 
    config(
		materialized="view",
		tags=["periodo/diario"],
	) 
}}
--dbt dagster

SELECT Emp_Id, 
    Monedero_Id, 
    Monedero_Nombre, 
    ABS(CAST(HASHBYTES('SHA1', CONCAT(Monedero_Id, 0, Emp_Id)) AS bigint)) AS Hash_MonederoPlanEmp
FROM {{ref ('DL_Kielsa_Monedero_Plan')}}