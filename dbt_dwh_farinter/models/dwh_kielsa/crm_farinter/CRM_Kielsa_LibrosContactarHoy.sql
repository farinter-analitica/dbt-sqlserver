{%- set unique_key_list = ["Emp_Id","Sucursal_Id", "Identidad", "Articulo_Id" ,"Contactar_El"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
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

/*
2024-03-22: Creacion de la vista
autor: Axell Padilla
Esta vista obtiene los libros que se deben contactar hoy, se asigna un vendedor por cliente de forma aleatoria.
*/
WITH 
LibrosBase AS 
(
SELECT
	RC.Emp_Id
	, RC.Emp_Id as Pais_Id
	, RC.Ultima_Sucursal_Id Sucursal_Id
	--, RC.Sucursal_Nombre
    , RC.Fecha_Ultima_Compra Fecha_Compra
	, RC.Monedero_Id AS Identidad
	--, RC.Cliente_Nombre
	, RC.Articulo_Id
	, RC.Vendedor_Id
	--, RC.Articulo_Nombre
	--, RC.Cantidad_Recetada
	, RC.Presentacion AS Presentacion
	, RC.Consumo_Diario_Promedio
    , RC.UltimaCompra_Presentacion Comprado_Presentacion
	, RC.Patologias_Nombre
    , RC.Contactar_Estimado_El AS Contactar_El
	--, RC.Indicacion_Receta
	--, COUNT(*) OVER (PARTITION BY RC.Pais_Id, RC.Sucursal_Id ) AS Clientes_Sucursal
FROM	DL_FARINTER.[dbo].[VDL_Kielsa_LibrosCalculosClienteArticulo] RC
WHERE RC.Indicador_A_Tiempo = 1
	AND RC.Contactar_Estimado_El >= CONVERT(DATE, GETDATE())
	AND RC.Contactar_Estimado_El < CONVERT(DATE, GETDATE() + 1)
	AND RC.Fecha_Ultima_Compra >= DATEADD(MONTH, -12, GETDATE())
    AND RC.Indicador_Tratamiento_Completo = 0
),
LBReparticion AS
(
SELECT LB.*
	, DENSE_RANK() OVER (PARTITION BY LB.Emp_Id, LB.Identidad ORDER BY LB.Vendedor_Id) AS Orden_Vendedor_Identidad
FROM LibrosBase LB
)

--SELECT * FROM LB ORDER BY Pais_Id, Sucursal_Id, Identidad_Sucursal_Orden
SELECT
	ISNULL(LB.Emp_Id, 0) Emp_Id
	, ISNULL(LB.Pais_Id, 0) Pais_Id
	, ISNULL(LB.Sucursal_Id, 0) Sucursal_Id
	, S.Sucursal_Nombre
	, LB.Fecha_Compra
	, ISNULL(LB.Identidad, '') Identidad
	, M.Monedero_Nombre Cliente_Nombre
	, ISNULL(LB.Articulo_Id,'') Articulo_Id
	, A.Articulo_Nombre
	, LB.Presentacion
	, LB.Consumo_Diario_Promedio
	--, LB.Cantidad_Recetada
	, cast(LB.Comprado_Presentacion as decimal(18,4)) Comprado_Presentacion
	--, LB.Indicacion_Receta
	--, LB.Patologias_Nombre
	, LB.Vendedor_Id
	, V.Empleado_Nombre
    , ISNULL(LB.Contactar_El,'19000101') Contactar_El
FROM	LBReparticion LB
LEFT JOIN DL_fARINTER.dbo.DL_Kielsa_Empleado V
	ON LB.Vendedor_Id = V.Empleado_Id AND LB.Emp_Id = V.Emp_Id
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Sucursal S
	ON LB.Sucursal_Id = S.Sucursal_Id AND LB.Emp_Id = S.Emp_Id
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Monedero M
	ON LB.Identidad = M.Monedero_Id  AND LB.Emp_Id = M.Emp_Id
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Articulo A
	ON LB.Articulo_Id = A.Articulo_Id AND LB.Emp_Id = A.Emp_Id
LEFT JOIN CRM_Kielsa_RecetasContactarHoy RCH
	ON LB.Emp_Id = RCH.Pais_Id AND LB.Sucursal_Id = RCH.Sucursal_Id AND LB.Identidad = RCH.Identidad
WHERE LB.Orden_Vendedor_Identidad =1 AND RCH.Identidad IS NULL
--AND LB.Identidad = '0501196909896'

/*
select * from(
select  Identidad, COUNT(DISTINCT Vendedor_Id) as total
from CRM_FARINTER.[dbo].[CRM_Kielsa_LibrosContactarHoy]
group by  Identidad ) as A where total > 1

SELECT count(*) FROM [dbo].[CRM_Kielsa_LibrosContactarHoy]

SELECT * 
INTO CRM_Kielsa_LibrosContactarHoy_Temp
FROM [dbo].[CRM_Kielsa_LibrosContactarHoy]

select * from(
select  Identidad, COUNT(DISTINCT Sucursal_Id) as total
from CRM_FARINTER.[dbo].[CRM_Kielsa_LibrosContactarHoy]
group by  Identidad ) as A where total > 1
*/