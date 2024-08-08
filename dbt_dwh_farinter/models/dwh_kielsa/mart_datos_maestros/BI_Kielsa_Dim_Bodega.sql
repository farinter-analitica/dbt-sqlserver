
{% set unique_key_list = ["Sucursal_Id","Bodega_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario", "periodo/por_hora"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}

SELECT
	*
	, ABS(CAST(HASHBYTES('SHA1', CONCAT(A.Sucursal_Id, '-', A.Bodega_Id, '-', A.Emp_Id)) AS BIGINT)) AS Hash_SucursalBodegaEmp
    , ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_SucBodEmp]
FROM
	(SELECT --TOP (1000) 
		ISNULL(B.Suc_Id,0) AS [Sucursal_Id]
		, ISNULL(B.Bodega_Id, 0) AS [Bodega_Id]
		, ISNULL(B.Emp_Id,0) AS [Emp_Id]
		, [Bodega_Nombre]
		, CASE
			WHEN B.Bodega_Nombre LIKE '%VENCIDO%'
				THEN 'VENCIDOS'
			WHEN B.Bodega_Nombre LIKE '%BLOQUEO%'
				THEN 'BLOQUEO'
			WHEN B.Bodega_Nombre LIKE '%CALIDAD%'
				THEN 'CALIDAD'
			WHEN B.Bodega_Nombre LIKE '%VENTA%'
				THEN 'VENTAS'
			WHEN B.Bodega_Nombre LIKE '%SUMINISTRO%'
				THEN 'SUMINISTROS'
			WHEN B.Bodega_Nombre LIKE '%EQUIPO%MOB%' OR B.Bodega_Nombre LIKE '%MOBILIARIO%'
				THEN 'EQUIPO Y MOBILIARIO'
			WHEN B.Bodega_Nombre LIKE '%CANJE%'
				THEN 'CANJES'
			WHEN B.Bodega_Nombre LIKE '%CORTO%VENCE%'
				THEN 'CORTO VENCE'
			WHEN B.Bodega_Nombre LIKE '%MUESTRA%'
				THEN 'MUESTRAS'
			WHEN B.Bodega_Nombre LIKE '%UNIFORME%'
				THEN 'UNIFORMES'
			WHEN B.Bodega_Nombre LIKE '%MATERIAL%OFIC%%'
				THEN 'MATERIAL DE OFICINA'
			WHEN B.Bodega_Nombre LIKE '%PROVEEDURIA%'
				THEN 'PROVEEDURIA - MATERIAL ADMON'
			WHEN B.Bodega_Nombre LIKE '%MERCAD%'
				THEN 'MERCADEO'
			WHEN B.Bodega_Nombre LIKE '%LIBRERIA%'
				THEN 'LIBRERIA'
			WHEN B.Bodega_Nombre LIKE '%PRODUCTO%DA%ADO%'
				THEN 'PRODUCTO DAÑADO'
			ELSE 'OTRAS'
		END AS Bodega_Tipo
		, B.[Bodega_Responsable]
		, S.[Sucursal_Nombre]
		, S.[Marca]
		, S.[Zona_Id]
		, S.[Zona_Nombre]
		, S.[Departamento_Id]
		, S.[Departamento_Nombre]
		, S.[Municipio_Id]
		, S.[Municipio_Nombre]
		, S.[Ciudad_Id]
		, S.[Ciudad_Nombre]
		, S.[TipoSucursal_Id]
		, S.[TipoSucursal_Nombre]
		, S.[Direccion]
		, S.[Estado]
		, S.JOP
		, S.[Supervisor]
		, S.[Longitud]
		, S.[Latitud]
		, CASE
			WHEN B.Bodega_Nombre LIKE '%VENCIDO%'
				THEN 2
			WHEN B.Bodega_Nombre LIKE '%BLOQUEO%'
				THEN 4
			WHEN B.Bodega_Nombre LIKE '%CALIDAD%'
				THEN 3
			WHEN B.Bodega_Nombre LIKE '%VENTA%'
				THEN 1
			WHEN B.Bodega_Nombre LIKE '%SUMINISTRO%'
				THEN 10
			WHEN B.Bodega_Nombre LIKE '%EQUIPO%MOB%' OR B.Bodega_Nombre LIKE '%MOBILIARIO%'
				THEN 11
			WHEN B.Bodega_Nombre LIKE '%CANJE%'
				THEN 5
			WHEN B.Bodega_Nombre LIKE '%CORTO%VENCE%'
				THEN 6
			WHEN B.Bodega_Nombre LIKE '%MUESTRA%'
				THEN 7
			WHEN B.Bodega_Nombre LIKE '%UNIFORME%'
				THEN 12
			WHEN B.Bodega_Nombre LIKE '%MATERIAL%OFIC%%'
				THEN 13
			WHEN B.Bodega_Nombre LIKE '%PROVEEDURIA%'
				THEN 14
			WHEN B.Bodega_Nombre LIKE '%MERCAD%'
				THEN 15
			WHEN B.Bodega_Nombre LIKE '%LIBRERIA%'
				THEN 16
			WHEN B.Bodega_Nombre LIKE '%PRODUCTO%DA%ADO%'
				THEN 8
			ELSE 99
		END AS [TipoBodega_Id]
        , S.HashStr_SucEmp
	FROM {{source ('DL_FARINTER', 'DL_Kielsa_Bodega')}} B
	INNER JOIN {{ref ('BI_Kielsa_Dim_Sucursal')}}  S
		ON B.[Suc_Id] = S.[Sucursal_Id] AND B.[Emp_Id] = S.[Emp_Id]) A