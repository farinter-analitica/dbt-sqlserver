{% set unique_key_list = ["ArticuloPadre_Id", "Departamento_Id", "Municipio_Id", "Ciudad_Id", "Emp_Id", "TipoBodega_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		on_schema_change="append_new_columns",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      	"{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

SELECT --TOP (1000) 
	ISNULL(EH.[Emp_Id],0) AS Emp_Id,
	ISNULL(EH.[ArticuloPadre_Id],0) AS ArticuloPadre_Id,
	ISNULL(B.[TipoBodega_Id],0) AS TipoBodega_Id,
	ISNULL(s.[Departamento_Id],0) AS Departamento_Id,
	ISNULL(s.[Municipio_Id],0) AS Municipio_Id,
	ISNULL(s.[Ciudad_Id],0) AS Ciudad_Id,
	MAX(s.[Zona_Id]) AS Zona_Id,
	{{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'ArticuloPadre_Id'], input_length=49, table_alias='EH')}} AS EmpArt_Id,
	MAX(S.[EmpZona_Id]) AS EmpZona_Id,
	MAX(S.[EmpDep_Id]) AS EmpDep_Id,
	MAX(S.[EmpDepMun_Id]) AS EmpDepMun_Id,
	MAX(S.[EmpDepMunCiu_Id]) AS EmpDepMunCiu_Id,
	MAX(EH.[Stock_Id]) AS Stock_Id,
	SUM(EH.[CantidadPadre_Existencia]) AS Cantidad_Existencia,
	SUM(EH.[Valor_Existencia]) AS Valor_Existencia,
	MAX(EH.[Fecha_Actualizado]) AS Fecha_Actualizado
FROM [DL_FARINTER].[dbo].[DL_Kielsa_ExistenciaHist] EH -- {{ source ('DL_FARINTER', 'DL_Kielsa_ExistenciaHist') }}
INNER JOIN {{ ref('BI_Kielsa_Dim_Bodega')}} B
	ON EH.Bodega_Id = B.Bodega_Id 
	AND EH.Emp_Id = B.Emp_Id
	AND EH.Sucursal_Id = B.Sucursal_Id
INNER JOIN {{ ref('BI_Kielsa_Dim_Sucursal')}} S
	ON EH.Sucursal_Id = S.Sucursal_Id AND EH.Emp_Id = S.Emp_Id
WHERE AnioMes_Id = YEAR(GETDATE())*100 + MONTH(GETDATE())
GROUP BY EH.[Emp_Id],
	B.[TipoBodega_Id],
	EH.[ArticuloPadre_Id],
	S.[Departamento_Id],
	S.[Municipio_Id],
	S.[Ciudad_Id]
