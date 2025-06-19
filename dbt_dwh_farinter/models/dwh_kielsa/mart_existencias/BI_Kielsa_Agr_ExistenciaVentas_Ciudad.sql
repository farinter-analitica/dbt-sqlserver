{% set unique_key_list = ["Articulo_Id", "Departamento_Id", "Municipio_Id", "Ciudad_Id", "Emp_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
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

SELECT ISNULL(EC.Emp_Id,0) AS Emp_Id,	
    ISNULL(EC.ArticuloPadre_Id,0) AS Articulo_Id,
    ISNULL(EC.Departamento_Id,0) AS Departamento_Id,
    ISNULL(EC.Municipio_Id,0) AS Municipio_Id,
    ISNULL(EC.Ciudad_Id,0) AS Ciudad_Id,
    ISNULL(EC.Cantidad_Existencia,0) AS Cantidad_Existencia
FROM BI_FARINTER.dbo.BI_Kielsa_Agr_Existencia_Ciudad EC --{{ ref('BI_Kielsa_Agr_Existencia_Ciudad') }}
INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_TipoBodega TB --{{ ref('BI_Kielsa_Dim_TipoBodega') }}
    ON EC.Emp_Id = TB.Emp_Id
    AND EC.TipoBodega_Id = TB.TipoBodega_Id
WHERE TipoBodega_Nombre = 'VENTAS' AND EC.Cantidad_Existencia > 0