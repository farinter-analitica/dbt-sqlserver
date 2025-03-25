{%- set unique_key_list = ["Articulo_Id","Emp_Id"]  -%}
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

WITH 
Articulos_Recomendables as
(
    SELECT Emp_Id,
        Articulo_Id,
        Articulo_Nombre
    FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo A
    WHERE A.SubCategoria2Art_Nombre NOT IN (
            'ALIMENTOS',
            'BATERIAS Y ENCENDEDORES',
            'BEBE ACCES',
            'BEBIDAS',
            'BELLEZA ACCES',
            'COSMETICOS ACCES',
            'DECORACION/UTIL HOG',
            'DULCES/SNACKS',
            'EQUIPO/MATERIAL MED',
            'HIGIENE DEL HOG',
            'LIBRERIA',
            'MAMA ACCES',
            'OTROS DONACIONES',
            'PAQUETES CLARO',
            'PAQUETES TIGO',
            'PRESENCIAL',
            'PRIMEROS AUXILIOS',
            'PROMOCIONALES',
            'RECARGAS CLARO',
            'RECARGAS TIGO',
            'SIM CARD TIGO',
            'TARJETA TENGO',
            'TECNOLOGÍA'
        )
)
SELECT *, GETDATE() AS Fecha_Actualizado
FROM Articulos_Recomendables
