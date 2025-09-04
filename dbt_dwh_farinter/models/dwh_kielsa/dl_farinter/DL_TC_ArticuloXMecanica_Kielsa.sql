{% set unique_key_list = ["Articulo_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      	"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['MecanicaCanje_Id','Articulo_Id'], included_columns=['Inicio','Final']) }}",
        ]
	) 
}}

SELECT
    B.Fecha_Inicio AS Inicio,
    B.Fecha_Final AS Final,
    B.Activa AS Estado,
    ISNULL(CONCAT(A.Emp_Id, '-', B.Alerta_Id), 'X') AS MecanicaCanje_Id,
    ISNULL(A.Articulo_Id, '') AS Articulo_Id,
    ISNULL(A.Alerta_Id, 0) AS Alerta_Id,
    ISNULL(A.Emp_Id, 0) AS Emp_Id,
    GETDATE() AS Fecha_Actualizado
FROM
    (
        SELECT
            A.Emp_Id,
            A.Articulo_Id,
            B.Alerta_Id AS [Alerta_Id],
            B.Activa AS [activa],
            ROW_NUMBER() OVER (PARTITION BY A.Articulo_Id, A.Emp_Id ORDER BY B.Fecha_Final DESC) AS rn
        FROM {{ source ('DL_FARINTER', 'DL_Kielsa_Articulo_Alerta') }} AS A
        INNER JOIN {{ source ('DL_FARINTER', 'DL_Kielsa_PV_Alerta') }} AS B
            ON A.Alerta_Id = B.Alerta_Id AND A.Emp_Id = B.Emp_Id
        WHERE (SUBSTRING(B.Nombre, 1, 5) = 'CANJE' OR B.Nombre LIKE '%CANJE%') --AND A.Emp_Id=1
        -- and B.Alerta_Id <> 4
        AND B.Activa = 1
    ) AS A
INNER JOIN {{ source ('DL_FARINTER', 'DL_Kielsa_PV_Alerta') }} AS B
    ON A.Alerta_Id = B.Alerta_Id AND A.Emp_Id = B.Emp_Id
WHERE A.rn = 1
