
{% set unique_key_list = ["Usuario_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo/por_hora"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
			"{{ dwh_farinter_remove_incremental_temp_table() }}",
			"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Usuario_Login', 'Emp_Id'], create_unique=true) }}",
			"{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
) }}

WITH VendedorAsignacion AS (
    SELECT 
        VxU.Usuario_Id,
        VxU.Emp_Id,
        VxU.Vendedor_Id,
        VxU.Ult_Fec_Actualizacion,
        -- Use ROW_NUMBER to get latest assignment per user
        ROW_NUMBER() OVER (
            PARTITION BY VxU.Usuario_Id, VxU.Emp_Id 
            ORDER BY VxU.Ult_Fec_Actualizacion DESC
        ) as rn
    FROM {{ ref("DL_Kielsa_Vendedor_x_Usuario") }} VxU
)
SELECT 
    U.Usuario_Id,
    U.Emp_Id,
	CONCAT(U.Emp_Id,'-',U.Usuario_Id) AS EmpUsu_Id
    U.Usuario_Nombre,
    U.Usuario_Login,
	CONCAT(U.Emp_Id,'-',U.Usuario_Login) AS EmpLogin_Id
    U.Usuario_Email,
    LVA.Vendedor_Id as Ultimo_Vendedor_Id_Asignado,
    LVA.Ult_Fec_Actualizacion as Fecha_Actualizado
FROM {{ ref("DL_Kielsa_Seg_Usuario") }} U
LEFT JOIN VendedorAsignacion LVA ON  
    U.Usuario_Id = LVA.Usuario_Id 
    AND U.Emp_Id = LVA.Emp_Id
    AND LVA.rn = 1
