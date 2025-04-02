
{% set unique_key_list = ["Empleado_Id","Emp_Id"] %}
{% set no_definido = "'No Definido'" %}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","automation/periodo_por_hora"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(
				unique_key=" ~ unique_key_list | tojson ~ ", 
				is_incremental=0,
				custom_column_values={'Emp_Id':1,'Empleado_Nombre': " ~ no_definido | tojson ~ "},
			) }}"
		]
		
) }}

SELECT
	*
	, {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Empleado_Id'], input_length=29, table_alias='')}} [EmpEmpl_Id]
    , ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_EmplEmp]
FROM (
		SELECT [Empleado_Id],
			[Empleado_Nombre],
			[Empleado_Id] AS [Vendedor_Id],
			CAST([Empleado_Nombre] AS VARCHAR(50)) AS [Vendedor_Nombre],
			[Rol_Id],
			[Usuario_Id],
			[Rol],
			[Emp_Id],
			[Hash_EmpleadoEmp],
			[Sucursal_Id_Asignado_Meta],
			COALESCE(
				[Sucursal_Id_Asignado_Meta],
				[Sucursal_Id_Ultima_Factura]
			) AS [Sucursal_Id_Asignado],
			[Bit_Activo]
		FROM {{ source ('DL_FARINTER', 'DL_Kielsa_Empleado') }}
	) A