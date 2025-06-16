{# TODO: Activar carga con la nueva version dbt 1.9#}
{% snapshot BI_Kielsa_Dim_UsuarioSucursal_scd %}
    {{
        config(
            tags=["periodo/diario", "detener_carga/si"] , 
            target_schema='dbt_snapshot',
            strategy='check',
            unique_key= "EmpSucUsu_Id" ,
            check_cols=['Rol_Id', 'Vendedor_Id'],
        )
    }}
--Source SCRIPT cannot have identity columns
    SELECT [Usuario_Id]
        ,[Suc_Id]
        ,[Emp_Id]
        ,[Rol_Sucursal]
        ,[Rol_Id]
        ,[Rol_Nombre]
        ,[Rol_Jerarquia]
        ,[Vendedor_Id]
        ,[Usuario_Nombre]
        ,[Bit_Activo]
        ,[Rol_Fec_Actualizacion]
        ,[Fecha_Actualizado]
        ,[EmpSuc_Id]
        ,[EmpSucUsu_Id]
        ,[EmpRol_Id]
        ,[EmpVen_Id]
    FROM [BI_FARINTER].[dbo].[BI_Kielsa_Dim_UsuarioSucursal]
{% endsnapshot %}

