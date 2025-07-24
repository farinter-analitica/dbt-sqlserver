{% set unique_key_list = ["Vendedor_Id","Emp_Id"] %}
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

WITH base_vendedor AS (
    SELECT
        Emp_Id,
        Vendedor_Id,
        Vendedor_Nombre_Completo AS Vendedor_Nombre
    FROM {{ ref('DL_Kielsa_Vendedor') }}
),

sucursal_vendedor AS (
    SELECT
        Emp_Id,
        Vendedor_Id,
        Suc_Id AS Sucursal_Id_Vendedor,
        COUNT(*) OVER (
            PARTITION BY Emp_Id, Vendedor_Id
        ) AS Cantidad_Sucursales,
        ROW_NUMBER() OVER (PARTITION BY Emp_Id, Vendedor_Id ORDER BY Vendedor_Fec_Actualizacion DESC) AS fila
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_VendedorSucursal') }}
),

usuario_rol AS (
    SELECT
        U.Emp_Id,
        U.Vendedor_Id,
        U.Usuario_Id,
        U.Bit_Borrado,
        U.Bit_Activo,
        U.Cantidad_Roles,
        U.Rol_Id,
        U.Rol_Id_Mapeado,
        U.Rol_Nombre_Mapeado,
        U.Sucursal_Id_Asignado_Meta,
        COUNT(*) OVER (
            PARTITION BY U.Emp_Id, U.Vendedor_Id
        ) AS Cantidad_Usuarios,
        ROW_NUMBER() OVER (
            PARTITION BY U.Emp_Id, U.Vendedor_Id
            ORDER BY U.Bit_Activo DESC, U.Fecha_Actualizado DESC
        ) AS Fila
    FROM {{ ref('BI_Kielsa_Dim_Usuario') }} AS U
),

rol_nombre AS (
    SELECT
        r.Emp_Id,
        r.Rol_Id,
        r.Rol_Nombre
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_Seg_Rol') }} AS r
),

sucursal_factura AS (
    SELECT
        Emp_Id,
        Vendedor_Id,
        Suc_Id AS Sucursal_Id_Ultima_Factura,
        MAX(Factura_Fecha) AS Fecha_Ultima_Factura,
        ROW_NUMBER() OVER (PARTITION BY Emp_Id, Vendedor_Id ORDER BY MAX(Factura_Fecha) DESC) AS fila
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado') }}
    GROUP BY Emp_Id, Vendedor_Id, Suc_Id
),

Final AS (
    SELECT -- noqa: ST06
        bv.Vendedor_Id,
        bv.Emp_Id,
        CAST(bv.Vendedor_Nombre AS VARCHAR(50)) AS [Vendedor_Nombre],
        ur.Rol_Id,
        rn.Rol_Nombre,
        ur.[Rol_Id_Mapeado],
        ISNULL(ur.Usuario_Id, 0) AS [Usuario_Id],
        ur.[Rol_Nombre_Mapeado],
        ISNULL(ur.Cantidad_Roles, 0) AS [Cantidad_Roles],
        ISNULL(ur.Cantidad_Usuarios, 0) AS [Cantidad_Usuarios],
        ISNULL(ur.Sucursal_Id_Asignado_Meta, NULL) AS [Sucursal_Id_Asignado_Meta],
        ISNULL(ur.Bit_Borrado, 0) AS [Bit_Borrado],
        ISNULL(ur.Bit_Activo, 0) AS [Bit_Activo],
        COALESCE(sv.Sucursal_Id_Vendedor, ur.Sucursal_Id_Asignado_Meta, sf.Sucursal_Id_Ultima_Factura) AS [Sucursal_Id_Asignado],
        sv.Cantidad_Sucursales AS [Cantidad_Sucursales_Asignadas_LD]
    FROM base_vendedor AS bv
    LEFT JOIN usuario_rol AS ur ON bv.Vendedor_Id = ur.Vendedor_Id AND bv.Emp_Id = ur.Emp_Id AND ur.Fila = 1
    LEFT JOIN rol_nombre AS rn ON ur.Rol_Id = rn.Rol_Id AND bv.Emp_Id = rn.Emp_Id
    LEFT JOIN sucursal_factura AS sf ON bv.Emp_Id = sf.Emp_Id AND bv.Vendedor_Id = sf.Vendedor_Id AND sf.fila = 1
    LEFT JOIN sucursal_vendedor AS sv ON bv.Emp_Id = sv.Emp_Id AND bv.Vendedor_Id = sv.Vendedor_Id AND sv.fila = 1
)

SELECT
    Vendedor_Id,
    Emp_Id,
    Usuario_Id,
    Sucursal_Id_Asignado_Meta,
    Sucursal_Id_Asignado,
    ISNULL({{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Usuario_Id'], input_length=30, table_alias='') }}, 0) AS [EmpUsu_Id],
    ISNULL({{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Vendedor_Id'], input_length=30, table_alias='') }}, 0) AS [EmpVen_Id],
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Sucursal_Id_Asignado"], input_length=99) }} AS EmpSuc_Id,
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Sucursal_Id_Asignado","Vendedor_Id"], input_length=99) }} AS EmpSucVen_Id,
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Rol_Id"], input_length=99) }} AS EmpRol_Id,
    Vendedor_Nombre,
    Rol_Id,
    Rol_Nombre,
    Rol_Id_Mapeado,
    Rol_Nombre_Mapeado,
    Cantidad_Roles,
    Cantidad_Usuarios,
    Bit_Borrado,
    Bit_Activo,
    Fecha_Actualizado = GETDATE()
FROM Final
