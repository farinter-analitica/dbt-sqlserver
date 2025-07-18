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

usuario_rol AS (
    SELECT
        VU.Emp_Id,
        VU.Vendedor_Id,
        UR.Usuario_Id,
        UR.Rol_Id,
        U.Usuario_Eliminado,
        U.Usuario_Cuenta_Deshabilitada,
        COUNT(UR.Rol_Id) OVER (PARTITION BY UR.Usuario_Id) AS Cantidad_Roles,
        COUNT(VU.Usuario_Id) OVER (PARTITION BY VU.Vendedor_Id) AS Cantidad_Usuarios,
        ROW_NUMBER() OVER (PARTITION BY VU.Emp_Id, VU.Vendedor_Id ORDER BY UR.Fec_Actualizacion DESC) AS Fila
    FROM {{ ref('DL_Kielsa_Vendedor_x_Usuario') }} AS VU
    INNER JOIN {{ ref('DL_Kielsa_Seg_Usuario_x_Rol') }} AS UR ON VU.Usuario_Id = UR.Usuario_Id AND VU.Emp_Id = UR.Emp_Id
    INNER JOIN {{ ref('DL_Kielsa_Seg_Usuario') }} AS U ON UR.Usuario_Id = U.Usuario_Id AND UR.Emp_Id = U.Emp_Id
    WHERE U.Usuario_Eliminado = 0 AND U.Usuario_Cuenta_Deshabilitada = 0
),

meta_hist_ult AS (
    SELECT
        Emp_Id,
        MAX(AnioMes_Id) AS AnioMes_Id
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_MetaHist') }}
    GROUP BY Emp_Id
),

meta_hist AS (
    SELECT
        MH.Emp_Id,
        MH.Empleado_Id AS Vendedor_Id,
        MAX(MH.Empleado_Rol) AS Vendedor_Rol,
        MAX(MH.Sucursal_Id) AS Sucursal_Id_Asignado_Meta
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_MetaHist') }} AS MH
    INNER JOIN meta_hist_ult AS Ult ON MH.AnioMes_Id = Ult.AnioMes_Id AND MH.Emp_Id = Ult.Emp_Id
    WHERE MH.Empleado_Rol IS NOT NULL
    GROUP BY MH.Emp_Id, MH.Empleado_Id
),

sucursal_vendedor AS (
    SELECT
        Emp_Id,
        Vendedor_Id,
        Suc_Id AS Sucursal_Id_Vendedor,
        ROW_NUMBER() OVER (PARTITION BY Emp_Id, Vendedor_Id ORDER BY Vendedor_Fec_Actualizacion DESC) AS fila
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_VendedorSucursal') }}
),

rol_nombre AS (
    SELECT
        Rol_Id,
        Emp_Id,
        Rol_Nombre
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_Seg_Rol') }}
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
)

SELECT
    bv.Vendedor_Id,
    bv.Emp_Id,
    CAST(bv.Vendedor_Nombre AS VARCHAR(50)) AS [Vendedor_Nombre],
    ISNULL(ur.Rol_Id, 0) AS [Rol_Id],
    ISNULL(ur.Usuario_Id, 0) AS [Usuario_Id],
    ISNULL(rn.Rol_Nombre, 'Sin_Rol') AS [Rol_Nombre],
    ISNULL({{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Vendedor_Id'], input_length=30, table_alias='bv') }}, 0) AS [Hash_EmpVen],
    ISNULL(ur.Cantidad_Roles, 0) AS [Cantidad_Roles],
    ISNULL(ur.Cantidad_Usuarios, 0) AS [Cantidad_Usuarios],
    ISNULL(mh.Sucursal_Id_Asignado_Meta, NULL) AS [Sucursal_Id_Asignado_Meta],
    ISNULL(ur.Usuario_Eliminado, 0) AS [Bit_Borrado],
    CASE WHEN ur.Usuario_Cuenta_Deshabilitada = 1 THEN 0 ELSE 1 END AS [Bit_Activo],
    COALESCE(sv.Sucursal_Id_Vendedor, mh.Sucursal_Id_Asignado_Meta, sf.Sucursal_Id_Ultima_Factura) AS [Sucursal_Id_Asignado]
FROM base_vendedor AS bv
LEFT JOIN usuario_rol AS ur ON bv.Vendedor_Id = ur.Vendedor_Id AND bv.Emp_Id = ur.Emp_Id AND ur.Fila = 1
LEFT JOIN rol_nombre AS rn ON ur.Rol_Id = rn.Rol_Id AND bv.Emp_Id = rn.Emp_Id
LEFT JOIN meta_hist AS mh ON bv.Vendedor_Id = mh.Vendedor_Id AND bv.Emp_Id = mh.Emp_Id
LEFT JOIN sucursal_factura AS sf ON bv.Emp_Id = sf.Emp_Id AND bv.Vendedor_Id = sf.Vendedor_Id AND sf.fila = 1
LEFT JOIN sucursal_vendedor AS sv ON bv.Emp_Id = sv.Emp_Id AND bv.Vendedor_Id = sv.Vendedor_Id AND sv.fila = 1
