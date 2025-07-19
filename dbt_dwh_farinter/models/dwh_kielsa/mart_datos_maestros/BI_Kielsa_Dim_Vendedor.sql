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

rol_icentivos AS (
    SELECT
        kr.id,
        kr.emp_id,
        kr.rol_id_ld AS rol_id_original,
        kr.nombre AS rol_nombre_original,
        COALESCE(krm.rol_id_ld, kr.rol_id_ld) AS Rol_Id_Mapeado,
        COALESCE(krm.nombre, kr.nombre) AS rol_nombre_final
    FROM {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} AS kr
    LEFT JOIN {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_rol') }} AS krm
        ON kr.mapear_a_id = krm.id AND kr.emp_id = krm.emp_id
),

cte_vendedor_x_usuario AS (
    SELECT
        *,
        COUNT(Usuario_Id) OVER (PARTITION BY Vendedor_Id, Emp_Id) AS Cantidad_Usuarios
    FROM {{ ref('DL_Kielsa_Vendedor_x_Usuario') }}
),

cte_usuario_x_rol AS (
    SELECT
        *,
        COUNT(Rol_Id) OVER (PARTITION BY Usuario_Id, Emp_Id) AS Cantidad_Roles
    FROM {{ ref('DL_Kielsa_Seg_Usuario_x_Rol') }}
),

usuario_rol AS (
    SELECT
        VU.Emp_Id,
        VU.Vendedor_Id,
        UR.Usuario_Id,
        U.Usuario_Eliminado,
        U.Usuario_Cuenta_Deshabilitada,
        UR.Cantidad_Roles,
        VU.Cantidad_Usuarios,
        UR.Rol_Id,
        COALESCE(ri.Rol_Id_Mapeado, UR.Rol_Id) AS Rol_Id_Mapeado,
        ROW_NUMBER() OVER (
            PARTITION BY VU.Emp_Id, VU.Vendedor_Id
            ORDER BY kr.profundidad ASC, UR.Fec_Actualizacion DESC
        ) AS Fila
    FROM cte_vendedor_x_usuario AS VU
    INNER JOIN cte_usuario_x_rol AS UR ON VU.Usuario_Id = UR.Usuario_Id AND VU.Emp_Id = UR.Emp_Id
    INNER JOIN {{ ref('DL_Kielsa_Seg_Usuario') }} AS U
        ON
            UR.Usuario_Id = U.Usuario_Id
            AND UR.Emp_Id = U.Emp_Id
    LEFT JOIN rol_icentivos AS ri
        ON
            UR.Rol_Id = ri.rol_id_original
            AND UR.Emp_Id = ri.emp_id
    LEFT JOIN {{ ref('dlv_kielsa_incentivo_rol_jerarquia') }} AS kr
        ON
            UR.Rol_Id = kr.rol_id_ld
            AND UR.Emp_Id = kr.emp_id

    WHERE
        U.Usuario_Eliminado = 0
        AND U.Usuario_Cuenta_Deshabilitada = 0
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
        CASE
            WHEN bv.Emp_Id = 5
                THEN COALESCE(rn_meta.Rol_Id, ur.Rol_Id_Mapeado, 0)
            ELSE COALESCE(ur.Rol_Id_Mapeado, rn_meta.Rol_Id, 0)
        END
            AS [Rol_Id_Mapeado],
        ISNULL(ur.Usuario_Id, 0) AS [Usuario_Id],
        CASE
            WHEN bv.Emp_Id = 5
                THEN COALESCE(rn_meta.Rol_Nombre, rn.Rol_Nombre, 'Sin_Rol')
            ELSE COALESCE(rnm.Rol_Nombre, rn_meta.Rol_Nombre, 'Sin_Rol')
        END
            AS [Rol_Nombre_Mapeado],
        ISNULL(ur.Cantidad_Roles, 0) AS [Cantidad_Roles],
        ISNULL(ur.Cantidad_Usuarios, 0) AS [Cantidad_Usuarios],
        ISNULL(mh.Sucursal_Id_Asignado_Meta, NULL) AS [Sucursal_Id_Asignado_Meta],
        ISNULL(ur.Usuario_Eliminado, 0) AS [Bit_Borrado],
        CASE WHEN ur.Usuario_Cuenta_Deshabilitada = 1 THEN 0 ELSE 1 END AS [Bit_Activo],
        COALESCE(sv.Sucursal_Id_Vendedor, mh.Sucursal_Id_Asignado_Meta, sf.Sucursal_Id_Ultima_Factura) AS [Sucursal_Id_Asignado]
    FROM base_vendedor AS bv
    LEFT JOIN usuario_rol AS ur ON bv.Vendedor_Id = ur.Vendedor_Id AND bv.Emp_Id = ur.Emp_Id AND ur.Fila = 1
    LEFT JOIN rol_nombre AS rn ON ur.Rol_Id = rn.Rol_Id AND bv.Emp_Id = rn.Emp_Id
    LEFT JOIN rol_nombre AS rnm ON ur.Rol_Id_Mapeado = rnm.Rol_Id AND bv.Emp_Id = rnm.Emp_Id
    LEFT JOIN meta_hist AS mh ON bv.Vendedor_Id = mh.Vendedor_Id AND bv.Emp_Id = mh.Emp_Id
    LEFT JOIN sucursal_factura AS sf ON bv.Emp_Id = sf.Emp_Id AND bv.Vendedor_Id = sf.Vendedor_Id AND sf.fila = 1
    LEFT JOIN sucursal_vendedor AS sv ON bv.Emp_Id = sv.Emp_Id AND bv.Vendedor_Id = sv.Vendedor_Id AND sv.fila = 1
    LEFT JOIN rol_nombre AS rn_meta ON mh.Vendedor_Rol = rn_meta.Rol_Nombre AND bv.Emp_Id = rn_meta.Emp_Id
)

SELECT
    Vendedor_Id,
    Sucursal_Id_Asignado,
    Emp_Id,
    Usuario_Id,
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
