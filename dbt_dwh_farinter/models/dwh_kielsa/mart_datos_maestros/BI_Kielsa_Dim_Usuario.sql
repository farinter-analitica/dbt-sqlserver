{% set unique_key_list = ["Usuario_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
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
        COUNT(VxU.Vendedor_Id) OVER (PARTITION BY VxU.Usuario_Id, VxU.Emp_Id) AS Cantidad_Vendedores,
        -- Use ROW_NUMBER to get latest assignment per user
        ROW_NUMBER() OVER (
            PARTITION BY VxU.Usuario_Id, VxU.Emp_Id
            ORDER BY VxU.Ult_Fec_Actualizacion DESC
        ) AS Fila
    FROM {{ ref("DL_Kielsa_Vendedor_x_Usuario") }} AS VxU
),

Usuario_Sucursal AS (
    SELECT
        V.Emp_Id,
        V.Usuario_Id,
        V.Suc_Id,
        COUNT(*) OVER (PARTITION BY V.Emp_Id, V.Usuario_Id) AS Cantidad_Sucursales,
        ROW_NUMBER() OVER (PARTITION BY V.Emp_Id, V.Usuario_Id ORDER BY V.SxU_Fec_Actualizacion DESC) AS Fila
    FROM {{ ref("DL_Kielsa_Seg_Sucursal_x_Usuario") }} AS V
),

Ultima_Factura AS (
    SELECT
        Emp_Id,
        Usuario_Id,
        Suc_Id,
        MAX(Factura_Fecha) AS Fecha_Ultima_Factura,
        ROW_NUMBER() OVER (PARTITION BY Emp_Id, Usuario_Id ORDER BY MAX(Factura_Fecha) DESC) AS fila
    FROM DL_FARINTER.dbo.DL_Kielsa_FacturaEncabezado -- {{ source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado') }}
    WHERE AnioMes_Id >= (YEAR(GETDATE()) - 1) * 100 + MONTH(GETDATE())
    GROUP BY Emp_Id, Usuario_Id, Suc_Id
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

cte_usuario_x_rol AS (
    SELECT
        *,
        COUNT(Rol_Id) OVER (PARTITION BY Usuario_Id, Emp_Id) AS Cantidad_Roles
    FROM {{ ref('DL_Kielsa_Seg_Usuario_x_Rol') }}
),

Roles AS (
    SELECT
        UR.Emp_Id,
        UR.Usuario_Id,
        UR.Rol_Id,
        UR.Cantidad_Roles,
        UR.Fec_Actualizacion,
        COALESCE(ri.Rol_Id_Mapeado, UR.Rol_Id) AS Rol_Id_Mapeado,
        ROW_NUMBER() OVER (
            PARTITION BY UR.Emp_Id, UR.Usuario_Id
            ORDER BY kr.profundidad ASC, UR.Fec_Actualizacion DESC
        ) AS Fila
    FROM cte_usuario_x_rol AS UR
    LEFT JOIN rol_icentivos AS ri
        ON
            UR.Rol_Id = ri.rol_id_original
            AND UR.Emp_Id = ri.emp_id
    LEFT JOIN {{ ref('dlv_kielsa_incentivo_rol_jerarquia') }} AS kr
        ON
            UR.Rol_Id = kr.rol_id_ld
            AND UR.Emp_Id = kr.emp_id
    --where ur.usuario_id = 709
),

Metas AS (
    SELECT
        MH.Emp_id,
        MH.Empleado_Id AS Vendedor_Id,
        MAX(MH.Empleado_Rol) AS Empleado_Rol,
        MAX(MH.Sucursal_Id) AS Sucursal_Id_Asignado_Meta
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_MetaHist') }} AS MH
    --Ultimo aniomes_id de la empresa
    INNER JOIN ( --noqa: ST05
        SELECT
            Emp_id,
            MAX(AnioMes_Id) AS AnioMes_Id
        FROM {{ source('DL_FARINTER', 'DL_Kielsa_MetaHist') }}
        GROUP BY Emp_id
    ) AS Ult
        ON
            MH.AnioMes_Id = Ult.AnioMes_Id
            AND MH.Emp_id = Ult.Emp_id
    WHERE MH.Empleado_Rol IS NOT NULL
    GROUP BY MH.Emp_id, MH.Empleado_Id
)

SELECT --noqa: ST06
    U.Usuario_Id,
    U.Emp_Id,
    U.Usuario_Nombre,
    U.Usuario_Login,
    U.Usuario_Email,
    U.Usuario_Huella1,
    U.Usuario_Huella2,
    ISNULL(R.Rol_Id, 0) AS [Rol_Id],
    ROL.Rol_Nombre,
    CASE
        WHEN U.Emp_Id = 5
            THEN COALESCE(rn_meta.Rol_Id, R.Rol_Id_Mapeado, 0)
        ELSE COALESCE(R.Rol_Id_Mapeado, rn_meta.Rol_Id, 0)
    END
        AS [Rol_Id_Mapeado],
    CASE
        WHEN U.Emp_Id = 5
            THEN COALESCE(rn_meta.Rol_Nombre, ROLM.Rol_Nombre, 'Sin_Rol')
        ELSE COALESCE(ROLM.Rol_Nombre, rn_meta.Rol_Nombre, 'Sin_Rol')
    END
        AS [Rol_Nombre_Mapeado],
    R.Cantidad_Roles,
    LVA.Vendedor_Id,
    LVA.Vendedor_Id AS Ultimo_Vendedor_Id_Asignado,
    LVA.Cantidad_Vendedores,
    FES.Suc_Id AS Sucursal_Id_Ultima_Factura,
    M.Sucursal_Id_Asignado_Meta,
    US.Suc_Id AS Sucursal_Id_Ultimo_Asignado_LD,
    US.Cantidad_Sucursales AS Cantidad_Sucursales_Asignadas_LD,
    CONCAT(U.Emp_Id, '-', U.Usuario_Id) AS EmpUsu_Id,
    ROW_NUMBER() OVER (
        PARTITION BY U.Usuario_Nombre,
        U.Emp_Id
        ORDER BY U.Usuario_Id DESC
    ) AS Numero_Por_Nombre,
    CONCAT(U.Emp_Id, '-', U.Usuario_Login) AS EmpLogin_Id,
    COALESCE(US.Suc_Id, M.Sucursal_Id_Asignado_Meta, FES.Suc_Id) AS Sucursal_Id_Asignado,
    CAST(CASE
        WHEN
            U.Usuario_Eliminado = 1
            OR U.Usuario_Cuenta_Deshabilitada = 1
            OR U.Usuario_Cuenta_Bloqueada = 1
            THEN 0
        ELSE 1
    END AS BIT) AS Bit_Activo,
    U.Usuario_Eliminado AS Bit_Borrado,
    COALESCE(
        LVA.Ult_Fec_Actualizacion,
        U.Usuario_Fec_Actualizacion
    ) AS Fecha_Actualizado
FROM {{ ref("DL_Kielsa_Seg_Usuario") }} AS U
LEFT JOIN VendedorAsignacion AS LVA
    ON
        U.Usuario_Id = LVA.Usuario_Id
        AND U.Emp_Id = LVA.Emp_Id
        AND LVA.Fila = 1
LEFT JOIN Ultima_Factura AS FES
    ON
        U.Emp_Id = FES.Emp_Id
        AND U.Usuario_Id = FES.Usuario_Id
        AND FES.Fila = 1
LEFT JOIN Roles AS R
    ON
        U.Emp_Id = R.Emp_Id
        AND U.Usuario_Id = R.Usuario_Id
        AND R.Fila = 1
LEFT JOIN {{ source('DL_FARINTER','DL_Kielsa_Seg_Rol') }} AS ROL
    ON
        R.Rol_Id = ROL.Rol_Id
        AND R.Emp_Id = ROL.Emp_Id
LEFT JOIN {{ source('DL_FARINTER','DL_Kielsa_Seg_Rol') }} AS ROLM
    ON
        R.Rol_Id_Mapeado = ROLM.Rol_Id
        AND R.Emp_Id = ROLM.Emp_Id
LEFT JOIN Metas AS M
    ON
        LVA.Emp_Id = M.Emp_id
        AND LVA.Vendedor_Id = M.Vendedor_Id
LEFT JOIN Usuario_Sucursal AS US
    ON
        U.Emp_Id = US.Emp_Id
        AND U.Usuario_Id = US.Usuario_Id
        AND US.Fila = 1
LEFT JOIN {{ source('DL_FARINTER','DL_Kielsa_Seg_Rol') }} AS rn_meta
    ON
        M.Empleado_Rol = rn_meta.Rol_Nombre
        AND M.Emp_Id = rn_meta.Emp_Id
