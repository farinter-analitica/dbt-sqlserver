
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
        ) as Fila
    FROM {{ ref("DL_Kielsa_Vendedor_x_Usuario") }} VxU
),
Usuario_Sucursal AS
(
    SELECT V.Emp_Id, V.Usuario_Id, V.Suc_Id,
    ROW_NUMBER() OVER(PARTITION BY V.Emp_Id, V.Usuario_Id ORDER BY V.SxU_Fec_Actualizacion DESC) AS Fila
    FROM {{ ref("DL_Kielsa_Seg_Sucursal_x_Usuario") }} V
),
Ultima_Factura AS
(
    SELECT Emp_Id, 
        Usuario_Id, 
        Suc_Id , 
        MAX(Factura_Fecha) AS Fecha_Ultima_Factura, 
        row_number() OVER(PARTITION BY Emp_Id, Usuario_Id ORDER BY MAX(Factura_Fecha) DESC) AS fila
    FROM DL_FARINTER.dbo.DL_Kielsa_FacturaEncabezado -- {{source('DL_FARINTER', 'DL_Kielsa_FacturaEncabezado')}}
    WHERE AnioMes_Id >=(YEAR(GETDATE())-1)*100+MONTH(GETDATE())		 
    GROUP BY Emp_Id, Usuario_Id, Suc_Id
),
Roles AS
(
    SELECT UR.Emp_Id, UR.Usuario_Id, 
        UR.Rol_Id, 
        Cantidad_Roles,
        U.Usuario_Eliminado,
        U.Usuario_Cuenta_Deshabilitada,
        ROW_NUMBER() OVER(PARTITION BY UR.Emp_Id, UR.Usuario_Id ORDER BY UR.Fec_Actualizacion DESC) AS Fila
    FROM (SELECT *,
            COUNT(UR.Rol_Id) OVER (PARTITION BY UR.Usuario_Id, UR.Emp_Id)  Cantidad_Roles
            FROM DL_FARINTER.dbo.DL_Kielsa_Seg_Usuario_x_Rol UR) UR --{{ref('DL_Kielsa_Seg_Usuario_x_Rol')}}
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Seg_Usuario U -- {{ref('DL_Kielsa_Seg_Usuario')}}
    ON UR.Usuario_Id = U.Usuario_Id
    AND UR.Emp_Id = U.Emp_Id
    WHERE U.Usuario_Eliminado = 0
        AND U.Usuario_Cuenta_Deshabilitada = 0
),
Metas AS
(
    SELECT MH.Emp_id, Empleado_Id AS Vendedor_Id, MAX(Empleado_Rol) Empleado_Rol, MAX(Sucursal_Id) Sucursal_Id_Asignado_Meta
    FROM DL_FARINTER.dbo.DL_Kielsa_MetaHist MH --{{source('DL_FARINTER', 'DL_Kielsa_MetaHist')}}
    --Ultimo aniomes_id de la empresa
    INNER JOIN	(SELECT Emp_id, MAX(AnioMes_Id) AnioMes_Id 
            FROM DL_FARINTER.dbo.DL_Kielsa_MetaHist MH --{{source('DL_FARINTER', 'DL_Kielsa_MetaHist')}}
            GROUP BY MH.Emp_id) Ult
        ON MH.AnioMes_Id = Ult.AnioMes_Id
        AND MH.Emp_id = Ult.Emp_id
    WHERE MH.Empleado_Rol IS NOT NULL
    GROUP BY MH.Emp_id, MH.Empleado_Id
)
SELECT U.Usuario_Id,
    U.Emp_Id,
    CONCAT(U.Emp_Id, '-', U.Usuario_Id) AS EmpUsu_Id,
    U.Usuario_Nombre,
    ROW_NUMBER() OVER(
        PARTITION BY U.Usuario_Nombre,
        U.Emp_Id
        ORDER BY U.Usuario_Id DESC
    ) Numero_Por_Nombre,
    U.Usuario_Login,
    CONCAT(U.Emp_Id, '-', U.Usuario_Login) AS EmpLogin_Id,
    U.Usuario_Email,
    R.Rol_Id,
    ROL.Rol_Nombre,
    R.Cantidad_Roles,
    LVA.Vendedor_Id AS Ultimo_Vendedor_Id_Asignado,
    LVA.Cantidad_Vendedores,
    FES.Suc_Id AS Sucursal_Id_Ultima_Factura,
    M.Sucursal_Id_Asignado_Meta,
    US.Suc_Id AS Sucursal_Id_Ultimo_Asignado_LD,
    COALESCE(M.Sucursal_Id_Asignado_Meta, FES.Suc_Id, US.Suc_Id) AS Sucursal_Id_Asignado,
    CAST(CASE
        WHEN U.Usuario_Eliminado = 1
        OR U.Usuario_Cuenta_Deshabilitada = 1
        OR U.Usuario_Cuenta_Bloqueada = 1
        THEN 0
        ELSE 1
    END AS BIT) AS Bit_Activo,
    COALESCE(
        LVA.Ult_Fec_Actualizacion,
        U.Usuario_Fec_Actualizacion
    ) AS Fecha_Actualizado
FROM {{ ref("DL_Kielsa_Seg_Usuario") }} U
LEFT JOIN VendedorAsignacion LVA ON  
    U.Usuario_Id = LVA.Usuario_Id 
    AND U.Emp_Id = LVA.Emp_Id
    AND LVA.Fila = 1
LEFT JOIN Ultima_Factura FES
    ON U.Emp_Id = FES.Emp_Id
    AND U.Usuario_Id = FES.Usuario_Id
    AND FES.Fila = 1
LEFT JOIN Roles R
    ON U.Emp_Id = R.Emp_Id
    AND U.Usuario_Id = R.Usuario_Id
    AND R.Fila = 1
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Seg_Rol ROL --{{source('DL_FARINTER','DL_Kielsa_Seg_Rol')}}
    ON R.Rol_Id = ROL.Rol_Id
    AND R.Emp_Id = ROL.Emp_Id
LEFT JOIN Metas M
    ON LVA.Emp_Id = M.Emp_id
    AND LVA.Vendedor_Id = M.Vendedor_Id
LEFT JOIN Usuario_Sucursal US
    ON U.Emp_Id = US.Emp_Id
    AND U.Usuario_Id = US.Usuario_Id
    AND US.Fila = 1
