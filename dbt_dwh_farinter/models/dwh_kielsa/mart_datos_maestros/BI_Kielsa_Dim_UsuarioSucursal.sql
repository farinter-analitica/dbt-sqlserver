{%- set unique_key_list = ["Usuario_Id", "Suc_Id", "Emp_Id"] -%}
{{ 
    config(
        as_columnstore=true,
        tags=["periodo/diario"],
        materialized="table",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Rol_Sucursal']) }}"
        ]
    ) 
}}

WITH SucursalUsuario AS (
    SELECT 
        Usuario_Id,
        Suc_Id,
        Emp_Id,
        SxU_Fec_Actualizacion,
        Consecutivo,
        Hash_UsuSucEmp
    FROM {{ ref('DL_Kielsa_Seg_Sucursal_x_Usuario') }}
),
RolAsignado AS (
    SELECT 
        Usuario_Id,
        Suc_Id,
        Emp_Id,
        Rol_Sucursal,
        Rol_Id,
        Rol_Nombre,
        SxU_Fec_Actualizacion AS Rol_Fec_Actualizacion
    FROM {{ ref('DL_Kielsa_Sucursal_Rol_Usuario_Asignado') }}
),
UsuarioInfo AS (
    SELECT 
        Usuario_Id,
        Emp_Id,
        Ultimo_Vendedor_Id_Asignado AS Vendedor_Id,
        Usuario_Email,
        Usuario_Nombre,
        Rol_Id,
        Bit_Activo,
        Fecha_Actualizado
    FROM {{ ref('BI_Kielsa_Dim_Usuario') }}
),
Roles AS (
    SELECT 
        Emp_Id,
        Rol_Id,
        Rol_Nombre,
        Rol_Jerarquia
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_Seg_Rol') }}
),
Final AS (
SELECT 
    ISNULL(SU.Usuario_Id, 0) AS Usuario_Id,
    ISNULL(SU.Suc_Id, 0) AS Suc_Id,
    ISNULL(SU.Emp_Id, 0) AS Emp_Id,
    COALESCE(RA.Rol_Sucursal, ROL.Rol_Nombre, 'No Definido') AS Rol_Sucursal,
    COALESCE(RA.Rol_Id, UI.Rol_Id, 0) AS Rol_Id,
    COALESCE(ROL.Rol_Nombre, 'No Definido') AS Rol_Nombre,
    ROL.Rol_Jerarquia,
    UI.Vendedor_Id,
    COALESCE(UI.Usuario_Nombre, 'No Definido') AS Usuario_Nombre,
    UI.Bit_Activo,
    RA.Rol_Fec_Actualizacion,
    GETDATE() AS Fecha_Actualizado
FROM SucursalUsuario SU
INNER JOIN UsuarioInfo UI
    ON SU.Usuario_Id = UI.Usuario_Id
    AND SU.Emp_Id = UI.Emp_Id
LEFT JOIN RolAsignado RA
    ON SU.Usuario_Id = RA.Usuario_Id
    AND SU.Suc_Id = RA.Suc_Id
    AND SU.Emp_Id = RA.Emp_Id
LEFT JOIN Roles ROL
    ON ROL.Emp_Id = SU.Emp_Id
    AND ROL.Rol_Id = COALESCE(RA.Rol_Id, UI.Rol_Id, 0)
)
SELECT *,
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Suc_Id"], input_length=99)}} [EmpSuc_Id],
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Suc_Id","Usuario_Id"], input_length=99)}} [EmpSucUsu_Id],
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Rol_Id"], input_length=99)}} [EmpRol_Id],
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Vendedor_Id"], input_length=99)}} [EmpVen_Id]
FROM Final