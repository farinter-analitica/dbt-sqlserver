{%- set unique_key_list = ["Vendedor_Id", "Suc_Id", "Emp_Id"] -%}
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
        ]
    ) 
}}

WITH VendedorSucursal AS (
    SELECT
        Vendedor_Id,
        Suc_Id,
        Emp_Id,
        Vendedor_Fec_Actualizacion,
        Consecutivo,
        Hash_VendedorSucEmp
    FROM {{ source('DL_FARINTER', 'DL_Kielsa_VendedorSucursal') }}
),

VendedorInfo AS (
    SELECT
        Vendedor_Id,
        Emp_Id,
        Vendedor_Nombre,
        Rol_Id,
        Bit_Activo
    FROM {{ ref('BI_Kielsa_Dim_Vendedor') }}
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
        VI.Vendedor_Nombre,
        VI.Rol_Id,
        VI.Bit_Activo,
        VS.Vendedor_Fec_Actualizacion,
        R.Rol_Jerarquia,
        ISNULL(VS.Vendedor_Id, 0) AS Vendedor_Id,
        ISNULL(VS.Suc_Id, 0) AS Suc_Id,
        ISNULL(VS.Emp_Id, 0) AS Emp_Id,
        GETDATE() AS Fecha_Actualizado,
        COALESCE(R.Rol_Nombre, 'No Definido') AS Rol_Nombre
    FROM VendedorSucursal AS VS
    INNER JOIN VendedorInfo AS VI
        ON
            VS.Vendedor_Id = VI.Vendedor_Id
            AND VS.Emp_Id = VI.Emp_Id
    LEFT JOIN Roles AS R
        ON
            VS.Emp_Id = R.Emp_Id
            AND VI.Rol_Id = R.Rol_Id
)

SELECT
    Vendedor_Id,
    Suc_Id,
    Emp_Id,
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Suc_Id"], input_length=99) }} AS EmpSuc_Id,
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Suc_Id","Vendedor_Id"], input_length=99) }} AS EmpSucVen_Id,
    {{ dwh_farinter_concat_key_columns(columns=["Emp_Id","Rol_Id"], input_length=99) }} AS EmpRol_Id,
    Vendedor_Nombre,
    Rol_Id,
    Bit_Activo,
    Vendedor_Fec_Actualizacion,
    Rol_Jerarquia,
    Fecha_Actualizado,
    Rol_Nombre
FROM Final
