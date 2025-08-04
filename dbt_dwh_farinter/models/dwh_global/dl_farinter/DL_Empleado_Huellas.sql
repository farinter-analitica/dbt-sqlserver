{%- set unique_key_list = [
    "Empleado_Id", "Huella_id", "Base_Origen"
] -%}

{{-
    config(
        as_columnstore=true,
        tags=["automation/periodo_diario"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        merge_soft_delete_column="Es_Eliminado",
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ",
                create_clustered=false,
                is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
    ) 
}}


{%- set bases_config = [
    {'base': 'HUELLAS_HN_FA', 'Pais_ISO2': 'HN'},
    {'base': 'HUELLAS_FA_BR', 'Pais_ISO2': 'SV'},
    {'base': 'HUELLAS_CR_ALT', 'Pais_ISO2': 'CR'},
    {'base': 'HUELLAS_NI_RE', 'Pais_ISO2': 'NI'},
    {'base': 'HUELLAS_SV', 'Pais_ISO2': 'SV'}
] -%}

{%- set servidor = var('P_SQLLDSUBS_LS') -%}


WITH huellas_union AS (
    {%- for item in bases_config %}
        {%- if not loop.first %} UNION ALL {% endif %}
        SELECT
            '{{ item.Pais_ISO2 }}' AS Pais_ISO2,
            A.Empleado_Id,
            A.Empleado_Identificador COLLATE DATABASE_DEFAULT AS Empleado_Identificador, -- noqa: RF02
            A.Empleado_Nombre COLLATE DATABASE_DEFAULT AS Empleado_Nombre, -- noqa: RF02
            A.Emp_Fecha_Actualizacion,
            B.Empleado_Huella COLLATE DATABASE_DEFAULT AS Empleado_Huella, -- noqa: RF02
            B.Huella_id,
            '{{ item.base }}' AS Base_Origen,
            0 AS Es_Eliminado,
            GETDATE() AS Fecha_Carga,
            GETDATE() AS Fecha_Actualizado
        FROM {{ servidor }}.{{ item.base }}.dbo.Empleado AS A
        INNER JOIN {{ servidor }}.{{ item.base }}.dbo.Empleado_Huellas AS B
            ON A.Empleado_Id = B.Empleado_Id
    {%- endfor %}
)

SELECT * FROM huellas_union
