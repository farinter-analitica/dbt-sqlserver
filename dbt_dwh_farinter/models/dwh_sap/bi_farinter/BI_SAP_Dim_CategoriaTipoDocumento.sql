{% set unique_key_list = ["CategoriaTipoDocumento_Id"] %}
{{ 
    config(
        as_columnstore=true,
        tags=["automation/periodo_semanal_1", "automation_only"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}"
      		"{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
    ) 
}}

SELECT 
    A.DOMVALUE_L AS CategoriaTipoDocumento_Id,
    A.DDTEXT AS CategoriaTipoDocumento_Nombre,
    B.DDTEXT AS CategoriaTipoDocumento_Nombre_EN,
    GETDATE() AS Fecha_Actualizado
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD','DD07V')}} A
LEFT JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD','DD07V')}} B
    ON A.DOMVALUE_L = B.DOMVALUE_L 
    AND B.DOMNAME = 'VBTYP' 
    AND B.DDLANGUAGE = 'E'
WHERE A.DOMNAME = 'VBTYP' 
AND A.DDLANGUAGE = 'S'