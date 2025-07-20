{% set unique_key_list = ["Rol_Id", "Emp_Id"] %}
{{
    config(
        as_columnstore=true,
        tags=["periodo/diario", "automation/periodo_por_hora"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        merge_exclude_columns=unique_key_list + ["Rol_Fec_Creacion"],
        merge_check_diff_exclude_columns=unique_key_list + ["Rol_Fec_Creacion", "Rol_Fec_Actualizacion"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
    )
}}


select
    seg.Emp_Id,
    seg.Rol_Id,
    seg.Rol_Nombre,
    seg.Rol_Padre,
    seg_padre.Rol_Nombre as Rol_Nombre_Padre,
    seg.Rol_Nivel,
    seg.Rol_Fec_Actualizacion,
    seg.Rol_Porc_Cambio_Precio,
    seg.Rol_Ver_Todas_Sucursal_Rep,
    seg.Usuario_Id_Crea,
    seg.Usuario_Id_Actualiza,
    seg.Rol_Fec_Creacion,
    jer.rol_id_ld_responsable as [Rol_Id_Responsable],
    seg_responsable.Rol_Nombre as [Rol_Nombre_Responsable],
    jer.profundidad as [Rol_Jerarquia],
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Rol_Id'], input_length=29, table_alias='seg') }} as [EmpRol_Id]
from {{ source('DL_FARINTER', 'DL_Kielsa_Seg_Rol') }} as seg
left join {{ ref('dlv_kielsa_incentivo_rol_jerarquia') }} as jer
    on seg.Rol_Id = jer.rol_id_ld and seg.Emp_Id = jer.emp_id
left join {{ source('DL_FARINTER', 'DL_Kielsa_Seg_Rol') }} as seg_responsable
    on jer.rol_id_ld_responsable = seg_responsable.Rol_Id and seg.Emp_Id = seg_responsable.Emp_Id
left join {{ source('DL_FARINTER', 'DL_Kielsa_Seg_Rol') }} as seg_padre
    on seg.Rol_Padre = seg_padre.Rol_Id and seg.Emp_Id = seg_padre.Emp_Id
