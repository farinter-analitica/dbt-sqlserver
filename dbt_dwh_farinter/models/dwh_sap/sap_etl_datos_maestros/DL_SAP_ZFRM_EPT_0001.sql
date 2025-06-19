{% set unique_key_list = ["BNAME"] %}
{{ 
    config(
		as_columnstore=False,
        tags=["periodo/diario","sap_modulo/ztables"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
    pre_hook=[
      "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            ],
		post_hook=[
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        ]
	) 
}}

SELECT 
    A.[BNAME] COLLATE DATABASE_DEFAULT AS [BNAME],
    A.[VKGRP] COLLATE DATABASE_DEFAULT AS [VKGRP],
    GETDATE() as Fecha_Carga,
    GETDATE() as Fecha_Actualizado
FROM  {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'ZFRM_EPT_0001') }} A 
WHERE A.MANDT = '300'
