{% set unique_key_list = ["BNAME"] %}
{{ 
    config(
		as_columnstore=False,
        tags=["periodo/diario","sap_modulo/basis"],
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
    A.[NAME_FIRST] COLLATE DATABASE_DEFAULT AS [NAME_FIRST],
    A.[NAME_LAST] COLLATE DATABASE_DEFAULT AS [NAME_LAST],
    A.[NAME_TEXTC] COLLATE DATABASE_DEFAULT AS [NAME_TEXTC],
    A.[TEL_EXTENS] COLLATE DATABASE_DEFAULT AS [TEL_EXTENS],
    A.[KOSTL] COLLATE DATABASE_DEFAULT AS [KOSTL],
    A.[BUILDING] COLLATE DATABASE_DEFAULT AS [BUILDING],
    A.[ROOMNUMBER] COLLATE DATABASE_DEFAULT AS [ROOMNUMBER],
    A.[DEPARTMENT] COLLATE DATABASE_DEFAULT AS [DEPARTMENT],
    A.[INHOUSE_ML] COLLATE DATABASE_DEFAULT AS [INHOUSE_ML],
    A.[NAME1] COLLATE DATABASE_DEFAULT AS [NAME1],
    A.[CITY1] COLLATE DATABASE_DEFAULT AS [CITY1],
    A.[POST_CODE1] COLLATE DATABASE_DEFAULT AS [POST_CODE1],
    GETDATE() as Fecha_Carga,
    GETDATE() as Fecha_Actualizado
FROM  {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'USER_ADDR') }} A 
WHERE A.MANDT = '300'
