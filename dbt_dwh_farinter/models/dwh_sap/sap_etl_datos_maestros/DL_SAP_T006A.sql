{% set unique_key_list = ["SPRAS","MSEHI"] %}
{{ 
    config(
		as_columnstore=False,
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
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
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}
/*
    full_refresh= true,
*/


SELECT ISNULL(CAST(A.[SPRAS] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SPRAS]  -- X-Clave de idioma-Check:T002-Datatype:LANG-Len:(1,0)
    , ISNULL(CAST(A.[MSEHI] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [MSEHI]  -- X-Unidad de medida-Check:T006-Datatype:UNIT-Len:(3,0)
    , ISNULL(CAST(A.[MSEH3] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [MSEH3]  --  -Unidad de medida externa repr. comercial (3 posiciones)-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[MSEH6] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [MSEH6]  --  -Unidad de medida externa repr.técnica (6 posiciones)-Check: -Datatype:CHAR-Len:(6,0)
    , ISNULL(CAST(A.[MSEHT] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [MSEHT]  --  -Texto para la unidad de medida (máx. 10 posiciones)-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[MSEHL] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [MSEHL]  --  -Texto para la unidad de medida (máx.30 posiciones)-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T006A')}} A
WHERE A.MANDT = '300'


