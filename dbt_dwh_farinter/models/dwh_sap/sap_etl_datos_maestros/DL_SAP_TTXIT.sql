{% set unique_key_list = ["TDOBJECT","TDID","TDSPRAS"] %}
{{ 
    config(
		as_columnstore=False,
    tags=["periodo/diario","sap_modulo/basis"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="ignore",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
    pre_hook=[
      "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            ],
		post_hook=[
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}

SELECT ISNULL(CAST(A.[TDSPRAS] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [TDSPRAS]  -- X-Clave de idioma-Check:T002-Datatype:LANG-Len:(1,0)
    , ISNULL(CAST(A.[TDOBJECT] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [TDOBJECT]  -- X-Textos: Objeto de aplicación-Check:TTXOB-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[TDID] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [TDID]  -- X-ID de texto-Check:TTXID-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[TDTEXT] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [TDTEXT]  --  -Texto breve-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD','TTXIT')}} A
WHERE A.[TDSPRAS] = 'S'
