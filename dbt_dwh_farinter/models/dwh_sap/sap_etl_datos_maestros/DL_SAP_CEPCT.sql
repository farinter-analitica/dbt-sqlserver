{% set unique_key_list = ["PRCTR","KOKRS","SPRAS","DATBI"] %}
{{ 
    config(
		as_columnstore=False,
    tags=["periodo/diario","sap_modulo/co"],
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
    , ISNULL(CAST(A.[PRCTR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [PRCTR]  -- X-Centro de beneficio-Check:CEPC-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[DATBI] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [DATBI]  -- X-Fecha fin validez-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[KOKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [KOKRS]  -- X-Sociedad CO-Check:CEPC-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[KTEXT] COLLATE DATABASE_DEFAULT AS VARCHAR(20)),'')  AS [KTEXT]  --  -Denominación general-Check: -Datatype:CHAR-Len:(20,0)
    , ISNULL(CAST(A.[LTEXT] COLLATE DATABASE_DEFAULT AS VARCHAR(40)),'')  AS [LTEXT]  --  -Texto explicativo-Check: -Datatype:CHAR-Len:(40,0)
    , ISNULL(CAST(A.[MCTXT] COLLATE DATABASE_DEFAULT AS VARCHAR(20)),'')  AS [MCTXT]  --  -Concepto de búsqueda para utilizar matchcodes-Check: -Datatype:CHAR-Len:(20,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'CEPCT')}} A
WHERE A.MANDT = '300'
  AND A.SPRAS IN ('S')

