{% set unique_key_list = ["MATNR","CHARG"] %}
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
{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,max(Fecha_Actualizado), 112), '00000000')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}
{% set last_year = last_date[0:4]%}
{% set last_month = last_date[4:6]%}

SELECT ISNULL(CAST(A.[MATNR] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [MATNR]  -- X-Número de material-Check:MARA-Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[CHARG] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [CHARG]  -- X-Número de lote-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[LVORM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LVORM]  --  -Petición de borrado p.todos los datos de un lote-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[ERSDA] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ERSDA]  --  -Fecha de creación-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[ERNAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [ERNAM]  --  -Nombre del responsable que ha añadido el objeto-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[AENAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [AENAM]  --  -Nombre del responsable que ha modificado el objeto-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[LAEDA] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [LAEDA]  --  -Fecha última modificación-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[VERAB] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [VERAB]  --  -Fecha de disponibilidad-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[VFDAT] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [VFDAT]  --  -Fecha de caducidad o fecha preferente de consumo-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[ZUSCH] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ZUSCH]  --  -Clave de estado de lote-Check:*-Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[ZUSTD] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ZUSTD]  --  -Lote en stock no libre-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[ZAEDT] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ZAEDT]  --  -Fecha de la última modificación de estado-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[LIFNR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [LIFNR]  --  -Número de cuenta del proveedor-Check:LFA1-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[LICHA] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [LICHA]  --  -Número de lote de proveedor-Check: -Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[VLCHA] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [VLCHA]  --  -Número de lote original (desactivado)-Check:*-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[VLWRK] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [VLWRK]  --  -Centro de origen (desactivado)-Check:MARC-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[VLMAT] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [VLMAT]  --  -Núm. material orig.  (desactivado)-Check:MARA-Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[CHAME] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [CHAME]  --  -Unidad de medida de salida del lote (desactivado)-Check:*-Datatype:UNIT-Len:(3,0)
    , ISNULL(CAST(A.[LWEDT] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [LWEDT]  --  -Fecha de la última entrada de mercancía-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[FVDT1] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [FVDT1]  --  -Fecha de libre utilización-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[FVDT2] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [FVDT2]  --  -Fecha de libre utilización-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[FVDT3] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [FVDT3]  --  -Fecha de libre utilización-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[FVDT4] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [FVDT4]  --  -Fecha de libre utilización-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[FVDT5] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [FVDT5]  --  -Fecha de libre utilización-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[FVDT6] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [FVDT6]  --  -Fecha de libre utilización-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[HERKL] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [HERKL]  --  -País de origen del material (origen CCI)-Check:T005-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[HERKR] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [HERKR]  --  -Región de origen del material (origen Cámara de Comercio)-Check:T005S-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[MTVER] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [MTVER]  --  -Grupo de materiales exportación p.comercio exterior-Check:TVFM-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[QNDAT] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [QNDAT]  --  -Próxima fecha inspección-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[HSDAT] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [HSDAT]  --  -Fecha de producción/fabricación-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[CUOBJ_BM] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [CUOBJ_BM]  --  -Número interno objeto: Clasificación lotes-Check: -Datatype:NUMC-Len:(18,0)
    , ISNULL(CAST(A.[DEACT_BM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [DEACT_BM]  --  -El lote ya no está activo-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BATCH_TYPE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BATCH_TYPE]  --  -Type of Batch-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'MCH1')}} A
WHERE A.MANDT = '300'
{% if is_incremental() %}
  AND (
    (A.LAEDA >= '{{ last_date }}')
    OR (A.ERSDA >= '{{ last_date }}')
    )
{% else %}
  --and S.LAEDA >= '00000000'
{% endif %}

