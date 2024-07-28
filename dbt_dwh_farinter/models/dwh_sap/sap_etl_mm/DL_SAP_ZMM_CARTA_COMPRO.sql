{% set unique_key_list = ["BUKRS","MATNR","LICHA","EBELN","ID"] %}
{{ 
    config(
		as_columnstore=False,
    tags=["periodo/diario","sap_modulo/ztables"],
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
        ]
	) 
}}
/*
    full_refresh= true,
*/
{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -3, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}

SELECT
    ISNULL(CAST(A.[ID] AS INT),0) AS [ID]  -- X-Número natural-Check: -Datatype:INT4-Len:(10,0)
    , ISNULL(CAST(A.[BUKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BUKRS]  -- X-Sociedad-Check:*-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[MATNR] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [MATNR]  -- X-Número de material-Check:*-Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[LICHA] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [LICHA]  -- X-Número de lote de proveedor-Check: -Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[EBELN] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [EBELN]  -- X-Número del documento de compras-Check:*-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[ZCARTA] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ZCARTA]  --  -Carta compromiso-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[ZAPRO_INT] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ZAPRO_INT]  --  -Aprobación Interna-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[USNAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [USNAM]  --  -Nombre del usuario-Check:*-Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[ERDAT] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ERDAT]  --  -Fecha de creación del registro-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[ERZET] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [ERZET]  --  -Hora registrada-Check: -Datatype:TIMS-Len:(6,0)
    , ISNULL(CAST(A.[AENAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [AENAM]  --  -Nombre del responsable que ha modificado el objeto-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[AEDAT] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [AEDAT]  --  -Fecha última modificación-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[UTIME] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [UTIME]  --  -Hora de modificación-Check: -Datatype:TIMS-Len:(6,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'ZMM_CARTA_COMPRO')}} A
WHERE A.MANDT = '300'
{% if is_incremental() %}
  and (A.ERDAT >= '{{last_date}}' or A.AEDAT >= '{{last_date}}')
{% else %}
  --and S.LAEDA >= '00000000'
{% endif %}

