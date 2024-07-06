{% set unique_key_list = ["WERKS","LGORT"] %}
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


SELECT ISNULL(CAST(A.[WERKS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [WERKS]  -- X-Centro-Check:T001W-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[LGORT] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [LGORT]  -- X-Almacén-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[LGOBE] COLLATE DATABASE_DEFAULT AS VARCHAR(16)),'')  AS [LGOBE]  --  -Denominación de almacén-Check: -Datatype:CHAR-Len:(16,0)
    , ISNULL(CAST(A.[SPART] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [SPART]  --  -Sector-Check:TSPA-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[XLONG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XLONG]  --  -Stocks negativos permitidos en almacén-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XBUFX] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XBUFX]  --  -Está permitido fijar stock teórico en almacén-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DISKZ] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [DISKZ]  --  -Indicador de planificación de necesidades de almacén-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XBLGO] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XBLGO]  --  -Autorización almacén activa durante movimientos mercancías-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XRESS] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XRESS]  --  -Almacén asignado a recurso (recurso de almacén)-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XHUPF] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XHUPF]  --  -Obligación unidad manipulación-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[PARLG] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [PARLG]  --  -Almacén interlocutor en unidad de manipulación-Check:*-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[VKORG] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [VKORG]  --  -Organización de ventas-Check:TVKO-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[VTWEG] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [VTWEG]  --  -Canal de distribución-Check:TVTW-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[VSTEL] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [VSTEL]  --  -Pto.exped./depto.entrada mcía.-Check:TVST-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[LIFNR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [LIFNR]  --  -Número de cuenta del proveedor-Check:LFA1-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[KUNNR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [KUNNR]  --  -Número de cuenta del cliente-Check:KNA1-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[OIH_LICNO] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [OIH_LICNO]  --  -Número de permiso para stock exento de impuestos-Check: -Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[OIG_ITRFL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [OIG_ITRFL]  --  -TD: Indicador en tránsito-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[OIB_TNKASSIGN] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [OIB_TNKASSIGN]  --  -Gestión de silos: indicador de asignación de tanques-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001L')}} A
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001W')}} W WITH (NOLOCK)
  ON W.MANDT = A.MANDT
    AND W.WERKS = A.WERKS
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001K')}} K WITH (NOLOCK)
  ON K.MANDT = W.MANDT
    AND K.BWKEY = W.BWKEY
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001')}} S WITH (NOLOCK)
  ON S.MANDT = K.MANDT
    AND S.BUKRS = K.BUKRS
WHERE S.MANDT = '300'
  AND S.BUKRS NOT IN ('3100')

