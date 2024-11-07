{% set unique_key_list = ["MATNR","WERKS","LGORT"] %}
{{ 
    config(
		as_columnstore=False,
    tags=["periodo/por_hora","sap_modulo/mm"],
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
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -3, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}
{% set last_year = last_date[0:4]%}
{% set last_month = last_date[4:6]%}

SELECT ISNULL(CAST(A.[MATNR] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [MATNR]  -- X-Número de material-Check:MARA-Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[WERKS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [WERKS]  -- X-Centro-Check:MARC-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[LGORT] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [LGORT]  -- X-Almacén-Check:T001L-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[PSTAT] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [PSTAT]  --  -Status de actualización-Check: -Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[LVORM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LVORM]  --  -Marcar para borrado material a nivel de almacén-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[LFGJA] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [LFGJA]  --  -Ejercicio del período actual-Check: -Datatype:NUMC-Len:(4,0)
    , ISNULL(CAST(A.[LFMON] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [LFMON]  --  -Período actual (período contable)-Check: -Datatype:NUMC-Len:(2,0)
    , ISNULL(CAST(A.[SPERR] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SPERR]  --  -Indicador de bloqueo para inventario-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[LABST] AS DECIMAL(16,4)),0)  AS [LABST]  --  -Stock valorado de libre utilización-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[UMLME] AS DECIMAL(16,4)),0)  AS [UMLME]  --  -Stock en traslado (de almacén a almacén)-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[INSME] AS DECIMAL(16,4)),0)  AS [INSME]  --  -Stock en control de calidad-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[EINME] AS DECIMAL(16,4)),0)  AS [EINME]  --  -Stock total de lotes (todos) no libres-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[SPEME] AS DECIMAL(16,4)),0)  AS [SPEME]  --  -Stock bloqueado-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[RETME] AS DECIMAL(16,4)),0)  AS [RETME]  --  -Stock bloqueado de devoluciones-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VMLAB] AS DECIMAL(16,4)),0)  AS [VMLAB]  --  -Stock valorado de utilización libre de periodo anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VMUML] AS DECIMAL(16,4)),0)  AS [VMUML]  --  -Stock en traslado a otro almacén del periodo anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VMINS] AS DECIMAL(16,4)),0)  AS [VMINS]  --  -Stock en inspección de calidad del periodo precedente-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VMEIN] AS DECIMAL(16,4)),0)  AS [VMEIN]  --  -Stock no libre del periodo anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VMSPE] AS DECIMAL(16,4)),0)  AS [VMSPE]  --  -Stock bloqueado del periodo anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VMRET] AS DECIMAL(16,4)),0)  AS [VMRET]  --  -Stock bloqueado de devoluciones del período anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[KZILL] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZILL]  --  -Indicador de inventario para stock almacén año actual-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZILQ] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZILQ]  --  -Ind. de invent. para stock en control calidad año actual-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZILE] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZILE]  --  -Indicador de inventario para stock de utilización limitada-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZILS] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZILS]  --  -Indicador de inventario para stock bloqueado-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZVLL] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZVLL]  --  -Ind.de inventario para stock almacén en el período anterior-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZVLQ] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZVLQ]  --  -Ind-inventario para stock en control-calidad en periodo ant.-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZVLE] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZVLE]  --  -Ind-inventario para stock utiliz. restring en periodo anter.-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZVLS] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZVLS]  --  -Ind. de inventario para stock bloqueado en el periodo anter.-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[DISKZ] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [DISKZ]  --  -Indicador de planificación de necesidades de almacén-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[LSOBS] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [LSOBS]  --  -Clase aprovisionam. especial almacén-Check:T460A-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[LMINB] AS DECIMAL(16,4)),0)  AS [LMINB]  --  -Punto de pedido para planificación de necesidades-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[LBSTF] AS DECIMAL(16,4)),0)  AS [LBSTF]  --  -Cantidad de reposición para planif.necesidades de almacén-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[HERKL] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [HERKL]  --  -País de origen del material (origen CCI)-Check:T005-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[EXPPG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [EXPPG]  --  -Indicador de preferencia (desactiv.)-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[EXVER] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [EXVER]  --  -Indicador de exportación (desactiv.)-Check: -Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[LGPBE] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [LGPBE]  --  -Ubicación-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[KLABS] AS DECIMAL(16,4)),0)  AS [KLABS]  --  -Stock en consignación de libre utilización-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[KINSM] AS DECIMAL(16,4)),0)  AS [KINSM]  --  -Stock de consignación en control de calidad-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[KEINM] AS DECIMAL(16,4)),0)  AS [KEINM]  --  -Stock consi no libre-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[KSPEM] AS DECIMAL(16,4)),0)  AS [KSPEM]  --  -Stock artículos en consignación bloqueado-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[DLINL] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [DLINL]  --  -Fecha último recuento inventario stock libre utilización-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[PRCTL] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [PRCTL]  --  -Centro de beneficio-Check:*-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[ERSDA] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ERSDA]  --  -Fecha de creación-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[VKLAB] AS DECIMAL(16,4)),0)  AS [VKLAB]  --  -Valor de stocks de un material de valor al precio de venta-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[VKUML] AS DECIMAL(16,4)),0)  AS [VKUML]  --  -Valor de venta en el traslado (de almacén a almacén)-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[LWMKB] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [LWMKB]  --  -Área de picking para lean WM-Check:*-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[BSKRF] AS DECIMAL(16,16)),0)  AS [BSKRF]  --  -Factor de corrección de stock-Check: -Datatype:FLTP-Len:(16,16)
    , ISNULL(CAST(A.[MDRUE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MDRUE]  --  -El reg.MARDH p.per.ante-anterior del per.MARD ya existe-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MDJIN] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [MDJIN]  --  -Ejercicio del indicador de inventario actual-Check: -Datatype:NUMC-Len:(4,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'MARD')}} A
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001W')}} W WITH (NOLOCK)
  ON W.MANDT = A.MANDT
    AND W.WERKS = A.WERKS
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001K')}} K WITH (NOLOCK)
  ON K.MANDT = W.MANDT
    AND K.BWKEY = W.BWKEY
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001')}} S WITH (NOLOCK)
  ON S.MANDT = K.MANDT
    AND S.BUKRS = K.BUKRS
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001L')}} E WITH (NOLOCK)
  ON S.MANDT = E.MANDT
    AND W.WERKS = E.WERKS
    AND A.LGORT = E.LGORT
WHERE S.MANDT = '300'
  AND S.BUKRS NOT IN ('3100')
{% if is_incremental() %}
  AND (
    (
      A.LFGJA = '{{ last_year }}'
      AND A.LFMON >= '{{ last_month }}'
      )
    OR (A.LFGJA > '{{ last_year }}')
    )
{% else %}
  --and S.LAEDA >= '00000000'
{% endif %}

