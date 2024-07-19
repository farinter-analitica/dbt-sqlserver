{% set unique_key_list = ["MATNR","WERKS","LGORT","CHARG"] %}
{{ 
    config(
		as_columnstore=False,
    tags=["periodo/por_hora"],
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
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -3, max(Fecha_Actualizado)), 112), '00000000')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}
{% set last_year = last_date[0:4]%}
{% set last_month = last_date[4:6]%}

SELECT ISNULL(CAST(A.[MATNR] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [MATNR]  -- X-Número de material-Check:MARA-Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[WERKS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [WERKS]  -- X-Centro-Check:MARC-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[LGORT] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [LGORT]  -- X-Almacén-Check:MARD-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[CHARG] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [CHARG]  -- X-Número de lote-Check:MCHA-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[LVORM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LVORM]  --  -Petición de borrado para datos (todos) de stock lotes-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[ERSDA] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ERSDA]  --  -Fecha de creación-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[ERNAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [ERNAM]  --  -Nombre del responsable que ha añadido el objeto-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[LAEDA] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [LAEDA]  --  -Fecha última modificación-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[AENAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [AENAM]  --  -Nombre del responsable que ha modificado el objeto-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[LFGJA] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [LFGJA]  --  -Ejercicio del período actual-Check: -Datatype:NUMC-Len:(4,0)
    , ISNULL(CAST(A.[LFMON] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [LFMON]  --  -Período actual (período contable)-Check: -Datatype:NUMC-Len:(2,0)
    , ISNULL(CAST(A.[SPERC] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SPERC]  --  -Indicador de bloqueo para inventario-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[CLABS] AS DECIMAL(16,4)),0)  AS [CLABS]  --  -Stock valorado de libre utilización-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CUMLM] AS DECIMAL(16,4)),0)  AS [CUMLM]  --  -Stock en traslado (de almacén a almacén)-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CINSM] AS DECIMAL(16,4)),0)  AS [CINSM]  --  -Stock en control de calidad-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CEINM] AS DECIMAL(16,4)),0)  AS [CEINM]  --  -Stock total de lotes (todos) no libres-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CSPEM] AS DECIMAL(16,4)),0)  AS [CSPEM]  --  -Stock bloqueado-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CRETM] AS DECIMAL(16,4)),0)  AS [CRETM]  --  -Stock bloqueado de devoluciones-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CVMLA] AS DECIMAL(16,4)),0)  AS [CVMLA]  --  -Stock valorado de utilización libre de periodo anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CVMUM] AS DECIMAL(16,4)),0)  AS [CVMUM]  --  -Stock en traslado a otro almacén del periodo anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CVMIN] AS DECIMAL(16,4)),0)  AS [CVMIN]  --  -Stock en inspección de calidad del periodo precedente-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CVMEI] AS DECIMAL(16,4)),0)  AS [CVMEI]  --  -Stock no libre del periodo anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CVMSP] AS DECIMAL(16,4)),0)  AS [CVMSP]  --  -Stock bloqueado del periodo anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[CVMRE] AS DECIMAL(16,4)),0)  AS [CVMRE]  --  -Stock bloqueado de devoluciones del período anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[KZICL] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZICL]  --  -Indicador de inventario para stock almacén año actual-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZICQ] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZICQ]  --  -Ind. de invent. para stock en control calidad año actual-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZICE] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZICE]  --  -Indicador de inventario para stock de utilización limitada-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZICS] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZICS]  --  -Indicador de inventario para stock bloqueado-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZVCL] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZVCL]  --  -Ind.de inventario para stock almacén en el período anterior-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZVCQ] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZVCQ]  --  -Ind-inventario para stock en control-calidad en periodo ant.-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZVCE] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZVCE]  --  -Ind-inventario para stock utiliz. restring en periodo anter.-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KZVCS] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [KZVCS]  --  -Ind. de inventario para stock bloqueado en el periodo anter.-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[HERKL] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [HERKL]  --  -País de origen del material (origen CCI)-Check:T005-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[CHDLL] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [CHDLL]  --  -Fecha último recuento inventario stock libre utilización-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[CHJIN] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [CHJIN]  --  -Ejercicio del indicador de inventario actual-Check: -Datatype:NUMC-Len:(4,0)
    , ISNULL(CAST(A.[CHRUE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [CHRUE]  --  -El reg.MCHBH p.per.ante-anterior del per.MCHB ya existe-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'MCHB')}} A
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
    OR (A.LAEDA >= '{{ last_date }}')
    OR (A.ERSDA >= '{{ last_date }}')
    )
{% else %}
  --and S.LAEDA >= '00000000'
{% endif %}

