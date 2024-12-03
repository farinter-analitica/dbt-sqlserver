{% set unique_key_list = ["MATNR"] %}
{{ 
    config(
		as_columnstore=False,
    tags=["periodo/diario", "periodo/por_hora","sap_modulo/mm"],
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

SELECT
    ISNULL(CAST(A.[MATNR] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [MATNR]  -- X-Número de material-Check: -Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[ERSDA] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ERSDA]  --  -Fecha de creación-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[ERNAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [ERNAM]  --  -Nombre del responsable que ha añadido el objeto-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[LAEDA] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [LAEDA]  --  -Fecha última modificación-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[AENAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [AENAM]  --  -Nombre del responsable que ha modificado el objeto-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[VPSTA] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [VPSTA]  --  -Status de actualización del material completo-Check: -Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[PSTAT] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [PSTAT]  --  -Status de actualización-Check: -Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[LVORM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LVORM]  --  -Marcar para borrado material a nivel de mandante-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MTART] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [MTART]  --  -Tipo de material-Check:T134-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[MBRSH] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MBRSH]  --  -Ramo-Check:T137-Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MATKL] COLLATE DATABASE_DEFAULT AS VARCHAR(9)),'')  AS [MATKL]  --  -Grupo de artículos-Check:T023-Datatype:CHAR-Len:(9,0)
    , ISNULL(CAST(A.[BISMT] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [BISMT]  --  -Número de material antiguo-Check: -Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[MEINS] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [MEINS]  --  -Unidad de medida base-Check:T006-Datatype:UNIT-Len:(3,0)
    , ISNULL(CAST(A.[BSTME] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [BSTME]  --  -Unidad de medida de pedido-Check:T006-Datatype:UNIT-Len:(3,0)
    , ISNULL(CAST(A.[GROES] COLLATE DATABASE_DEFAULT AS VARCHAR(32)),'')  AS [GROES]  --  -Tamaño/Dimensión-Check: -Datatype:CHAR-Len:(32,0)
    , ISNULL(CAST(A.[NORMT] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [NORMT]  --  -Registro Sanitario-Check: -Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[LABOR] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [LABOR]  --  -Marca de material para materiales de consumo-Check:T024L-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[BRGEW] AS DECIMAL(16,4)),0)  AS [BRGEW]  --  -Peso bruto-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[NTGEW] AS DECIMAL(16,4)),0)  AS [NTGEW]  --  -Peso neto-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[GEWEI] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [GEWEI]  --  -Unidad de peso-Check:T006-Datatype:UNIT-Len:(3,0)
    , ISNULL(CAST(A.[VOLUM] AS DECIMAL(16,4)),0)  AS [VOLUM]  --  -Volumen-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VOLEH] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [VOLEH]  --  -Unidad de volumen-Check:T006-Datatype:UNIT-Len:(3,0)
    , ISNULL(CAST(A.[RAUBE] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [RAUBE]  --  -Condiciones de almacenaje-Check:T142-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[TEMPB] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [TEMPB]  --  -Indicador para condiciones de temperatura-Check:T143-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[DISST] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [DISST]  --  -Nivel de planificación de necesidades-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[TRAGR] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [TRAGR]  --  -Grupo de transporte-Check:TTGR-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[SPART] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [SPART]  --  -Sector-Check:TSPA-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[KUNNR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [KUNNR]  --  -Competencia-Check:V_KNA1WETT-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[SAISO] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [SAISO]  --  -Tipo de temporada-Check:T6WSP-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[ETIAR] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [ETIAR]  --  -Clase de etiqueta-Check:T6WP3-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[EAN11] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [EAN11]  --  -Número de artículo europeo (EAN)-Check: -Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[NUMTP] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [NUMTP]  --  -Tipo de número del Número de Artículo Europeo-Check:TNTP-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[LAENG] AS DECIMAL(16,4)),0)  AS [LAENG]  --  -Longitud-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[BREIT] AS DECIMAL(16,4)),0)  AS [BREIT]  --  -Ancho-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[HOEHE] AS DECIMAL(16,4)),0)  AS [HOEHE]  --  -Altura-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[MEABM] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [MEABM]  --  -Unidad de medida para longitud/ancho/altura-Check:T006-Datatype:UNIT-Len:(3,0)
    , ISNULL(CAST(A.[PRDHA] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [PRDHA]  --  -Jerarquía de productos-Check:T179-Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[QMPUR] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [QMPUR]  --  -QM activo en aprovisionam.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VABME] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [VABME]  --  -Unidad variable de medida de pedido activa-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[KZKFG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KZKFG]  --  -Material configurable-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XCHPF] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XCHPF]  --  -Indicador: Sujeto a lote-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VHART] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [VHART]  --  -Tipo material embalaje-Check:TVTY-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[MAGRV] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [MAGRV]  --  -Grupo materiales de material embalaje-Check:TVEGR-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[BEGRU] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BEGRU]  --  -Grupo de autorizaciones-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[EXTWG] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [EXTWG]  --  -Principio activo para materiales farma-Check:TWEW-Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[KZNFM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KZNFM]  --  -Indicador: El material tiene un material reemplazante-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MSTAE] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [MSTAE]  --  -Status material para todos los centros-Check:T141-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[MSTAV] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [MSTAV]  --  -Status de material común a todas las cadenas de distribución-Check:TVMS-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[MSTDE] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [MSTDE]  --  -Fecha a partir de la cual es vál.status mat.común todos ce.-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[MSTDV] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [MSTDV]  --  -Fecha inicio validez status material p.todas cadenas distr.-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[MHDRZ] AS DECIMAL(16,4)),0)  AS [MHDRZ]  --  -Tiempo mínimo de duración restante-Check: -Datatype:DEC-Len:(4,0)
    , ISNULL(CAST(A.[MHDHB] AS DECIMAL(16,4)),0)  AS [MHDHB]  --  -Duración total de conservación-Check: -Datatype:DEC-Len:(4,0)
    , ISNULL(CAST(A.[INHME] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [INHME]  --  -Unidad de medida contenido-Check:T006-Datatype:UNIT-Len:(3,0)
    , ISNULL(CAST(A.[MFRPN] COLLATE DATABASE_DEFAULT AS VARCHAR(40)),'')  AS [MFRPN]  --  -Nº pieza fabricante-Check: -Datatype:CHAR-Len:(40,0)
    , ISNULL(CAST(A.[MFRNR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [MFRNR]  --  -Número de un fabricante-Check:LFA1-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[KZWSM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KZWSM]  --  -Utilización/Clases de unidades de medida-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[IPRKZ] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [IPRKZ]  --  -Indicador período p.fe.caducidad-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[RDMHD] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [RDMHD]  --  -Regla de redondeo p.calcular FPC-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MTPOS_MARA] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [MTPOS_MARA]  --  -Grupo de tipos de posición general-Check:TPTM-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[SLED_BBD] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SLED_BBD]  --  -Fecha de caducidad/Fecha de expiración-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'MARA')}} A
WHERE A.MANDT = '300'
{% if is_incremental() %}
  and A.LAEDA >= '{{last_date}}'
{% else %}
  --and S.LAEDA >= '00000000'
{% endif %}

