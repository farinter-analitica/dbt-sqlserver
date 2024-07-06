{% set unique_key_list = ["BWKEY"] %}
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


SELECT ISNULL(CAST(A.[BWKEY] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BWKEY]  -- X-Ámbito de valoración-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[BUKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BUKRS]  --  -Sociedad-Check:T001-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[BWMOD] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BWMOD]  --  -Constante de modificación de valoración-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[XBKNG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XBKNG]  --  -Stocks negativos admitidos en área valoración-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MLBWA] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MLBWA]  --  -Ledger materiales activado a nivel de ámbito de valoración-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MLBWV] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MLBWV]  --  -Ledger material activado vinculante a nivel de ámbito val.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XVKBW] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XVKBW]  --  -Valoración precio venta activa-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[ERKLAERKOM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ERKLAERKOM]  --  -Herramienta explicación ledger-material activo/inactivo-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[UPROF] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [UPROF]  --  -Perfil de revaloración precio de venta-Check:TWUP-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[WBPRO] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [WBPRO]  --  -Perfil para gestión de stocks según valor-Check:TWPR-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[MLAST] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MLAST]  --  -Liquidación de ledger de materiales: Control-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MLASV] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MLASV]  --  -Control determinación precio vinculante en área valoración-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BDIFP] AS DECIMAL(16,4)),0)  AS [BDIFP]  --  -Tolerancia de ajuste de stocks-Check: -Datatype:DEC-Len:(5,2)
    , ISNULL(CAST(A.[XLBPD] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XLBPD]  --  -Contab.diferencias precios en EM p.pedido subcontratación-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XEWRX] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XEWRX]  --  -Contabilizar cuenta compras con valor entrada-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[X2FDO] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [X2FDO]  --  -Dos documentos FI en cuenta compras-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[PRSFR] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [PRSFR]  --  -Liberación precios-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MLCCS] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MLCCS]  --  -Indicador estratificación de costes real activa-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XEFRE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XEFRE]  --  -Cost.indirect.adquis.tmb.en dif.precios en cuenta compr.act.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[EFREJ] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [EFREJ]  --  -Inicio validez p.costes indirect.d.adquisic.sobre dif.d.prec-Check: -Datatype:NUMC-Len:(4,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001K')}} A
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001')}} S WITH (NOLOCK)
  ON S.MANDT = A.MANDT
    AND S.BUKRS = A.BUKRS
WHERE S.MANDT = '300'
  AND S.BUKRS NOT IN ('3100')

