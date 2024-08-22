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


SELECT ISNULL(CAST(A.[PRCTR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [PRCTR]  -- X-Centro de beneficio-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[DATBI] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [DATBI]  -- X-Fecha fin validez-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[KOKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [KOKRS]  -- X-Sociedad CO-Check:TKA01-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[DATAB] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [DATAB]  --  -Fecha inicio validez-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[ERSDA] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ERSDA]  --  -Fecha entrada-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[USNAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [USNAM]  --  -Nombre del autor-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[MERKMAL] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [MERKMAL]  --  -Nombre del campo de una característica del CO-PA-Check:*-Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[ABTEI] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [ABTEI]  --  -Departamento-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[VERAK] COLLATE DATABASE_DEFAULT AS VARCHAR(20)),'')  AS [VERAK]  --  -Responsable del centro de beneficio-Check: -Datatype:CHAR-Len:(20,0)
    , ISNULL(CAST(A.[VERAK_USER] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [VERAK_USER]  --  -Usuario responsable del centro de beneficio-Check:USR02-Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[WAERS] COLLATE DATABASE_DEFAULT AS VARCHAR(5)),'')  AS [WAERS]  --  -Clave de moneda-Check:TCURC-Datatype:CUKY-Len:(5,0)
    , ISNULL(CAST(A.[NPRCTR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [NPRCTR]  --  -Profit center sucesor-Check:*-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[LAND1] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [LAND1]  --  -Clave de país-Check:T005-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[ANRED] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [ANRED]  --  -Tratamiento-Check: -Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[NAME1] COLLATE DATABASE_DEFAULT AS VARCHAR(35)),'')  AS [NAME1]  --  -Nombre 1-Check: -Datatype:CHAR-Len:(35,0)
    , ISNULL(CAST(A.[NAME2] COLLATE DATABASE_DEFAULT AS VARCHAR(35)),'')  AS [NAME2]  --  -Nombre 2-Check: -Datatype:CHAR-Len:(35,0)
    , ISNULL(CAST(A.[NAME3] COLLATE DATABASE_DEFAULT AS VARCHAR(35)),'')  AS [NAME3]  --  -Nombre 3-Check: -Datatype:CHAR-Len:(35,0)
    , ISNULL(CAST(A.[NAME4] COLLATE DATABASE_DEFAULT AS VARCHAR(35)),'')  AS [NAME4]  --  -Nombre 4-Check: -Datatype:CHAR-Len:(35,0)
    , ISNULL(CAST(A.[ORT01] COLLATE DATABASE_DEFAULT AS VARCHAR(35)),'')  AS [ORT01]  --  -Población-Check: -Datatype:CHAR-Len:(35,0)
    , ISNULL(CAST(A.[ORT02] COLLATE DATABASE_DEFAULT AS VARCHAR(35)),'')  AS [ORT02]  --  -Distrito-Check: -Datatype:CHAR-Len:(35,0)
    , ISNULL(CAST(A.[STRAS] COLLATE DATABASE_DEFAULT AS VARCHAR(35)),'')  AS [STRAS]  --  -Calle y nº-Check: -Datatype:CHAR-Len:(35,0)
    , ISNULL(CAST(A.[PFACH] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [PFACH]  --  -Apartado-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[PSTLZ] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [PSTLZ]  --  -Código postal-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[PSTL2] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [PSTL2]  --  -Código postal del apartado-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[SPRAS] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SPRAS]  --  -Clave de idioma-Check:T002-Datatype:LANG-Len:(1,0)
    , ISNULL(CAST(A.[TELBX] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [TELBX]  --  -Número de telebox-Check: -Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[TELF1] COLLATE DATABASE_DEFAULT AS VARCHAR(16)),'')  AS [TELF1]  --  -1º número de teléfono-Check: -Datatype:CHAR-Len:(16,0)
    , ISNULL(CAST(A.[TELF2] COLLATE DATABASE_DEFAULT AS VARCHAR(16)),'')  AS [TELF2]  --  -Nº de teléfono 2-Check: -Datatype:CHAR-Len:(16,0)
    , ISNULL(CAST(A.[TELFX] COLLATE DATABASE_DEFAULT AS VARCHAR(31)),'')  AS [TELFX]  --  -Nº telefax-Check: -Datatype:CHAR-Len:(31,0)
    , ISNULL(CAST(A.[TELTX] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [TELTX]  --  -Número de teletex-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[TELX1] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [TELX1]  --  -Número de télex-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[DATLT] COLLATE DATABASE_DEFAULT AS VARCHAR(14)),'')  AS [DATLT]  --  -Nº línea transmisión de datos-Check: -Datatype:CHAR-Len:(14,0)
    , ISNULL(CAST(A.[DRNAM] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [DRNAM]  --  -Nombre de impresora p.CeBe-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[KHINR] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [KHINR]  --  -Área de centros de beneficio-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[BUKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BUKRS]  --  -Sociedad-Check:T001-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[VNAME] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [VNAME]  --  -Joint Venture-Check:T8JV-Datatype:CHAR-Len:(6,0)
    , ISNULL(CAST(A.[RECID] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [RECID]  --  -Tipo de coste-Check:T8JJ-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[ETYPE] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [ETYPE]  --  -Clase de participación-Check:T8JE-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[TXJCD] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [TXJCD]  --  -Domicilio fiscal-Check:TTXJ-Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[REGIO] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [REGIO]  --  -Región (Estado federal, "land", provincia, condado)-Check:T005S-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[KVEWE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KVEWE]  --  -Utilización de tabla de condiciones-Check:T681V-Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[KAPPL] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [KAPPL]  --  -Aplicación-Check:T681A-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[KALSM] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [KALSM]  --  -Esquema (determ.precio, mensajes, determ.cuentas,...)-Check:T683-Datatype:CHAR-Len:(6,0)
    , ISNULL(CAST(A.[LOGSYSTEM] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [LOGSYSTEM]  --  -Sistema lógico-Check:TBDLS-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[LOCK_IND] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LOCK_IND]  --  -Indicador de bloqueo-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[PCA_TEMPLATE] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [PCA_TEMPLATE]  --  -Modelo para la planificación fórmulas en CeBe-Check:COTPL-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[SEGMENT] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [SEGMENT]  --  -Segmento para reporting de segmento-Check:FAGL_SEGM-Datatype:CHAR-Len:(10,0)    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'CEPC')}} A
--No llevan sociedad estos
--INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001')}} S
--ON A.MANDT = S.MANDT
--  AND A.BUKRS = S.BUKRS		
WHERE A.MANDT = '300'
--  AND S.OPVAR LIKE 'Z%'	

