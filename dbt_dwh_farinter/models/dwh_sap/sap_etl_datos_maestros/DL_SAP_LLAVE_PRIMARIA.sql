{% set unique_key_list = ["TABNAME","FIELDNAME"] %}
{{ 
    config(
		as_columnstore=False,
        tags=["periodo/diario","sap_modulo/basis"],
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
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), create_clustered=true, columns=['TABNAME','POSITION']) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}

/*

"SELECT TOP 10 ISNULL(CAST(A.[TABNAME] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [TABNAME]  -- X-Nombre de tabla-Check:DD02L-Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[FIELDNAME] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [FIELDNAME]  -- X-Nombre campo-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[FLDSTAT] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [FLDSTAT]  -- X-Status de activación de un objeto del Repository-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[ROLLNAME] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [ROLLNAME]  -- X-Elemento de datos (dominio semántico)-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[ROLLSTAT] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ROLLSTAT]  -- X-Status de activación de un objeto del Repository-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DOMNAME] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [DOMNAME]  -- X-Denominación de un dominio-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[DOMSTAT] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [DOMSTAT]  -- X-Status de activación de un objeto del Repository-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[TEXTSTAT] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [TEXTSTAT]  -- X-Status de activación de un objeto del Repository-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DDLANGUAGE] AS INT),0) AS [DDLANGUAGE]  -- X-Clave de idioma-Check: -Datatype:LANG-Len:(1,0)
    , ISNULL(CAST(A.[POSITION] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [POSITION]  -- X-Posición de un campo en la tabla-Check: -Datatype:NUMC-Len:(4,0)
    , ISNULL(CAST(A.[KEYFLAG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KEYFLAG]  -- X-Marca un campo clave de una tabla-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MANDATORY] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MANDATORY]  -- X-Indica campo obligatorio (NOT BLANK)-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[CHECKTABLE] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [CHECKTABLE]  -- X-Nombre de la tabla de verificación de la clave externa-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[ADMINFIELD] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ADMINFIELD]  -- X-Nivel de anidamiento en Includes-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[INTTYPE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [INTTYPE]  -- X-Tipo de datos ABAP (C,D,N...)-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[INTLEN] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [INTLEN]  -- X-Longitud interna en byte-Check: -Datatype:NUMC-Len:(6,0)
    , ISNULL(CAST(A.[REFTABLE] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [REFTABLE]  -- X-Tabla de referencia del campo-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[PRECFIELD] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [PRECFIELD]  -- X-Nombre de la tabla incluida-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[REFFIELD] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [REFFIELD]  -- X-Campo referencia para campos de moneda y de cantidades-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[CONROUT] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [CONROUT]  -- X-Módulo de verificación o de generación para campos-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[ROUTPUTLEN] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [ROUTPUTLEN]  -- X-Longitud (cantidad de caracteres)-Check: -Datatype:NUMC-Len:(6,0)
    , ISNULL(CAST(A.[MEMORYID] COLLATE DATABASE_DEFAULT AS VARCHAR(20)),'')  AS [MEMORYID]  -- X-ID parámetro Set/Get-Check:TPARA-Datatype:CHAR-Len:(20,0)
    , ISNULL(CAST(A.[LOGFLAG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LOGFLAG]  -- X-Indicador para crear documentos de modificación-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[HEADLEN] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [HEADLEN]  -- X-longitud máxima de cabecera-Check: -Datatype:NUMC-Len:(2,0)
    , ISNULL(CAST(A.[SCRLEN1] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [SCRLEN1]  -- X-Longitud máxima del denominador de campo breve-Check: -Datatype:NUMC-Len:(2,0)
    , ISNULL(CAST(A.[SCRLEN2] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [SCRLEN2]  -- X-Longitud máxima del denominador de campo mediano-Check: -Datatype:NUMC-Len:(2,0)
    , ISNULL(CAST(A.[SCRLEN3] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [SCRLEN3]  -- X-Longitud máxima del denominador de campo largo-Check: -Datatype:NUMC-Len:(2,0)
    , ISNULL(CAST(A.[DTELGLOBAL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [DTELGLOBAL]  -- X-Indic.objetos Dict privados (no utilizado)-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DTELMASTER] AS INT),0) AS [DTELMASTER]  -- X-Idioma original en objetos de Repository-Check: -Datatype:LANG-Len:(1,0)
    , ISNULL(CAST(A.[RESERVEDTE] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [RESERVEDTE]  -- X-SDIC: Reserva para elementos de datos (no utiliz.)-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[DATATYPE] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [DATATYPE]  -- X-Tipo de datos en Dictionary ABAP-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[LENG] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [LENG]  -- X-Longitud (cantidad de caracteres)-Check: -Datatype:NUMC-Len:(6,0)
    , ISNULL(CAST(A.[OUTPUTLEN] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [OUTPUTLEN]  -- X-Longitud de salida-Check: -Datatype:NUMC-Len:(6,0)
    , ISNULL(CAST(A.[DECIMALS] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [DECIMALS]  -- X-Cantidad de decimales-Check: -Datatype:NUMC-Len:(6,0)
    , ISNULL(CAST(A.[LOWERCASE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LOWERCASE]  -- X-Permitido/No permitido utilizar letras minúsculas-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[SIGNFLAG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SIGNFLAG]  -- X-Visualizar signo +/- para campos numéricos-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[LANGFLAG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LANGFLAG]  -- X-Indicador p.valores supeditados a clave idioma (no usado)-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VALEXI] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [VALEXI]  -- X-Existencia de valores fijos-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[ENTITYTAB] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [ENTITYTAB]  -- X-Tabla de valores-Check:DD02L-Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[CONVEXIT] COLLATE DATABASE_DEFAULT AS VARCHAR(5)),'')  AS [CONVEXIT]  -- X-Rutina de conversión-Check: -Datatype:CHAR-Len:(5,0)
    , ISNULL(CAST(A.[MASK] COLLATE DATABASE_DEFAULT AS VARCHAR(20)),'')  AS [MASK]  -- X-Máscara edición (no utiliz.)-Check: -Datatype:CHAR-Len:(20,0)
    , ISNULL(CAST(A.[MASKLEN] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [MASKLEN]  -- X-Longitud de la máscara de edición (no utilizada)-Check: -Datatype:NUMC-Len:(4,0)
    , ISNULL(CAST(A.[ACTFLAG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ACTFLAG]  -- X-Indicador de activación-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DOMMASTER] AS INT),0) AS [DOMMASTER]  -- X-Idioma original en objetos de Repository-Check: -Datatype:LANG-Len:(1,0)
    , ISNULL(CAST(A.[RESERVEDOM] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [RESERVEDOM]  -- X-Reserva para dominios (no utilizada)-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[DOMGLOBAL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [DOMGLOBAL]  -- X-Indic.objetos Dict privados (no utilizado)-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DDTEXT] COLLATE DATABASE_DEFAULT AS VARCHAR(60)),'')  AS [DDTEXT]  -- X-Descripción breve de objetos de Repository-Check: -Datatype:CHAR-Len:(60,0)
    , ISNULL(CAST(A.[REPTEXT] COLLATE DATABASE_DEFAULT AS VARCHAR(55)),'')  AS [REPTEXT]  -- X-Cabecera-Check: -Datatype:CHAR-Len:(55,0)
    , ISNULL(CAST(A.[SCRTEXT_S] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [SCRTEXT_S]  -- X-Denominador de campo breve-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[SCRTEXT_M] COLLATE DATABASE_DEFAULT AS VARCHAR(20)),'')  AS [SCRTEXT_M]  -- X-Denominador de campo mediano-Check: -Datatype:CHAR-Len:(20,0)
    , ISNULL(CAST(A.[SCRTEXT_L] COLLATE DATABASE_DEFAULT AS VARCHAR(40)),'')  AS [SCRTEXT_L]  -- X-Denominador de campo largo-Check: -Datatype:CHAR-Len:(40,0)
   
FROM SAPQA.QAS.qas.DD03M A"

*/
{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -3, max(Fecha_Actualizado)), 112), '00000000')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}

SELECT
	 ISNULL(CAST(DD03L.TABNAME COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'') AS TABNAME
    , ISNULL(CAST(DD03L.FIELDNAME COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'') AS FIELDNAME
    , ISNULL(CAST(DD03L.POSITION COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'') AS POSITION
    , ISNULL(CAST(DD03L.KEYFLAG COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'') AS KEYFLAG
    , ISNULL(CAST(DD03L.DATATYPE COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'') AS DATATYPE
    , ISNULL(CAST(DD03L.LENG COLLATE DATABASE_DEFAULT AS INT),0) AS LENG
    , ISNULL(CAST(DD03L.DECIMALS COLLATE DATABASE_DEFAULT AS INT),0) AS DECIMALS
    , ISNULL(CAST(DD03L.CHECKTABLE COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'') AS CHECKTABLE
    , ISNULL(CAST(DD03M.DOMNAME COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'') AS DOMNAME
    , ISNULL(CAST(DD03M.DDTEXT COLLATE DATABASE_DEFAULT AS VARCHAR(60)),'') AS DDTEXT
    , ISNULL(CAST(DD03M.REPTEXT COLLATE DATABASE_DEFAULT AS VARCHAR(55)),'') AS REPTEXT
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM	{{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD','DD03L')}} DD03L WITH (NOLOCK)
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD','DD02L')}} DD02L WITH (NOLOCK)
    ON DD03L.TABNAME = DD02L.TABNAME AND DD03L.AS4LOCAL = DD02L.AS4LOCAL --AND DD03L.AS4VERS = DD02L.AS4VERS
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD','DD03M')}} DD03M WITH (NOLOCK)
    ON DD03L.TABNAME = DD03M.TABNAME AND DD03L.FIELDNAME = DD03M.FIELDNAME AND DD03L.POSITION = DD03M.POSITION 
    AND DD03L.AS4LOCAL = DD03M.FLDSTAT 
    AND DD02L.AS4LOCAL = 'A'
WHERE 
{%- if is_incremental() %}
    1=0
    /*AND DD03L.TABNAME
    IN (SELECT TDOBJECT 
        FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD','STXH')}} STXH WITH (NOLOCK)
        WHERE STXH.MANDT = '300'
            AND STXH.TDLDATE > '{{ last_date }}'
        GROUP BY TDOBJECT)*/
{% else %}
    DD03L.KEYFLAG = 'X'     
    AND DD02L.TABNAME NOT LIKE '/%' 
    AND DD02L.TABCLASS = 'TRANSP' 
    AND DD03M.DDLANGUAGE = 'S'
    AND DD02L.AS4LOCAL = 'A'
{% endif %}
;
