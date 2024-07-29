{% set unique_key_list = ["WERKS"] %}
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
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}
/*
    full_refresh= true,
*/


SELECT ISNULL(CAST(A.[WERKS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [WERKS]  -- X-Centro-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[NAME1] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [NAME1]  --  -Nombre-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[BWKEY] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BWKEY]  --  -Ámbito de valoración-Check:T001K-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[KUNNR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [KUNNR]  --  -Número de cliente del centro-Check:KNA1-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[LIFNR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [LIFNR]  --  -Número de proveedor del centro-Check:LFA1-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[FABKL] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [FABKL]  --  -Clave de identidad para calendario de fábrica-Check:TFACD-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[NAME2] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [NAME2]  --  -Nombre 2-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[STRAS] COLLATE DATABASE_DEFAULT AS VARCHAR(30)),'')  AS [STRAS]  --  -Calle y número-Check: -Datatype:CHAR-Len:(30,0)
    , ISNULL(CAST(A.[PFACH] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [PFACH]  --  -Apartado-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[PSTLZ] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [PSTLZ]  --  -Código postal-Check: -Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[ORT01] COLLATE DATABASE_DEFAULT AS VARCHAR(25)),'')  AS [ORT01]  --  -Población-Check: -Datatype:CHAR-Len:(25,0)
    , ISNULL(CAST(A.[EKORG] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [EKORG]  --  -Organización de compras-Check:T024E-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[VKORG] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [VKORG]  --  -Organización de ventas para compensación interna-Check:TVKO-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[CHAZV] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [CHAZV]  --  -Indicador: Gestión de estado de lote activo-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[KKOWK] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KKOWK]  --  -Indicador: Condiciones en el nivel de centro-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[KORDB] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KORDB]  --  -Indicador: Sujeto a libro de pedidos-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BEDPL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BEDPL]  --  -Activación de la planificación de necesidades-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[LAND1] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [LAND1]  --  -Clave de país-Check:T005-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[REGIO] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [REGIO]  --  -Región (Estado federal, "land", provincia, condado)-Check:T005S-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[COUNC] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [COUNC]  --  -Código de condado-Check:T005E-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[CITYC] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [CITYC]  --  -Código municipal-Check:T005G-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[ADRNR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [ADRNR]  --  -Dirección-Check:*-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[IWERK] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [IWERK]  --  -Centro de planificación del mantenimiento-Check:T001W-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[TXJCD] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [TXJCD]  --  -Domicilio fiscal-Check:TTXJ-Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[VTWEG] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [VTWEG]  --  -Canal de distribución para compensación interna-Check:TVTW-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[SPART] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [SPART]  --  -Sector para compensación interna-Check:TSPA-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[SPRAS] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SPRAS]  --  -Clave de idioma-Check:T002-Datatype:LANG-Len:(1,0)
    , ISNULL(CAST(A.[WKSOP] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [WKSOP]  --  -Centro SOP-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[AWSLS] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [AWSLS]  --  -Clave de desviación-Check:TKV01-Datatype:CHAR-Len:(6,0)
    , ISNULL(CAST(A.[CHAZV_OLD] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [CHAZV_OLD]  --  -Indicador: Gestión de estado de lote activo-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VLFKZ] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [VLFKZ]  --  -Tipo de centro-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BZIRK] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [BZIRK]  --  -Zona de ventas-Check:T171-Datatype:CHAR-Len:(6,0)
    , ISNULL(CAST(A.[ZONE1] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [ZONE1]  --  -Región de suministro-Check:TZONE-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[TAXIW] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [TAXIW]  --  -Identificador de impuestos centro (Compras)-Check:TMKW1-Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BZQHL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BZQHL]  --  -Considerar proveedor regular-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[LET01] AS DECIMAL(16,0)),0)  AS [LET01]  --  -Número de días para la primera reclamación-Check: -Datatype:DEC-Len:(3,0)
    , ISNULL(CAST(A.[LET02] AS DECIMAL(16,0)),0)  AS [LET02]  --  -Cantidad de días para la segunda reclamación-Check: -Datatype:DEC-Len:(3,0)
    , ISNULL(CAST(A.[LET03] AS DECIMAL(16,0)),0)  AS [LET03]  --  -Cantidad de días para la tercera reclamación-Check: -Datatype:DEC-Len:(3,0)
    , ISNULL(CAST(A.[TXNAM_MA1] COLLATE DATABASE_DEFAULT AS VARCHAR(16)),'')  AS [TXNAM_MA1]  --  -Nombre del texto 1ª reclamación delcrac.proveedor-Check: -Datatype:CHAR-Len:(16,0)
    , ISNULL(CAST(A.[TXNAM_MA2] COLLATE DATABASE_DEFAULT AS VARCHAR(16)),'')  AS [TXNAM_MA2]  --  -Nombres del texto 2ª reclamación de declarac.proveedor-Check: -Datatype:CHAR-Len:(16,0)
    , ISNULL(CAST(A.[TXNAM_MA3] COLLATE DATABASE_DEFAULT AS VARCHAR(16)),'')  AS [TXNAM_MA3]  --  -Nombre del texto 3ª reclamación declarac.proveedor-Check: -Datatype:CHAR-Len:(16,0)
    , ISNULL(CAST(A.[BETOL] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [BETOL]  --  -Nº de días tolerancia pedido - compactación reg. info - UA-Check: -Datatype:NUMC-Len:(3,0)
    , ISNULL(CAST(A.[J_1BBRANCH] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [J_1BBRANCH]  --  -Lugar comercial-Check:J_1BBRANCH-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[VTBFI] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [VTBFI]  --  -Regla para determinación del área de ventas p.traslados-Check: -Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[FPRFW] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [FPRFW]  --  -Perfil de distribución a nivel de centro-Check:TWFPF-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[ACHVM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ACHVM]  --  -Petición de archivo central p.registro maestro-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DVSART] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [DVSART]  --  -Log de lotes: Clase de SGD utilizado-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[NODETYPE] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [NODETYPE]  --  -Tipo de nodo de grafo de cadena logística-Check:MDRP_NODT-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[NSCHEMA] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [NSCHEMA]  --  -Esquema para la formación de nombre-Check:CKMLMV007-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[PKOSA] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [PKOSA]  --  -Conexión de la contabilidad de objetos de coste activa-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MISCH] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MISCH]  --  -Actualización para el cálculo mixto coste activo-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MGVUPD] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MGVUPD]  --  -Actualización para el cálculo coste real activa-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VSTEL] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [VSTEL]  --  -Pto.exped./depto.entrada mcía.-Check:TVST-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[MGVLAUPD] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MGVLAUPD]  --  -Actualización consumo actividad en estructura cuantitativa-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MGVLAREVAL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MGVLAREVAL]  --  -Control de abono de centro de coste-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[SOURCING] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SOURCING]  --  -Acceso a determin.fuente aprovisionamiento mediante ATP-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[OILIVAL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [OILIVAL]  --  -Indicador de valoración de intercambio-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[OIHVTYPE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [OIHVTYPE]  --  -Clase de proveedores (refinería/fábrica/otros) (Brasil)-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[OIHCREDIPI] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [OIHCREDIPI]  --  -Crédito IPI permitido-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[STORETYPE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [STORETYPE]  --  -Tp.tienda p.diferenc.tienda, gran almacén, tienda convenien.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DEP_STORE] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [DEP_STORE]  --  -Grandes almacenes sup.-Check:*-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(S.[BUKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BUKRS]  --  -Código de empresa-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001W')}} A
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001K')}} K WITH (NOLOCK)
  ON K.MANDT = A.MANDT
    AND K.BWKEY = A.BWKEY
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001')}} S WITH (NOLOCK)
  ON S.MANDT = K.MANDT
    AND S.BUKRS = K.BUKRS
WHERE S.MANDT = '300'
  AND S.BUKRS NOT IN ('3100')

