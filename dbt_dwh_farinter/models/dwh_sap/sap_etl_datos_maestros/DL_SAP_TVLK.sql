{% set unique_key_list = ["LFART"] %}
{{ 
    config(
		as_columnstore=False,
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
    full_refresh= true,
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


SELECT ISNULL(CAST(A.[LFART] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [LFART]  -- X-Clase de entrega-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[KOPGR] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [KOPGR]  --  -Grupo secuencias de pantallas cabecera de doc.& posición-Check:TVHB-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[UEVOR] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [UEVOR]  --  -Cód.F propuesto para pantalla resumen-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[TXN08] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [TXN08]  --  -Número del texto estándar-Check: -Datatype:CHAR-Len:(8,0)
    , ISNULL(CAST(A.[UMFNG] COLLATE DATABASE_DEFAULT AS VARCHAR(20)),'')  AS [UMFNG]  --  -Alcance de visualización-Check: -Datatype:CHAR-Len:(20,0)
    , ISNULL(CAST(A.[NUMKI] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [NUMKI]  --  -Rango de números en caso de asignación interna-Check: -Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[NUMKE] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [NUMKE]  --  -Rango de números en caso de asignación externa-Check: -Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[NUMKIRULE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [NUMKIRULE]  --  -Regla de determinación de rango de núm.p.LES descentralizado-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[INCPO] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [INCPO]  --  -Incremento del número de la posición en documento comercial-Check: -Datatype:NUMC-Len:(6,0)
    , ISNULL(CAST(A.[PARGR] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [PARGR]  --  -Esquema interlocutor-Check:TVPG-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[AUFER] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [AUFER]  --  -Indicador: Necesidad de pedido anterior como base p.entrega-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DAART] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [DAART]  --  -Clase-pedido por defecto para entregas sin referencia-pedido-Check:TVAK-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[POBED] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [POBED]  --  -Condición para una posición independiente del pedido-Check: -Datatype:NUMC-Len:(3,0)
    , ISNULL(CAST(A.[REGGR] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [REGGR]  --  -Función dummy en la longitud 3-Check: -Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[REGLG] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [REGLG]  --  -Regla para la determinación del almacén de picking-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[WAAUS] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [WAAUS]  --  -Función ficticia de long.1-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[ROUTF] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [ROUTF]  --  -¿Desea determinación de rutas nueva con o sin verif.?-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VBTYP] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [VBTYP]  --  -Tipo de documento comercial-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[TXTGR] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [TXTGR]  --  -Esquema de texto-Check:TTXG-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[ERNAM] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [ERNAM]  --  -Nombre del responsable que ha añadido el objeto-Check: -Datatype:CHAR-Len:(12,0)
    , ISNULL(CAST(A.[KAPPL] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [KAPPL]  --  -Aplicación para condiciones de mensajes-Check:T681A-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[KALSM] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [KALSM]  --  -Esquema para mensajes-Check:T683-Datatype:CHAR-Len:(6,0)
    , ISNULL(CAST(A.[KSCHL] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [KSCHL]  --  -Clase de mensaje-Check:T685-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[STGAK] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [STGAK]  --  -Grupo de estadísticas clase de documento de ventas-Check:TVSF-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[CMGRL] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [CMGRL]  --  -Grupo crédito Entrega-Check:T691D-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[CMGRK] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [CMGRK]  --  -Grupo de crédito Picking-Check:T691D-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[CMGRW] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [CMGRW]  --  -Grupo de crédito Salida de mercancía-Check:T691D-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[QHERK] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [QHERK]  --  -Origen de lote de insp.-Check:TQ31-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[TRSPG] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [TRSPG]  --  -Motivo de bloqueo de transporte-Check:TTSG-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[TDIIX] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [TDIIX]  --  -Indicador de relevancia de transporte para entrega-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[PROFIDNETZ] COLLATE DATABASE_DEFAULT AS VARCHAR(7)),'')  AS [PROFIDNETZ]  --  -Perfil de grafos-Check:TCN41-Datatype:CHAR-Len:(7,0)
    , ISNULL(CAST(A.[EXCOK] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [EXCOK]  --  -Control legal para clase documento ventas activo-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[NEUTE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [NEUTE]  --  -Reprogramación de entregas-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[KALSP] COLLATE DATABASE_DEFAULT AS VARCHAR(6)),'')  AS [KALSP]  --  -Expedición: Esquema de cálculo determ. precio-Check:T683-Datatype:CHAR-Len:(6,0)
    , ISNULL(CAST(A.[FEHGR] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [FEHGR]  --  -Esquema de datos incompletos p.documento comercial-Check:TVUV-Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[LNSPL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LNSPL]  --  -Efectuar partición de entrega según número de almacén-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[AVERP] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [AVERP]  --  -Embalaje automático mediante propuesta de embalaje-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[PM_ITEM_GEN] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [PM_ITEM_GEN]  --  -Generación de posiciones entrega p.material embalaje UMs-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[REGTB] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [REGTB]  --  -Regla para determinar muelle y zona de puesta a disposición-Check: -Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[BZOPS] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BZOPS]  --  -¿Determin.zona puesta a disposición a nivel pos.entrega?-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[EXCBC] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [EXCBC]  --  -Verif.lst.boicot com.interl.estándar, activa p.comerc.ext.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[EXCEM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [EXCEM]  --  -Verif.embargo de interl.estándar, activa p.comerc.exterior-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[EXCLG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [EXCLG]  --  -Indicador: grabar log del control legal-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[J_1ADOCCLS] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [J_1ADOCCLS]  --  -Categ.documento-Check:J_1ADOCCLS-Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[RFPL_SW] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [RFPL_SW]  --  -Switch de plan de itinerarios para clases de entrega-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[TSEGTP] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [TSEGTP]  --  -Máscara de edición segmento de fechas cabecera de entrega-Check:TTSEGTPLH-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[UVEIB] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [UVEIB]  --  -Esquema doc.p.intergridad dat.exportac./importac.-Check:T609B-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[DBTCH] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [DBTCH]  --  -Modo de distribución de la entrega-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[DSFAD] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [DSFAD]  --  -Partición de entrega en interlocutores adicionales-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[SPOFI] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SPOFI]  --  -Regla para determinación de puesto de expedición-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[OIDELDCM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [OIDELDCM]  --  -Ind.activo p.gestión modificaciones documentos p.entregas-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[SPE_NUMKT] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [SPE_NUMKT]  --  -Rango de números para entregas entrantes temporales-Check: -Datatype:CHAR-Len:(2,0)
    , ISNULL(CAST(A.[SPE_NR_RECYCLING] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SPE_NR_RECYCLING]  --  -Activar reutilización números p.entrega no verificada/temp.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BORGR_LIFEX_MUST] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BORGR_LIFEX_MUST]  --  -Entrada obligatoria-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BORGR_LIFEX_UNQ] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BORGR_LIFEX_UNQ]  --  -Univocidad de ID externa-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BORGR_LIFEX_EEDI] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BORGR_LIFEX_EEDI]  --  -Tipo mensaje error si ID transferido por EDI no es unívoco-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BORGR_LIFEX_EDIA] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BORGR_LIFEX_EDIA]  --  -Tipo de mensaje de error si el ID online no es unívoco-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[BORGR_SODET] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BORGR_SODET]  --  -Determinación de pedidos automática-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[HOLD_DATA] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [HOLD_DATA]  --  -Indicador, si se permiten entrg.entrantes c/stat.Retenidas-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[TDID] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [TDID]  --  -ID de texto-Check:*-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[TDSPRAS] AS INT),0) AS [TDSPRAS]  --  -Clave de idioma-Check:*-Datatype:LANG-Len:(1,0)
    , ISNULL(CAST(A.[EDI_WEIGHTVOL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [EDI_WEIGHTVOL]  --  -Transferir peso y volumen del IDOC-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[SPE_ENABLE_VALID] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SPE_ENABLE_VALID]  --  -Enable Validation for All Deliveries within VL60-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MSR_FKARA] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [MSR_FKARA]  --  -Default Billing Type-Check:TVFK-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM {{ var('linked_server') }}.{{ source('SAPPRD', 'TVLK')}} A
WHERE A.MANDT = '300'


