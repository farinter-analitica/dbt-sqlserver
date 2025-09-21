{% set unique_key_list = ["MATNR","WERKS"] %}
{{ 
    config(
		as_columnstore=False,
    tags=["periodo/diario", "automation/periodo_por_hora","sap_modulo/mm"],
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
    {% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -3, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}
{% set last_year = last_date[0:4] %}
{% set last_month = last_date[4:6] %}

SELECT
    ISNULL(CAST(A.[MATNR] COLLATE DATABASE_DEFAULT AS VARCHAR(18)), '') AS [MATNR],  -- X-Número de material-Check:MARA-Datatype:CHAR-Len:(18,0)
    ISNULL(CAST(A.[WERKS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)), '') AS [WERKS],  -- X-Centro-Check:T001W-Datatype:CHAR-Len:(4,0)
    ISNULL(CAST(A.[PSTAT] COLLATE DATABASE_DEFAULT AS VARCHAR(15)), '') AS [PSTAT],  --  -Status de actualización-Check: -Datatype:CHAR-Len:(15,0)
    ISNULL(CAST(A.[LVORM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [LVORM],  --  -Marcar para borrado material a nivel de centro-Check: -Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[BWTTY] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [BWTTY],  --  -Tipo de valoración-Check:T149C-Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[XCHAR] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [XCHAR],  --  -Indicador para la gestión de lotes (interno)-Check: -Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[MMSTA] COLLATE DATABASE_DEFAULT AS VARCHAR(2)), '') AS [MMSTA],  --  -Status material específico centro-Check:T141-Datatype:CHAR-Len:(2,0)
    ISNULL(CAST(A.[MMSTD] COLLATE DATABASE_DEFAULT AS VARCHAR(8)), '') AS [MMSTD],  --  -Fecha entrada en vigor del status material específico centro-Check: -Datatype:DATS-Len:(8,0)
    ISNULL(CAST(A.[MAABC] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [MAABC],  --  -Indicador ABC-Check:TMABC-Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[EKGRP] COLLATE DATABASE_DEFAULT AS VARCHAR(3)), '') AS [EKGRP],  --  -Grupo de compras-Check:T024-Datatype:CHAR-Len:(3,0)
    ISNULL(CAST(A.[DISPR] COLLATE DATABASE_DEFAULT AS VARCHAR(4)), '') AS [DISPR],  --  -Material: Perfil de planificación de necesidades-Check:MDIP-Datatype:CHAR-Len:(4,0)
    ISNULL(CAST(A.[DISMM] COLLATE DATABASE_DEFAULT AS VARCHAR(2)), '') AS [DISMM],  --  -Característica de planificación de necesidades-Check:T438A-Datatype:CHAR-Len:(2,0)
    ISNULL(CAST(A.[DISPO] COLLATE DATABASE_DEFAULT AS VARCHAR(3)), '') AS [DISPO],  --  -Planificador de necesidades-Check:T024D-Datatype:CHAR-Len:(3,0)
    ISNULL(CAST(A.[PLIFZ] AS DECIMAL(16, 4)), 0) AS [PLIFZ],  --  -Plazo de entrega previsto en días-Check: -Datatype:DEC-Len:(3,0)
    ISNULL(CAST(A.[WEBAZ] AS DECIMAL(16, 4)), 0) AS [WEBAZ],  --  -Tiempo de tratamiento para la entrada de mercancía en días-Check: -Datatype:DEC-Len:(3,0)
    ISNULL(CAST(A.[PERKZ] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [PERKZ],  --  -Indicador de período-Check: -Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[DISLS] COLLATE DATABASE_DEFAULT AS VARCHAR(2)), '') AS [DISLS],  --  -Tamaño de lote de planificación de necesidades-Check:T439A-Datatype:CHAR-Len:(2,0)
    ISNULL(CAST(A.[BESKZ] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [BESKZ],  --  -Clase de aprovisionamiento-Check: -Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[SOBSL] COLLATE DATABASE_DEFAULT AS VARCHAR(2)), '') AS [SOBSL],  --  -Clase de aprovisionamiento especial-Check:T460A-Datatype:CHAR-Len:(2,0)
    ISNULL(CAST(A.[MINBE] AS DECIMAL(16, 4)), 0) AS [MINBE],  --  -Punto de pedido-Check: -Datatype:QUAN-Len:(13,3)
    ISNULL(CAST(A.[EISBE] AS DECIMAL(16, 4)), 0) AS [EISBE],  --  -Stock de seguridad-Check: -Datatype:QUAN-Len:(13,3)
    ISNULL(CAST(A.[BSTRF] AS DECIMAL(16, 4)), 0) AS [BSTRF],  --  -Valor de redondeo de la cantidad a pedir-Check: -Datatype:QUAN-Len:(13,3)
    ISNULL(CAST(A.[MABST] AS DECIMAL(16, 4)), 0) AS [MABST],  --  -Stock máximo-Check: -Datatype:QUAN-Len:(13,3)
    ISNULL(CAST(A.[SBDKZ] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [SBDKZ],  --  -Ind.necesidades secundarias p.nec.colectivas e individuales-Check: -Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[NFMAT] COLLATE DATABASE_DEFAULT AS VARCHAR(18)), '') AS [NFMAT],  --  -Material reemplazante-Check:MARA-Datatype:CHAR-Len:(18,0)
    ISNULL(CAST(A.[MAXLZ] AS DECIMAL(16, 4)), 0) AS [MAXLZ],  --  -Tiempo de almacenaje máximo-Check: -Datatype:DEC-Len:(5,0)
    ISNULL(CAST(A.[LZEIH] COLLATE DATABASE_DEFAULT AS VARCHAR(3)), '') AS [LZEIH],  --  -Unidad para el tiempo máximo de almacenaje-Check:T006-Datatype:UNIT-Len:(3,0)
    ISNULL(CAST(A.[LADGR] COLLATE DATABASE_DEFAULT AS VARCHAR(4)), '') AS [LADGR],  --  -Grupo de carga-Check:TLGR-Datatype:CHAR-Len:(4,0)
    ISNULL(CAST(A.[XCHPF] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [XCHPF],  --  -Indicador: Sujeto a lote-Check: -Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[LGRAD] AS DECIMAL(16, 4)), 0) AS [LGRAD],  --  -Nivel de servicio-Check: -Datatype:DEC-Len:(3,1)
    ISNULL(CAST(A.[MTVFP] COLLATE DATABASE_DEFAULT AS VARCHAR(2)), '') AS [MTVFP],  --  -Grupo de verificación p.verificación de disponibilidad-Check:TMVF-Datatype:CHAR-Len:(2,0)
    ISNULL(CAST(A.[PRCTR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)), '') AS [PRCTR],  --  -Centro de beneficio-Check:*-Datatype:CHAR-Len:(10,0)
    ISNULL(CAST(A.[TRAME] AS DECIMAL(16, 4)), 0) AS [TRAME],  --  -Stock en tránsito-Check: -Datatype:QUAN-Len:(13,3)
    ISNULL(CAST(A.[LOSGR] AS DECIMAL(16, 4)), 0) AS [LOSGR],  --  -Tamaño de lote del cálculo del coste del producto-Check: -Datatype:QUAN-Len:(13,3)
    ISNULL(CAST(A.[DISGR] COLLATE DATABASE_DEFAULT AS VARCHAR(4)), '') AS [DISGR],  --  -Grupo de planificación de necesidades-Check:T438M-Datatype:CHAR-Len:(4,0)
    ISNULL(CAST(A.[ABCIN] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [ABCIN],  --  -Indicador de inventario para recuento cíclico-Check:T159C-Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[RDPRF] COLLATE DATABASE_DEFAULT AS VARCHAR(4)), '') AS [RDPRF],  --  -Perfil de redondeo-Check:RDPR-Datatype:CHAR-Len:(4,0)
    ISNULL(CAST(A.[AUTRU] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [AUTRU],  --  -Anular automáticamente modelo pronóstico-Check: -Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[PREFE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [PREFE],  --  -Indicador de preferencia en inportación/exportación-Check: -Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[PLNTY] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [PLNTY],  --  -Tipo de hoja de ruta-Check:TCA01-Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[MCRUE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)), '') AS [MCRUE],  --  -El reg.MARDH p.per.ante-anterior del per.MARD ya existe-Check: -Datatype:CHAR-Len:(1,0)
    ISNULL(CAST(A.[LFMON] COLLATE DATABASE_DEFAULT AS VARCHAR(2)), '') AS [LFMON],  --  -Período actual (período contable)-Check: -Datatype:NUMC-Len:(2,0)
    ISNULL(CAST(A.[LFGJA] COLLATE DATABASE_DEFAULT AS VARCHAR(4)), '') AS [LFGJA],  --  -Ejercicio del período actual-Check: -Datatype:NUMC-Len:(4,0)
    ISNULL(CAST(GETDATE() AS DATETIME), '19000101') AS [Fecha_Carga],
    ISNULL(CAST(GETDATE() AS DATETIME), '19000101') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'MARC') }} A
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001W') }} W WITH (NOLOCK)
    ON
        W.MANDT = A.MANDT
        AND W.WERKS = A.WERKS
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001K') }} K WITH (NOLOCK)
    ON
        K.MANDT = W.MANDT
        AND K.BWKEY = W.BWKEY
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001') }} S WITH (NOLOCK)
    ON
        S.MANDT = K.MANDT
        AND S.BUKRS = K.BUKRS
WHERE
    S.MANDT = '300'
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

