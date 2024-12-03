{% set unique_key_list = ["MATNR","BWKEY","BWTAR"] %}
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
{% set last_year = last_date[0:4]%}
{% set last_month = last_date[4:6]%}

SELECT ISNULL(CAST(A.[MATNR] COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'')  AS [MATNR]  -- X-Número de material-Check:MARA-Datatype:CHAR-Len:(18,0)
    , ISNULL(CAST(A.[BWKEY] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BWKEY]  -- X-Ámbito de valoración-Check:T001K-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[BWTAR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [BWTAR]  -- X-Clase de valoración-Check:*-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[LVORM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LVORM]  --  -Petición-borrado para todos los datos de mat.de una cls-val.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[LBKUM] AS DECIMAL(16,4)),0)  AS [LBKUM]  --  -Stock total valorado-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[SALK3] AS DECIMAL(16,4)),0)  AS [SALK3]  --  -Valor del stock valorado total-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[VPRSV] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [VPRSV]  --  -Indicador de control de precios-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VERPR] AS DECIMAL(16,4)),0)  AS [VERPR]  --  -Precio medio variable/Precio interno periódico-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[STPRS] AS DECIMAL(16,4)),0)  AS [STPRS]  --  -Precio estándar-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[PEINH] AS DECIMAL(16,0)),0)  AS [PEINH]  --  -Cantidad base-Check: -Datatype:DEC-Len:(5,0)
    , ISNULL(CAST(A.[BKLAS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [BKLAS]  --  -Categoría de valoración-Check:T025-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[SALKV] AS DECIMAL(16,4)),0)  AS [SALKV]  --  -ValorEnBaseAlPrecioPromedioVariable(SóloSiControlPrecios S)-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[VMKUM] AS DECIMAL(16,4)),0)  AS [VMKUM]  --  -Stock total valorado del periodo anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VMSAL] AS DECIMAL(16,4)),0)  AS [VMSAL]  --  -Valor de stock total valorado en el periodo anterior-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[VMVPR] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [VMVPR]  --  -Indicador de control de precios del periodo precedente-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VMVER] AS DECIMAL(16,4)),0)  AS [VMVER]  --  -Precio medio variable/Precio int.periódico período anterior-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[VMSTP] AS DECIMAL(16,4)),0)  AS [VMSTP]  --  -Precio estándar el periodo anterior-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[VMPEI] AS DECIMAL(16,0)),0)  AS [VMPEI]  --  -Cantidad base del periodo anterior-Check: -Datatype:DEC-Len:(5,0)
    , ISNULL(CAST(A.[VMBKL] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [VMBKL]  --  -Categoría de valoración del periodo anterior-Check:T025-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[VMSAV] AS DECIMAL(16,4)),0)  AS [VMSAV]  --  -Valor en base al precio medio variable (periodo anterior)-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[VJKUM] AS DECIMAL(16,4)),0)  AS [VJKUM]  --  -Stock total valorado del ejercicio anterior-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VJSAL] AS DECIMAL(16,4)),0)  AS [VJSAL]  --  -Valor del stock total valorado en el año anterior-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[VJVPR] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [VJVPR]  --  -Indicador de control de precios del año precedente-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VJVER] AS DECIMAL(16,4)),0)  AS [VJVER]  --  -Precio medio variable/Precio interno periódico ejer.anterior-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[VJSTP] AS DECIMAL(16,4)),0)  AS [VJSTP]  --  -Precio estándar del año anterior-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[VJPEI] AS DECIMAL(16,0)),0)  AS [VJPEI]  --  -Cantidad base del año anterior-Check: -Datatype:DEC-Len:(5,0)
    , ISNULL(CAST(A.[VJBKL] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [VJBKL]  --  -Categoría de valoración del año anterior-Check:T025-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[VJSAV] AS DECIMAL(16,4)),0)  AS [VJSAV]  --  -Valor en base al precio medio variable (año anterior)-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[LFGJA] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [LFGJA]  --  -Ejercicio del período actual-Check: -Datatype:NUMC-Len:(4,0)
    , ISNULL(CAST(A.[LFMON] COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'')  AS [LFMON]  --  -Período actual (período contable)-Check: -Datatype:NUMC-Len:(2,0)
    , ISNULL(CAST(A.[BWTTY] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [BWTTY]  --  -Tipo de valoración-Check:T149-Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[STPRV] AS DECIMAL(16,4)),0)  AS [STPRV]  --  -Precio anterior-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[LAEPR] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [LAEPR]  --  -Fecha de la útima modificación del precio-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[ZKPRS] AS DECIMAL(16,4)),0)  AS [ZKPRS]  --  -Precio estándar futuro-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[ZKDAT] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ZKDAT]  --  -Inicio de la validez del precio-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[TIMESTAMP] AS DECIMAL(16,0)),0)  AS [TIMESTAMP]  --  -Cronomarcador UTC en forma breve (AAAAMMDDhhmmss)-Check: -Datatype:DEC-Len:(15,0)
    , ISNULL(CAST(A.[BWPRS] AS DECIMAL(16,4)),0)  AS [BWPRS]  --  -Precio de valoración fiscal: Nivel 1-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[BWPRH] AS DECIMAL(16,4)),0)  AS [BWPRH]  --  -Precio de valoración contable: Nivel 1-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[VJBWS] AS DECIMAL(16,4)),0)  AS [VJBWS]  --  -Precio de valoración fiscal: Nivel 3-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[VJBWH] AS DECIMAL(16,4)),0)  AS [VJBWH]  --  -Precio de valoración contable: Nivel 3-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[VVJSL] AS DECIMAL(16,4)),0)  AS [VVJSL]  --  -Valor del stock total valorado del antepenúltimo año-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[VVJLB] AS DECIMAL(16,4)),0)  AS [VVJLB]  --  -Stock total valorado del antepenúltimo año-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VVMLB] AS DECIMAL(16,4)),0)  AS [VVMLB]  --  -Stock total valorado del antepenúltimo periodo-Check: -Datatype:QUAN-Len:(13,3)
    , ISNULL(CAST(A.[VVSAL] AS DECIMAL(16,4)),0)  AS [VVSAL]  --  -Valor de stock total valorado en el antepenúltimo periodo-Check: -Datatype:CURR-Len:(13,2)
    , ISNULL(CAST(A.[ZPLPR] AS DECIMAL(16,4)),0)  AS [ZPLPR]  --  -Precio futuro previsto-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[ZPLP1] AS DECIMAL(16,4)),0)  AS [ZPLP1]  --  -Precio plan. futuro 1-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[ZPLP2] AS DECIMAL(16,4)),0)  AS [ZPLP2]  --  -Precio previsto 2-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[ZPLP3] AS DECIMAL(16,4)),0)  AS [ZPLP3]  --  -Precio previsto 3-Check: -Datatype:CURR-Len:(11,2)
    , ISNULL(CAST(A.[ZPLD1] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ZPLD1]  --  -Fecha de entrada en vigor del precio plan futuro 1-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[ZPLD2] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ZPLD2]  --  -Fecha de entrada en vigor del precio plan futuro 2-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[ZPLD3] COLLATE DATABASE_DEFAULT AS VARCHAR(8)),'')  AS [ZPLD3]  --  -Fecha de entrada en vigor del precio plan futuro 3-Check: -Datatype:DATS-Len:(8,0)
    , ISNULL(CAST(A.[KALKZ] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KALKZ]  --  -Indicador: Cálculo previsional para periodo futuro-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[KALKL] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KALKL]  --  -Indicador: Cálculo previsional para el periodo actual-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[KALKV] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [KALKV]  --  -Indicador: Cálculo previsional para el periodo anterior-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XLIFO] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XLIFO]  --  -Relevante LIFO/FIFO-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MYPOL] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [MYPOL]  --  -Número de pool para valoración LIFO-Check:TPOOL-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[PSTAT] COLLATE DATABASE_DEFAULT AS VARCHAR(15)),'')  AS [PSTAT]  --  -Status de actualización-Check: -Datatype:CHAR-Len:(15,0)
    , ISNULL(CAST(A.[KALN1] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [KALN1]  --  -Número de cálculo del coste - cálculo del coste del producto-Check:*-Datatype:NUMC-Len:(12,0)
    , ISNULL(CAST(A.[KALNR] COLLATE DATABASE_DEFAULT AS VARCHAR(12)),'')  AS [KALNR]  --  -Nº cálculo del coste p/cálculo del coste sin estr.cuantitat.-Check:*-Datatype:NUMC-Len:(12,0)
    , ISNULL(CAST(A.[BWVA1] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [BWVA1]  --  -Variante de valoración para futuros cálculos de coste plan-Check:TCK05-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[BWVA2] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [BWVA2]  --  -Variante de valoración para el cálculo de costes plan actual-Check:TCK05-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[BWVA3] COLLATE DATABASE_DEFAULT AS VARCHAR(3)),'')  AS [BWVA3]  --  -Variante de valoración para el cálculo de costes plan ant.-Check:TCK05-Datatype:CHAR-Len:(3,0)
    , ISNULL(CAST(A.[HRKFT] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [HRKFT]  --  -Grupo de orígenes como subdivisión de la clase de coste-Check:TKKH1-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[KOSGR] COLLATE DATABASE_DEFAULT AS VARCHAR(10)),'')  AS [KOSGR]  --  -Grupo de gastos generales del cálculo de costes-Check:TCK14-Datatype:CHAR-Len:(10,0)
    , ISNULL(CAST(A.[EKALR] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [EKALR]  --  -El material se calcula con estructura cuantitativa-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MLMAA] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MLMAA]  --  -Ledger materiales activado a nivel de material-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MLAST] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MLAST]  --  -Liquidación de ledger de materiales: Control-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[HKMAT] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [HKMAT]  --  -Origen con referencia a material-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[SPERW] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [SPERW]  --  -Indicador de bloqueo para inventario-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[LBWST] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [LBWST]  --  -Estrategia valoración precio plan actual, stock ped.cliente-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[VBWST] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [VBWST]  --  -Estrategia valoración para precio plan anterior, stock indv.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[FBWST] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [FBWST]  --  -Estrategia valoración para precio plan futuro, stock indiv.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[EKLAS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [EKLAS]  --  -Categoría de valoración para stock para pedido de cliente-Check:T025-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[QKLAS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [QKLAS]  --  -Categoría de valoración para stock para proyecto-Check:T025-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[MTUSE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MTUSE]  --  -Utilización del material-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MTORG] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MTORG]  --  -Origen de material-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[OWNPR] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [OWNPR]  --  -Fabricación propia-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[XBEWM] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [XBEWM]  --  -Valoración en base de la unidad de medida específica de lote-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[MBRUE] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [MBRUE]  --  -El reg.MBEWH p.per.ante-anterior del período MBEW ya existe-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(A.[OKLAS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [OKLAS]  --  -Categoría valoración para stock especial en proveedor-Check:T025-Datatype:CHAR-Len:(4,0)
    , ISNULL(CAST(A.[OIPPINV] COLLATE DATABASE_DEFAULT AS VARCHAR(1)),'')  AS [OIPPINV]  --  -Ind.inventario pagado anticip.p.segmento.cl.valoración mat.-Check: -Datatype:CHAR-Len:(1,0)
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'MBEW')}} A
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001K')}} K WITH (NOLOCK)
  ON K.MANDT = A.MANDT
    AND K.BWKEY = A.BWKEY
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'T001')}} S WITH (NOLOCK)
  ON S.MANDT = K.MANDT
    AND S.BUKRS = K.BUKRS
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

