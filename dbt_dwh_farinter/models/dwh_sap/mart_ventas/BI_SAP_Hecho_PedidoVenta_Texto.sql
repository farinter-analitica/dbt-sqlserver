
{{ 
    config(
		materialized="view",
		tags=["periodo/diario"],
	) 
}}

SELECT --TOP 100
	T.AnioMes_Id
	, C.TABNAME AS Tabla
	, C.clave_0002 AS Pedido_Id
	, C.clave_0003 AS Posicion_Id
	, T.TDTEXT AS Texto
	, T.TDID AS Texto_Tipo_Id
	, IDT.TDTEXT AS Texto_Tipo_Nombre
	, CASE WHEN C.TABNAME = 'VBAP' THEN 1 ELSE 0 END AS Es_Posicion
	, C.HashStr_0002_0003	
FROM {{ source('DL_FARINTER', 'DL_SAP_STXH') }} AS T
INNER JOIN {{ ref('DL_SAP_STXH_CLAVES') }} AS C
	ON T.[AnioMes_Id] = C.[AnioMes_Id]
	AND T.[TDOBJECT] = C.[TDOBJECT]
	AND T.[TDNAME] = C.[TDNAME]
	AND C.TABNAME IN ('VBAP','VBAK')
LEFT JOIN {{ ref('DL_SAP_TTXIT') }}  AS IDT
ON IDT.TDOBJECT = T.TDOBJECT
AND IDT.TDID = T.TDID
AND IDT.TDSPRAS = 'S'
WHERE T.TDSPRAS = 'S' 