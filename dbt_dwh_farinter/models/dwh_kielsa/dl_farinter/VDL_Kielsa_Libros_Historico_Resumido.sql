{{ 
    config(
		tags=["periodo/diario", "periodo_unico/si"],
		materialized="view",
	) 
}}
--AXELL PADILLA -- 20230727 
--Solo editable en dbt DAGSTER
SELECT --TOP 1000
 ULTR.Pais_Id
, LH.Suc_Id AS Ultima_Sucursal_Id
, ULTR.Monedero_Id
, ULTR.Ultimo_Consecutivo
, ULTR.Ultimo_AnioMes_Id
, LH.Vendedor_Id
, ULTR.Ultima_Fecha_Creacion
, LH.Objetivo_Id
, LH.Patologia_Id AS Patologia_1_Id
, PAT1.Enfermedad_Nombre AS Patologia_1_Nombre
, LH.Patologia_2_Id AS Patologia_2_Id
, PAT2.Enfermedad_Nombre AS Patologia_2_Nombre
, LH.Patologia_3_Id AS Patologia_3_Id
, LH.Patologia_4_Id AS Patologia_4_Id
, LH.Patologia_5_Id AS Patologia_5_Id
, LH.PatologiaTexto AS Patologia_Libro
, LHUP.Patologias AS Patologias_Nombre
, LH.Tipo_Id
, LH.Tipo_Registro
FROM
(SELECT --top 1000
	Pais_Id
    , Identidad_Limpia as Monedero_Id
    , MAX(autoid) as Ultimo_Consecutivo
    , MAX(AnioMes_Id) Ultimo_AnioMes_Id
    , MAX(Fecha_Creacion) Ultima_Fecha_Creacion
FROM DL_FARINTER.dbo.DL_Kielsa_Libros_Historico --{{source('DL_FARINTER','DL_Kielsa_Libros_Historico')}}
GROUP BY Pais_Id
    , Identidad_Limpia
) ULTR
INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Libros_Historico LH --{{source('DL_FARINTER','DL_Kielsa_Libros_Historico')}}
ON LH.AnioMes_Id = ULTR.Ultimo_AnioMes_Id
AND LH.autoid = ULTR.Ultimo_Consecutivo
INNER JOIN (
    SELECT autoid, AnioMes_Id, COALESCE(STRING_AGG(Patologia_Nombre,' , '),PatologiaTexto,'No Definido') AS Patologias
    FROM    (
        SELECT autoid, AnioMes_Id, PatologiaTexto,PAT.Enfermedad_Nombre as Patologia_Nombre
        FROM (
            SELECT --top 100 
                autoid, AnioMes_Id,PatologiaTexto, UPV.Patologias_Id,  UPV.Patologia_N
            FROM  DL_FARINTER.dbo.DL_Kielsa_Libros_Historico PV  --{{source('DL_FARINTER','DL_Kielsa_Libros_Historico')}}
            UNPIVOT (Patologias_Id FOR Patologia_N IN ( PV.Patologia_Id
                , PV.Patologia_2_Id
                , PV.Patologia_3_Id
                , PV.Patologia_4_Id
                , PV.Patologia_5_Id
                )) UPV) UPVR
        LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Enfermedades PAT --{{source('DL_FARINTER','DL_Kielsa_Enfermedades')}}
            ON PAT.Enfermedad_Id = UPVR.Patologias_Id
        GROUP BY autoid, AnioMes_Id,PatologiaTexto, PAT.Enfermedad_Nombre
        ) ListaPatologias
    GROUP BY autoid, AnioMes_Id, PatologiaTexto
    ) LHUP
    ON LHUP.AnioMes_Id = ULTR.Ultimo_AnioMes_Id
    AND LHUP.autoid = ULTR.Ultimo_Consecutivo
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Enfermedades PAT1 --{{source('DL_FARINTER','DL_Kielsa_Enfermedades')}}
    ON PAT1.Enfermedad_Id = LH.Patologia_Id
LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Enfermedades PAT2 --{{source('DL_FARINTER','DL_Kielsa_Enfermedades')}}
    ON PAT2.Enfermedad_Id = LH.Patologia_Id
--OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY
--SELECT * FROM VDL_Kielsa_Libros_Historico_Resumido