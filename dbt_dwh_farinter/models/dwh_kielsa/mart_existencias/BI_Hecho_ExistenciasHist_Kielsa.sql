
{{ 
    config(
		materialized="view",
		tags=["periodo/diario", "periodo/por_hora"],
	) 
}}
--dbt dagster

SELECT --top 100
    CAL.Anio_Calendario [Anio_Id]
      ,CAL.[Mes_Calendario] [Mes_Id]
      ,CAL.[AnioMes_Id]
      ,EHN.[Fecha_Id]
      ,EHN.[Emp_Id]
      ,EHN.[Emp_Id] [Pais_Id]
      ,EHN.EmpSuc_Id [Sucursal_Id]
      ,EHN.Sucursal_Id [Sucursal_Id_Solo]
      ,EHN.EmpArticuloPadre_Id [ArticuloPadre_Id]
      ,EHN.ArticuloPadre_Id [ArticuloPadre_Id_Solo]
      ,EHN.[Stock_Id]
      ,EHN.CantidadPadre_Existencia  [Existencia_Cantidad]
      ,EHN.Valor_Existencia  [Existencia_Valor]
      ,EHN.[Dias_SinStock]
    FROM	[DL_FARINTER].[dbo].[DL_Kielsa_ExistenciaHist] EHN --{{ source ('DL_FARINTER', 'DL_Kielsa_ExistenciaHist') }}
	INNER JOIN BI_FARINTER.dbo.BI_Dim_Calendario_Dinamico_Mensual CAL --{{ ref ('BI_Dim_Calendario_Dinamico_Mensual') }}
		ON CAL.AnioMes_Id = EHN.AnioMes_Id
    -- INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal SUC
    --     ON SUC.Sucursal_Id = EHN.Sucursal_Id
    --     AND SUC.Emp_Id = EHN.Emp_Id
    --     AND SUC.EmpSuc_Id = EHN.EmpSuc_Id
    -- INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo ART
    --     ON ART.Articulo_Id = EHN.ArticuloPadre_Id
    --     AND ART.Emp_Id = EHN.Emp_Id
    --     AND ART.EmpArt_Id = EHN.EmpArticuloPadre_Id
	WHERE EHN.Bodega_Id = 1
    AND CAL.Meses_Desde_Fecha_Procesado >=-24
    --WHERE CAL.AnioMes_Id =  202409 AND E.Anio_Id=2024 AND E.Mes_Id = 9
