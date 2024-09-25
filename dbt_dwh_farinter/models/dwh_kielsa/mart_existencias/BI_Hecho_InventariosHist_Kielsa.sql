
{{ 
    config(
		materialized="view",
		tags=["periodo/diario", "periodo_unico/si"],
	) 
}}
--dbt dagster

  SELECT	--TOP (1000)
		EHN.[Emp_Id] [Pais_Id]
		, EHN.Emp_Id [Emp_Id]
		, EHN.EmpSuc_Id [Sucursal_Id]
		, EHN.Sucursal_Id [Sucursal_Id_Solo]
    , EHN.EmpSucBod_Id [Bodega_Id]
    , EHN.[Bodega_Id] [Bodega_Id_Solo]
		, EHN.EmpZona_Id [Zona_Id]
		, EHN.EmpDepartamento_Id [Departamento_Id]
		, EHN.EmpMunicipio_Id [Municipio_Id]
		, EHN.EmpCiudad_Id [Ciudad_Id]
		, EHN.EmpTipoSucursal_Id [TipoSucursal_Id]
		, EHN.EmpArticulo_Id [Articulo_Id]
		, EHN.ArticuloPadreHijo_Id [Articulo_Id_Solo]
		, EHN.EmpArticuloPadre_Id[ArticuloPadre_Id]
		, EHN.ArticuloPadre_Id [ArticuloPadre_Id_Solo]
		, EHN.EmpCasa_Id [Casa_Id]
		, EHN.[EmpMarcaArt_Id] [Marca1_Id]
		, EHN.EmpCategoriaArt_Id [CategoriaArt_Id]
		, EHN.[EmpDeptoArt_Id] [DeptoArt_Id]
		, EHN.[EmpCatSubCategoria1Art_Id] [SubCategoria1Art_Id]
		, EHN.EmpCatSubCategoria1_2Art_Id [SubCategoria2Art_Id]
		, EHN.EmpCatSubCategoria1_2_3Art_Id [SubCategoria3Art_Id]
		, EHN.EmpCatSubCategoria1_2_3_4Art_Id [SubCategoria4Art_Id]
		, EHN.Proveedor_Id [Proveedor_Id]
		, EHN.[Cuadro_Id] [Cuadro_Id]
		, EHN.EmpAlertaMecanica_Id [Mecanica_Id]
    , EHN.Stock_Id [Stock_Id]
    , EHN.[Fecha_Id] [Fecha_Id]
    , CAL.Anio_Calendario [Anio_Id]
    , CAL.[AnioMes_Id] [AnioMes_Id]
    , CAL.[Trimestre_Calendario] [Trimestre_Id]
    , CAL.[Mes_Calendario] [Mes_Id]
    , EHN.[Dias_SinStock] [Dias_SinStock]
		, EHN.Cantidad_Existencia [Cantidad_Inventario]
		, EHN.CantidadPadre_Existencia [CantidadPadre_Inventario]
		, EHN.[Valor_Existencia] [Valor_Inventario]
		, EHN.Venta_120 [Venta_120]
    , EHN.Venta_180 [Venta_180]
    , EHN.Venta_240 [Venta_240]
    , EHN.Venta_365 [Venta_365]
		, EHN.Fecha_Actualizado AS [Fecha_Actualizado]
FROM	[DL_FARINTER].[dbo].[DL_Kielsa_ExistenciaHist] EHN --{{ source ('DL_FARINTER', 'DL_Kielsa_ExistenciaHist') }}
INNER JOIN BI_FARINTER.dbo.BI_Dim_Calendario_Dinamico CAL --{{ ref ('BI_Dim_Calendario_Dinamico') }}
	ON CAL.AnioMes_Id = EHN.AnioMes_Id
  AND CAL.Fecha_Id = EHN.[Fecha_Id]
-- INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal SUC
--     ON SUC.Sucursal_Id = EHN.Sucursal_Id
--     AND SUC.Emp_Id = EHN.Emp_Id
--     AND SUC.EmpSuc_Id = EHN.EmpSuc_Id
WHERE CAL.Meses_Desde_Fecha_Procesado >= -36
--CAL.Fecha_Calendario >= '20220101' --DATEFROMPARTS(YEAR(GETDATE())-2, 01, 01)
