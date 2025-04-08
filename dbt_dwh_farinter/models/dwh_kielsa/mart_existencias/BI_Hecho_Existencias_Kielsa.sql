{% set unique_key_list = ["Emp_Id","Sucursal_Id","Articulo_Id"] %}
{% set current_month_day = modules.datetime.datetime.now().day %}
{{ 
    config(
		as_columnstore=true,
		materialized="incremental",
		tags=["periodo/diario", "periodo/por_hora"],
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		full_refresh=true if current_month_day > 10 and current_month_day < 20 else false,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}
{% set v_fecha = (modules.datetime.datetime.now()).strftime('%Y%m%d') %}
{% set v_anio_mes =  v_fecha[:6]  %}

WITH current_data AS (
	SELECT	--TOP (1000)
			ISNULL(EHN.[Emp_Id],0) [Pais_Id]
			, ISNULL(EHN.Emp_Id,0) [Emp_Id]
			, ISNULL(EHN.EmpSuc_Id,0) [Sucursal_Id]
			, ISNULL(EHN.Sucursal_Id,0) [Sucursal_Id_Solo]
			, EHN.EmpZona_Id [Zona_Id]
			, EHN.EmpDepartamento_Id [Departamento_Id]
			, EHN.EmpMunicipio_Id [Municipio_Id]
			, EHN.EmpCiudad_Id [Ciudad_Id]
			, EHN.EmpTipoSucursal_Id [TipoSucursal_Id]
			, ISNULL(EHN.EmpArticulo_Id,'X') [Articulo_Id]
			, EHN.ArticuloPadreHijo_Id [Articulo_Id_Solo]
			, ISNULL(EHN.EmpArticuloPadre_Id,'X') [ArticuloPadre_Id]
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
			, ART.Alerta_Recomendacion AS [Alerta_Recomendacion_Id]
			, EHN.[Cuadro_Id] [Cuadro_Id]
			, EHN.EmpAlertaMecanica_Id [Mecanica_Id]
			, EHN.Cantidad_Existencia [Cantidad_Existencia]
			, EHN.CantidadPadre_Existencia [CantidadPadre_Existencia]
			, EHN.[Valor_Existencia] [Valor_Existencia]
			, ISNULL(G.Monto,0) [Incentivo_Existencia]
			, GETDATE() AS [Fecha_Actualizado]
	FROM	[DL_FARINTER].[dbo].[DL_Kielsa_ExistenciaHist] EHN --{{ source ('DL_FARINTER', 'DL_Kielsa_ExistenciaHist') }}
	INNER JOIN BI_FARINTER.dbo.BI_Dim_Calendario_Dinamico_Mensual CAL --{{ ref ('BI_Dim_Calendario_Dinamico_Mensual') }}
		ON CAL.AnioMes_Id = EHN.AnioMes_Id
	-- INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal SUC
	--     ON SUC.Sucursal_Id = EHN.Sucursal_Id
	--     AND SUC.Emp_Id = EHN.Emp_Id
	--     AND SUC.EmpSuc_Id = EHN.EmpSuc_Id
	INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo ART --{{ ref ('BI_Kielsa_Dim_Articulo') }}
		ON ART.Articulo_Id = EHN.ArticuloPadreHijo_Id AND ART.Emp_Id = EHN.Emp_Id 
	LEFT JOIN
			(SELECT --top 100
				A.Suc_Id AS Suc_Id
				, A.Articulo_Id AS Articulo_Id
				, B.Comision_Fecha_Inicial AS Inicio
				, B.Comision_Fecha_Final AS Final
				, A.Comision_Monto AS Monto
				, ROW_NUMBER() OVER (partition BY A.Suc_Id, A.Articulo_Id ORDER BY A.Comision_Monto
																				, B.Comision_Fecha_Inicial DESC) AS Ranking
			FROM	[DL_FARINTER].[dbo].DL_Kielsa_Comision_Detalle A --{{ ref ('DL_Kielsa_Comision_Detalle') }}
			INNER JOIN [DL_FARINTER].[dbo].DL_Kielsa_Comision_Encabezado B --{{ ref ('DL_Kielsa_Comision_Encabezado') }}
				ON A.Emp_Id = B.Emp_Id AND A.Comision_Id = B.Comision_Id
			WHERE B.Comision_Estado = 'AP' AND GETDATE() BETWEEN B.Comision_Fecha_Inicial AND B.Comision_Fecha_Final
			) G
		ON EHN.Sucursal_Id = G.Suc_Id AND EHN.ArticuloPadreHijo_Id = G.Articulo_Id AND G.Ranking=1
	WHERE EHN.Bodega_Id = 1 AND EHN.AnioMes_Id = {{v_anio_mes}}
)
{% if is_incremental() %}
-- For incremental loads, combine current data with existing records
SELECT 
    cd.Pais_Id,
    cd.Emp_Id,
    cd.Sucursal_Id,
    cd.Sucursal_Id_Solo,
    cd.Zona_Id,
    cd.Departamento_Id,
    cd.Municipio_Id,
    cd.Ciudad_Id,
    cd.TipoSucursal_Id,
    cd.Articulo_Id,
    cd.Articulo_Id_Solo,
    cd.ArticuloPadre_Id,
    cd.ArticuloPadre_Id_Solo,
    cd.Casa_Id,
    cd.Marca1_Id,
    cd.CategoriaArt_Id,
    cd.DeptoArt_Id,
    cd.SubCategoria1Art_Id,
    cd.SubCategoria2Art_Id,
    cd.SubCategoria3Art_Id,
    cd.SubCategoria4Art_Id,
    cd.Proveedor_Id,
    cd.Alerta_Recomendacion_Id,
    cd.Cuadro_Id,
    cd.Mecanica_Id,
    cd.Cantidad_Existencia,
    cd.CantidadPadre_Existencia,
    cd.Valor_Existencia,
    cd.Incentivo_Existencia,
    cd.Fecha_Actualizado
FROM current_data cd

UNION ALL

SELECT 
    existing.Pais_Id,
    existing.Emp_Id,
    existing.Sucursal_Id,
    existing.Sucursal_Id_Solo,
    existing.Zona_Id,
    existing.Departamento_Id,
    existing.Municipio_Id,
    existing.Ciudad_Id,
    existing.TipoSucursal_Id,
    existing.Articulo_Id,
    existing.Articulo_Id_Solo,
    existing.ArticuloPadre_Id,
    existing.ArticuloPadre_Id_Solo,
    existing.Casa_Id,
    existing.Marca1_Id,
    existing.CategoriaArt_Id,
    existing.DeptoArt_Id,
    existing.SubCategoria1Art_Id,
    existing.SubCategoria2Art_Id,
    existing.SubCategoria3Art_Id,
    existing.SubCategoria4Art_Id,
    existing.Proveedor_Id,
    existing.Alerta_Recomendacion_Id,
    existing.Cuadro_Id,
    existing.Mecanica_Id,
    0 AS Cantidad_Existencia, -- Set to zero for old records not in current data
    0 AS CantidadPadre_Existencia, -- Set to zero for old records not in current data
    0 AS Valor_Existencia, -- Set to zero for old records not in current data
    0 AS Incentivo_Existencia, -- Set to zero for old records not in current data
    GETDATE() AS Fecha_Actualizado
FROM {{ this }} existing
LEFT JOIN current_data cd 
    ON cd.Emp_Id = existing.Emp_Id 
    AND cd.Sucursal_Id = existing.Sucursal_Id 
    AND cd.Articulo_Id = existing.Articulo_Id
WHERE cd.Emp_Id IS NULL -- Records in existing but not in current_data

{% else %}
-- For full refresh, just get the current data
SELECT 
    Pais_Id,
    Emp_Id,
    Sucursal_Id,
    Sucursal_Id_Solo,
    Zona_Id,
    Departamento_Id,
    Municipio_Id,
    Ciudad_Id,
    TipoSucursal_Id,
    Articulo_Id,
    Articulo_Id_Solo,
    ArticuloPadre_Id,
    ArticuloPadre_Id_Solo,
    Casa_Id,
    Marca1_Id,
    CategoriaArt_Id,
    DeptoArt_Id,
    SubCategoria1Art_Id,
    SubCategoria2Art_Id,
    SubCategoria3Art_Id,
    SubCategoria4Art_Id,
    Proveedor_Id,
    Alerta_Recomendacion_Id,
    Cuadro_Id,
    Mecanica_Id,
    Cantidad_Existencia,
    CantidadPadre_Existencia,
    Valor_Existencia,
    Incentivo_Existencia,
    Fecha_Actualizado
FROM current_data
{% endif %}