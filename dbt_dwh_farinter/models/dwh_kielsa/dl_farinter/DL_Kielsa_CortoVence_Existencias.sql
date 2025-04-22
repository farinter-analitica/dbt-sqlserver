{% set unique_key_list = ["Sucursal_Id","Articulo_Id","Emp_Id",] %}
{% set current_month_day = modules.datetime.datetime.now().day %}
{{ 
    config(
		as_columnstore=true,
		materialized="incremental",
		tags=["periodo/diario", ],
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		full_refresh=true if not is_incremental() or (current_month_day > 10 and current_month_day < 20) else false,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}
{%- if is_incremental() %}
	{%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101') from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}
{%- set v_anio_mes = modules.datetime.datetime.now().strftime('%Y%m') %}


WITH current_data AS (
	SELECT	--TOP (1000)
			ISNULL(EHN.[Emp_Id],0) [Pais_Id]
			, ISNULL(EHN.Emp_Id,0) [Emp_Id]
			, ISNULL(EHN.EmpSuc_Id,0) [EmpSuc_Id]
			, ISNULL(EHN.Sucursal_Id,0) [Sucursal_Id]
			, ISNULL(EHN.EmpArticulo_Id,'X') [EmpArticulo_Id]
			, EHN.ArticuloPadreHijo_Id [Articulo_Id]
			, ISNULL(EHN.EmpArticuloPadre_Id,'X') [EmpArticuloPadre_Id]
			, EHN.ArticuloPadre_Id [ArticuloPadre_Id]
            , Art.Casa_Id [Casa_Id]
			, EHN.EmpCasa_Id [EmpCasa_Id]
			, EHN.Cantidad_Existencia [Cantidad_Existencia]
			, EHN.CantidadPadre_Existencia [CantidadPadre_Existencia]
			, EHN.[Valor_Existencia] [Valor_Existencia]
			, GETDATE() AS [Fecha_Actualizado]
	FROM	[DL_FARINTER].[dbo].[DL_Kielsa_ExistenciaHist] EHN --{{ source ('DL_FARINTER', 'DL_Kielsa_ExistenciaHist') }}
	INNER JOIN BI_FARINTER.dbo.BI_Dim_Calendario_Dinamico_Mensual CAL --{{ ref ('BI_Dim_Calendario_Dinamico_Mensual') }}
		ON CAL.AnioMes_Id = EHN.AnioMes_Id
	-- INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Sucursal SUC
	--     ON SUC.Sucursal_Id = EHN.Sucursal_Id
	--     AND SUC.Emp_Id = EHN.Emp_Id
	--     AND SUC.EmpSuc_Id = EHN.EmpSuc_Id
	INNER JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Articulo ART --{{ ref ('BI_Kielsa_Dim_Articulo') }}
		ON ART.Articulo_Id = EHN.ArticuloPadreHijo_Id 
        AND ART.Emp_Id = EHN.Emp_Id 
	WHERE EHN.Bodega_Id = 1 
    AND EHN.AnioMes_Id = {{v_anio_mes}}
)
{% if is_incremental() %}
-- For incremental loads, combine current data with existing records
SELECT 
    cd.Pais_Id,
    cd.Emp_Id,
    cd.Sucursal_Id,
    cd.EmpSuc_Id,
    cd.Articulo_Id,
    cd.EmpArticulo_Id,
    cd.ArticuloPadre_Id,
    cd.EmpArticuloPadre_Id,
    cd.Casa_Id,
    cd.Cantidad_Existencia,
    cd.CantidadPadre_Existencia,
    cd.Fecha_Actualizado
FROM current_data cd

UNION ALL

SELECT 
    existing.Pais_Id,
    existing.Emp_Id,
    existing.Sucursal_Id,
    existing.EmpSuc_Id,
    existing.Articulo_Id,
    existing.EmpArticulo_Id,
    existing.ArticuloPadre_Id,
    existing.EmpArticuloPadre_Id,
    existing.Casa_Id,
    0 AS Cantidad_Existencia, -- Set to zero for old records not in current data
    0 AS CantidadPadre_Existencia, -- Set to zero for old records not in current data
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
    EmpSuc_Id,
    Articulo_Id,
    EmpArticulo_Id,
    ArticuloPadre_Id,
    EmpArticuloPadre_Id,
    Casa_Id,
    Cantidad_Existencia,
    CantidadPadre_Existencia,
    Fecha_Actualizado
FROM current_data
{% endif %}