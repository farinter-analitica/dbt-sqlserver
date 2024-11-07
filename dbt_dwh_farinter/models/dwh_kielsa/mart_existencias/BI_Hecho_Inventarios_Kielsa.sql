{% set unique_key_list = ["Emp_Id","Sucursal_Id","Articulo_Id"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
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

SELECT	--TOP (1000)
		EHN.[Emp_Id] [Pais_Id]
		, EHN.Emp_Id [Emp_Id]
		, EHN.EmpSuc_Id [Sucursal_Id]
		, EHN.Sucursal_Id [Sucursal_Id_Solo]
    , EHN.EmpSucBod_Id [Bodega_Id]
    , EHN.[Bodega_Id] [Bodega_Id_Solo]
		, EHN.EmpArticulo_Id [Articulo_Id]
		, EHN.ArticuloPadreHijo_Id [Articulo_Id_Solo]
		, EHN.EmpArticuloPadre_Id[ArticuloPadre_Id]
		, EHN.ArticuloPadre_Id [ArticuloPadre_Id_Solo]
		, EHN.[Cuadro_Id] [Cuadro_Id]
    , EHN.Stock_Id [Stock_Id]
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
WHERE EHN.AnioMes_Id = {{v_anio_mes}} AND EHN.Bodega_Id = 1  