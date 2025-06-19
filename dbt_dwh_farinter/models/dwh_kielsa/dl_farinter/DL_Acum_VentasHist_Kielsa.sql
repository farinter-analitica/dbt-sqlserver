{%- set unique_key_list = ["AnioMes_Id",
		"Fecha_Id",
		"Pais_Id",
		"Sucursal_Id",
		"Articulo_Id",
		"Factura_Id",
		"Hora_Id"] -%}
{# Crear columnstore con macro personalizada para poder usar las particiones. #}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario", "periodo_unico/si"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		on_clause_filegroup = 'ps_' + this.identifier + "_fecha([Fecha_Id])",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		pre_hook=[
			"{%- if is_incremental() -%}
				 --Incremental
			{%- else -%}
					EXEC ADM_FARINTER.dbo.pa_inicializar_particiones
						@p_base_datos = '{{this.database}}',
						@p_nombre_esquema_particion = 'ps_{{this.identifier}}_fecha',
						@p_nombre_funcion_particion = 'pf_{{this.identifier}}_fecha',
						@p_periodo_tipo = 'Mensual', --'Anual' o 'Mensual'
						@p_tipo_datos = 'FechaHora', --'Fecha' o 'AnioMes' o 'FechaHora'
						@p_fecha_base = '2018-01-01'
			{%- endif -%}"
		],
		post_hook=[
		"{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(),
			if_another_exists_drop_it=true) }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", 
			create_clustered=false, 
			is_incremental=is_incremental(), 
			if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado'], included_columns=['Fecha_Id']) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Id']) }}",
		"EXEC ADM_FARINTER.dbo.pa_comprimir_indices_particiones_anteriores 
			@p_base_datos = '{{this.database}}',
		 	@p_esquema_tabla = '{{this.schema}}', 
			@p_nombre_tabla = '{{this.identifier}}', 
			@p_tipo_datos = 'Fecha';"		
			]
		
) }}
--Solo venta identificada con monedero valido

{% if is_incremental() %}
	{% set v_last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{% else %}
	{% set v_last_date = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=365*4)).replace(day=1,month=1).strftime('%Y%m%d') %}
{% endif %}
{% set v_anio_mes_inicio =  v_last_date[:6]  %}
{% set v_anio_inicio =  v_last_date[:4]  %}
{% set v_mes_inicio =  v_last_date[4:6]  %}
SELECT ISNULL(Pais_Id, 0) AS Pais_Id,
	Marca_Id,
	ISNULL(Sucursal_Id, 0) AS Sucursal_Id,
	Zona_Id,
	Departamento_Id,
	Municipio_Id,
	Ciudad_Id,
	TipoSucursal_Id,
	Monedero_Id,
	TipoMonedero_Id,
	GeneroMonedero_Id,
	RangoEdadMonedero_Id,
	Plan_Id,
	Cliente_Id,
	TipoCliente_Id,
	Empleado_Id,
	ISNULL(Articulo_Id,'') AS Articulo_Id,
	ArticuloPadre_Id,
	Casa_Id,
	Marca1_Id,
	CategoriaArt_Id,
	DeptoArt_Id,
	SubCategoria1Art_Id,
	SubCategoria2Art_Id,
	SubCategoria3Art_Id,
	SubCategoria4Art_Id,
	Proveedor_Id,
	CanalVenta_Id,
	Documento_Id,
	SubDocumento_Id,
	FacturaEstado_Id,
	ISNULL(Factura_Id,'') AS Factura_Id,
	Cuadro_Id,
	Mecanica_Id,
	ISNULL(Fecha_Id, '19000101') AS Fecha_Id,
	Anio_Id,
	Trimestre_Id,
	Mes_Id,
	Dias_Id,
	Dia_Id,
	ISNULL(Hora_Id,0) AS Hora_Id,
	Cantidad,
	Cantidad_Padre,
	Venta_Bruta,
	Venta_Neta,
	Utilidad,
	Costo,
	Descuento,
	Impuesto,
	Descuento_Financiero,
	Acum_Monedero,
	Descuento_Monedero,
	Descuento_Cupon,
	Descuento_TerceraEdad,
	Descuento_Laboratorio,
	ISNULL(Anio_Id * 100 + Mes_Id,0) AS AnioMes_Id,
	{% if is_incremental() %}
		GETDATE() AS Fecha_Carga,
		GETDATE() AS Fecha_Actualizado
	{% else %}
		cast(Fecha_Id as DATETIME) AS Fecha_Carga,
		cast(Fecha_Id as DATETIME) AS Fecha_Actualizado
	{% endif %}
FROM BI_FARINTER.dbo.BI_Hecho_VentasHist_Kielsa -- {{source('BI_FARINTER','BI_Hecho_VentasHist_Kielsa')}}
WHERE Anio_Id = {{ v_anio_inicio }}
	AND Mes_Id = {{ v_mes_inicio }}
	AND Fecha_Id >= '{{ v_fecha_inicio }}'
	AND Fecha_Id < CAST(GETDATE() AS DATE)
	AND Pais_Id = 1
	AND AN_FARINTER.[dbo].[AN_fnc_Verificacion Id](Pais_Id, Monedero_Id) = 1