
{%- set nombre_esquema_particion = "ps_" + this.identifier + "_fecha" -%}
{%- if is_incremental() -%}
	{% set sql_inicializar_particion= '' %}
{%- else -%}
	{%- set sql_inicializar_particion %}
		EXEC ADM_FARINTER.dbo.pa_inicializar_particiones
			@p_base_datos = '{{this.database}}',
			@p_nombre_esquema_particion = '{{nombre_esquema_particion}}',
			@p_nombre_funcion_particion = '{{"pf_" + this.identifier + "_fecha"}}',
			@p_periodo_tipo = 'Anual', --'Anual' o 'Mensual'
			@p_tipo_datos = 'Fecha', --'Fecha' o 'AnioMes'
			@p_fecha_base = '2024-01-01';
	{% endset -%}
{%- endif -%}
{%- set on_clause = nombre_esquema_particion ~ "([Fecha_Contable])" -%}
{% set unique_key_list = ['Id1','Id2', 'Fecha_Contable'] %}
{{ 
    config(
		as_columnstore=false,
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="ignore",
		on_clause_filegroup = on_clause,
		pre_hook=[sql_inicializar_particion],
		post_hook=[
			"{{ dwh_farinter_remove_incremental_temp_table() }}",
      		"{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
			"EXEC ADM_FARINTER.dbo.pa_comprimir_indices_particiones_anteriores 
				@p_base_datos = '{{this.database}}',
				@p_esquema_tabla = '{{this.schema}}', 
				@p_nombre_tabla = '{{this.identifier}}', 
				@p_tipo_datos = 'Fecha';"		
		]
		
) }}

WITH
staging as
(
SELECT 1 as [Id1]
	,2 as [Id2]
	, ISNULL(CAST(GETDATE() AS DATE),'19000101') AS [Fecha_Contable]
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Carga]
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
	
)
select * from staging

