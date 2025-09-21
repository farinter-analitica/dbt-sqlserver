
{%- set unique_key_list = ["id"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
      "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['TarjetaKC_Id']) }}",
		]
		
) }}

{% if is_incremental() %}
    {% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}


SELECT --TOP (1000) 
    [id],
    [TarjetaKC_Id],
    [Cliente_Nombre],
    [Usuario_Registro],
    [Sucursal_Registro],
    [TipoPlan],
    [FRegistro],
    [CodPlanKielsaClinica],
    [Estado],
    [ErrorMessage],
    GETDATE() AS Fecha_Actualizado
FROM {{ var('P_SQLLDSUBS_LS') }}.[KPP_DB].[dbo].[LogSuscripcion] -- {{ var('P_SQLLDSUBS_LS') }}.{{ source('KPP_DB', 'LogSuscripcion') }} 
{% if is_incremental() %}
    WHERE [FRegistro] > '{{ last_date }}'
{% else %}

  {% endif %}


