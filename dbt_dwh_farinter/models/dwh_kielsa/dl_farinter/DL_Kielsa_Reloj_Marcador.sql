{% set unique_key_list = ["Ciclo"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "periodo_unico/si"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
    on_schema_change="append_new_columns",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
		  "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        ]
	) 
}}

{% if is_incremental() %}
  {% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -15, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{% endif %}

SELECT --TOP (1000) 
    ISNULL(RM.[Ciclo],0) AS [Ciclo]
      ,RM.[Id_Empleado] COLLATE DATABASE_DEFAULT AS [Id_Empleado]
      ,RM.[HEntrada]
      ,RM.[HSAlmuerzo]
      ,RM.[HEAlmuerzo]
      ,RM.[HSPermiso1]
      ,RM.[HEPermiso1]
      ,RM.[HSPermiso2]
      ,RM.[HEPermiso2]
      ,RM.[HSPermiso3]
      ,RM.[HEPermiso3]
      ,RM.[HSalida]
      ,RM.[Computadora] COLLATE DATABASE_DEFAULT AS [Computadora]
      ,RM.[Pais] COLLATE DATABASE_DEFAULT as [Pais]
      ,RM.[HSReceso]
      ,RM.[HEReceso]
      ,RM.[HSReceso2]
      ,RM.[HEReceso2]
      , GETDATE() AS Fecha_Actualizado
  FROM {{ var('P_SQLLDSUBS_LS') }}.{{ source('SITEPLUS', 'Kielsa_Reloj_Marcador') }} RM
{% if is_incremental() %}
  WHERE RM.HEntrada > '{{ last_date }}'
  OR RM.HSalida > '{{ last_date }}'
{% endif %}