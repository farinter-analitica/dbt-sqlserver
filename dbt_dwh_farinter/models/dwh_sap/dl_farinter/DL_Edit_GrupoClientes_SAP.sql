
{%- set unique_key_list = ["Cliente_Id"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
		]
		
) }}

  SELECT 
        ISNULL(REPLICATE('0',10-LEN(cliente_id)) + cliente_id COLLATE DATABASE_DEFAULT, '0000000000') AS Cliente_Id,
        cliente_id COLLATE DATABASE_DEFAULT AS Cliente_Id_Original,
        ISNULL(gpo_cliente_id,'') AS GrupoClientes_Id,
        ISNULL(gpo_cliente_nombre COLLATE DATABASE_DEFAULT,'') AS GrupoClientes_Nombre,
        ISNULL(gpo_cliente_etiquetas COLLATE DATABASE_DEFAULT,'') AS GrupoClientes_Etiquetas,
        estad_necesidades COLLATE DATABASE_DEFAULT as Estadistica_Necesidades,
        CASE WHEN  ',' + gpo_cliente_etiquetas + ',' LIKE '%,KIELSA,%' THEN 1 ELSE 0 END AS Es_Kielsa,
        CASE WHEN  ',' + gpo_cliente_etiquetas + ',' LIKE '%,Exc_Forecast_Unificado,%' THEN 1 ELSE 0 END AS Excluir_Forecast_Unificado,
       GETDATE() AS Fecha_Actualizado
  FROM {{ var('P_SQLLDSUBS_LS') }}.[PLANNING_DB].[dbo].[GposCliente] -- {{ var('P_SQLLDSUBS_LS') }}.{{ source('PLANNING_DB', 'GposCliente') }} 
  --{% if is_incremental() %}
  --WHERE [Fecha_Creacion] > '{{ last_date }}'
  --{% else %}

  --{% endif %}