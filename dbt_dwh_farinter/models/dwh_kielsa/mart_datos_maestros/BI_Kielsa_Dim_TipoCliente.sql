
{% set unique_key_list = ["TipoCliente_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario","periodo/por_hora"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}
--dbt dagster 
SELECT --TOP (1000) 
    [Emp_Id],
    [TipoCliente_Id],
    {{ dwh_farinter_concat_key_columns(columns=unique_key_list, input_length=14) }} AS [EmpTipoCli_Id],
    UPPER(TipoCliente_Nombre) AS [TipoCliente_Nombre],
    ISNULL(
        {{ dwh_farinter_hash_column(unique_key_list) }},
        ''
    ) AS [HashStr_TipoCliEmp],
    [Fecha_Carga],
    [Fecha_Actualizado]
FROM [DL_FARINTER].[dbo].[DL_Kielsa_Tipo_Cliente] -- {{ ref( 'DL_Kielsa_Tipo_Cliente') }}