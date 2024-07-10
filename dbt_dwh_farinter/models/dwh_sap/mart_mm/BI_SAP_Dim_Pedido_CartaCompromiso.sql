{% set unique_key_list = ["Sociedad_Id","Articulo_Id","Lote_Id","Pedido_Id","CartaCompromiso_Id"] %}
{{ 
    config(
		as_columnstore=False,
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
    pre_hook=[
      "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            ],
		post_hook=[
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['CartaCompromiso_Id']) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}
/*
    full_refresh= true,
*/
{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,max(Fecha_Actualizado), 120), '00000000')  from  """ ~ this, relation_not_found_value='1900-01-01'|string)|string %}
{% else %}
	{% set last_date = '1900-01-01'|string %}
{% endif %}
SELECT
    [BUKRS] AS Sociedad_Id  
    , [MATNR] AS [Articulo_Id]
    , [LICHA] AS [Lote_Id]
    , [EBELN] AS [Pedido_Id]
    , [ID] AS [CartaCompromiso_Id]
    , [ZCARTA] AS [Indicador_CartaCompromiso]
    , [ZAPRO_INT] AS [Indicador_Aprobacion_Interna]
    , [USNAM] AS [Usuario_Id]
    , [ERDAT] AS [Fecha_Creacion]
    , [ERZET] AS [Hora_Creacion]
    , [AENAM] AS [Usuario_Modificacion]
    , [AEDAT] AS [Fecha_Modificacion]
    , [UTIME] AS [Hora_Modificacion]   
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Actualizado]
FROM {{ ref('DL_SAP_ZMM_CARTA_COMPRO') }} A
WHERE 1=1
{% if is_incremental() %}
  and (A.Fecha_Actualizado >= '{{last_date}}')
{% else %}
  --and S.LAEDA >= '00000000'
{% endif %}

