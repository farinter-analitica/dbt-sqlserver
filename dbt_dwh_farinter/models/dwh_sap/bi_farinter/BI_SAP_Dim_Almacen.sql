{% set unique_key_list = ["Almacen_Id","Centro_Id"] %}
{{ 
    config(
    tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}
/*
    full_refresh= true,
*/

SELECT 
  ISNULL(A.LGORT,'X') AS Almacen_Id,
  ISNULL(A.WERKS + '-' + A.LGORT,'X') AS CenAlm_Id,
  A.LGOBE AS Almacen_Nombre,
  isnull(B.WERKS, 'X') AS Centro_Id,
  isnull(B.NAME1, 'N/D') AS Centro_Nombre,
  isnull(C.BUKRS, 'X') AS Sociedad_Id,
  isnull(C.BUTXT, 'N/D') AS Sociedad_Nombre,
  ISNULL(CAST(GETDATE() AS DATETIME), '19000101') AS Fecha_Carga,
  ISNULL(CAST(GETDATE() AS DATETIME), '19000101') AS Fecha_Actualizado
FROM DL_FARINTER.dbo.DL_SAP_T001L A -- {{ ref('DL_SAP_T001L') }}
  LEFT JOIN DL_FARINTER.dbo.DL_SAP_T001W B  -- {{ ref('DL_SAP_T001W') }}
  ON A.WERKS = B.WERKS
  LEFT JOIN DL_FARINTER.dbo.DL_SAP_T001 C  -- {{ source('DL_FARINTER', 'DL_SAP_T001') }}
  ON B.BWKEY = C.BUKRS


