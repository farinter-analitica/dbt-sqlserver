
{% set unique_key_list = ["SubDocumento_Id","Documento_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario", "automation/periodo_por_hora"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}
WITH Datos_Base AS
(
SELECT
	ISNULL(D.Emp_Id,0) AS [Emp_Id]
	, ISNULL(D.Documento_Id,0) AS [Documento_Id]
	, D.Documento_Nombre
	, ISNULL(D.HashStr_DocEmp,'X') AS [HashStr_DocEmp]
    , D.Hash_DocumentoEmp
	, ISNULL(SD1.SubDocumento_Id,0) SubDocumento_Id
	, ISNULL(SD1.SubDocumento_Nombre,'Otros') SubDocumento_Nombre
	, ISNULL(SD1.Hash_DocumentoSubDocumentoEmp, D.Hash_DocumentoEmp) AS Hash_SubDocEmp
	, ISNULL(SD1.HashStr_SubDDocEmp, D.HashStr_DocEmp) AS [HashStr_SubDDocEmp]
FROM {{ ref('DL_Kielsa_Documento') }}	D
LEFT JOIN {{ ref('DL_Kielsa_SubDocumento') }} SD1
	ON D.Documento_Id = SD1.Documento_Id
    AND D.Emp_Id = SD1.Emp_Id
)
SELECT *
		, {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Documento_Id', 'SubDocumento_Id'], input_length=29, table_alias='')}} [EmpDocSubD_Id]
FROM Datos_Base