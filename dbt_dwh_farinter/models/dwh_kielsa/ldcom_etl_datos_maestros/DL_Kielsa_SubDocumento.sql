
{% set unique_key_list = ["SubDocumento_Id","Documento_Id","Emp_Id"] %}
{{ 
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}


WITH DatosBase
AS
(
	--LDCOMHN.LDCOM_KIELSA
	SELECT ISNULL(Emp_Id,0) AS [Emp_Id]
		, ISNULL(Doc_Id,0) AS [Documento_Id]
		, ISNULL(SubDoc_Id,0) AS [SubDocumento_Id]
		, SubDoc_Nombre COLLATE DATABASE_DEFAULT AS [SubDocumento_Nombre]
		, SubDoc_Tipo_Consecutivo AS [SubDocumento_Tipo_Consecutivo]
		, ABS(CAST(HASHBYTES('SHA1', CONCAT(Doc_Id, 0, SubDoc_Id, 0, Emp_Id)) AS bigint)) AS Hash_DocumentoSubDocumentoEmp 
	FROM [LDCOMHN].LDCOM_KIELSA.dbo.SubDocumento WHERE Emp_Id = 1 
	UNION ALL 
	--[LDCOMGT].LDCOM_KIELSA_GT
	SELECT ISNULL(Emp_Id,0) AS [Emp_Id]
		, ISNULL(Doc_Id,0) AS [Documento_Id]
		, ISNULL(SubDoc_Id,0) AS [SubDocumento_Id]
		, SubDoc_Nombre COLLATE DATABASE_DEFAULT AS [SubDocumento_Nombre]
		, SubDoc_Tipo_Consecutivo AS [SubDocumento_Tipo_Consecutivo]
		, ABS(CAST(HASHBYTES('SHA1', CONCAT(Doc_Id, 0, SubDoc_Id, 0, Emp_Id)) AS bigint)) AS Hash_DocumentoSubDocumentoEmp
	FROM [LDCOMGT].LDCOM_KIELSA_GT.dbo.SubDocumento WHERE Emp_Id = 2
	UNION ALL
	--[LDCOMNI].LDCOM_KIELSA_NIC
	SELECT ISNULL(Emp_Id,0) AS [Emp_Id]
		, ISNULL(Doc_Id,0) AS [Documento_Id]
		, ISNULL(SubDoc_Id,0) AS [SubDocumento_Id]
		, SubDoc_Nombre COLLATE DATABASE_DEFAULT AS [SubDocumento_Nombre]
		, SubDoc_Tipo_Consecutivo AS [SubDocumento_Tipo_Consecutivo]
		, ABS(CAST(HASHBYTES('SHA1', CONCAT(Doc_Id, 0, SubDoc_Id, 0, Emp_Id)) AS bigint)) AS Hash_DocumentoSubDocumentoEmp
	FROM [LDCOMNI].LDCOM_KIELSA_NIC.dbo.SubDocumento WHERE Emp_Id = 3
	UNION ALL
	--[LDCOMCR].LDCOM_KIELSA_CR
	SELECT ISNULL(Emp_Id,0) AS [Emp_Id]
		, ISNULL(Doc_Id,0) AS [Documento_Id]
		, ISNULL(SubDoc_Id,0) AS [SubDocumento_Id]
		, SubDoc_Nombre COLLATE DATABASE_DEFAULT AS [SubDocumento_Nombre]
		, SubDoc_Tipo_Consecutivo AS [SubDocumento_Tipo_Consecutivo]
		, ABS(CAST(HASHBYTES('SHA1', CONCAT(Doc_Id, 0, SubDoc_Id, 0, Emp_Id)) AS bigint)) AS Hash_DocumentoSubDocumentoEmp
	FROM [LDCOMCR].LDCOM_KIELSA_CR.dbo.SubDocumento WHERE Emp_Id = 4
	UNION ALL
	--[REPLICASLD].[LDCOMREPSLV]
	SELECT ISNULL(Emp_Id,0) AS [Emp_Id]
		, ISNULL(Doc_Id,0) AS [Documento_Id]
		, ISNULL(SubDoc_Id,0) AS [SubDocumento_Id]
		, SubDoc_Nombre COLLATE DATABASE_DEFAULT AS [SubDocumento_Nombre]
		, SubDoc_Tipo_Consecutivo AS [SubDocumento_Tipo_Consecutivo]
		, ABS(CAST(HASHBYTES('SHA1', CONCAT(Doc_Id, 0, SubDoc_Id, 0, Emp_Id)) AS bigint)) AS Hash_DocumentoSubDocumentoEmp
	FROM [REPLICASLD].[LDCOMREPSLV].dbo.SubDocumento
	WHERE Emp_Id = 5
)
SELECT *
	, ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_SubDDocEmp]
	, GETDATE() AS [Fecha_Carga]
	, GETDATE() AS [Fecha_Actualizado]
FROM datosBase