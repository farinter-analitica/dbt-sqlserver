
{% set unique_key_list = ["TABNAME","TDNAME", "AnioMes_Id"] %}
{{ 
    config(
		as_columnstore=False,
        tags=["periodo/diario"],
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
      	"{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), create_clustered=false, columns=['AnioMes_Id','TABNAME']) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['clave_0002','clave_0003'], included_columns = ['TDOBJECT','TDNAME','AnioMes_Id']) }}",
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}",
        ]
	) 
}}

{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -3, max(Fecha_Actualizado)), 112), '00000000')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}
{% set last_year = last_date[0:4]%}


WITH
otras_equivalencias AS
	(SELECT
		TABNAME, TDOBJECT
	FROM
		(VALUES ('VBAK', 'VBBK')
				, ('VBAP', 'VBBP')
				, ('VBRK', 'VBBK')
				, ('VBRP', 'VBBP')
				, ('VBKA', 'VBKA')
				, ('LIKP', 'VBBK')
				, ('LIPS', 'VBBP')) AS T (TABNAME, TDOBJECT) 
	),
datos_primarios AS
	(SELECT LP.TABNAME, ISNULL(OE.TDOBJECT, LP.TABNAME) TDOBJECT, LP.FIELDNAME, LP.LENG, LP.[POSITION]
		, SUM(LP.LENG) OVER(PARTITION BY LP.TABNAME ORDER BY LP.[POSITION]) AS ACUM_LENG
	FROM {{ ref('DL_SAP_LLAVE_PRIMARIA') }} LP
	LEFT JOIN otras_equivalencias OE
		ON LP.TABNAME = OE.TABNAME
	WHERE LP.DATATYPE <> 'CLNT'
	),
claves_textos AS
	(SELECT --TOP 100 
		AnioMes_Id,TDOBJECT, TDNAME, MAX(TDLDATE) AS TDLDATE
	FROM {{ source('DL_FARINTER', 'DL_SAP_STXH') }} TXT
	WHERE 
		 AnioMes_Id >= {{ last_year }}
		AND TDLDATE >= '{{ last_date }}'
		--AND TXT.TDOBJECT IN ( 'PLACEHOLDER'
		-- , 'VBBK'
		--, 'EKKO'
		-- , 'VBBP' 
		--)
	GROUP BY AnioMes_Id,TDOBJECT, TDNAME
	),
claves_individuales AS
(
SELECT	TXT.AnioMes_Id
		, TXT.TDOBJECT
		, LP.TABNAME
		, TXT.TDNAME
		, LP.FIELDNAME
		, LP.LENG
        , TXT.TDLDATE
		, valor_clave = SUBSTRING(TXT.TDNAME, LP.ACUM_LENG - LP.LENG + 1, LP.LENG)
		, LP.[POSITION]
		, LP.ACUM_LENG
		, clave_n = 'clave_'+LP.[POSITION]
FROM	claves_textos TXT
LEFT JOIN datos_primarios LP
	ON LP.TDOBJECT = TXT.TDOBJECT
),
--SELECT * FROM claves_individuales
pivot_table AS
(
		SELECT 
		AnioMes_Id,
		TDOBJECT, 
		TABNAME, 
		TDNAME, 
		ISNULL(clave_0001, '300') AS [clave_0001], 
		[clave_0002], 
		[clave_0003], 
		[clave_0004], 
		[clave_0005]
	FROM 
	(
		SELECT 
			AnioMes_Id,
			TDOBJECT, 
			TABNAME, 
			TDNAME, 
			clave_n, 
			valor_clave
		FROM 
			claves_individuales
	) AS src
	PIVOT
	(
		MAX(valor_clave)
		FOR clave_n IN ([clave_0001], [clave_0002], [clave_0003], [clave_0004], [clave_0005])
	) AS pvt
)
--SELECT * FROM pivot_table
, info_claves AS
(
	SELECT
		AnioMes_Id, TDOBJECT, TABNAME, TDNAME, MAX(TDLDATE) AS TDLDATE
		, STRING_AGG(FIELDNAME + '_' + [POSITION], ',')WITHIN GROUP(ORDER BY [POSITION]) AS campos_clave
	FROM	claves_individuales CI
	GROUP BY CI.AnioMes_Id, CI.TDOBJECT, CI.TABNAME, CI.TDNAME
)
SELECT
	ISNULL(IC.AnioMes_Id,1900) AS [AnioMes_Id]
	, ISNULL(IC.TABNAME,'') AS [TABNAME]
	, ISNULL(IC.TDNAME,'') AS [TDNAME]
	, PVT.[clave_0001]
	, PVT.[clave_0002]
	, PVT.[clave_0003]
	, PVT.[clave_0004]
	, PVT.[clave_0005]
	, IC.campos_clave
	, IC.TDOBJECT
    , ISNULL(CAST(GETDATE() AS DATETIME),'1900-01-01') AS [Fecha_Carga]
    , ISNULL(TRY_CONVERT(DATETIME, TDLDATE, 112),'1900-01-01') AS [Fecha_Actualizado]
FROM	info_claves IC
INNER JOIN pivot_table PVT
	ON IC.TDOBJECT = PVT.TDOBJECT AND IC.TABNAME = PVT.TABNAME AND IC.TDNAME = PVT.TDNAME
/*

SELECT TABNAME, FIELDNAME 
	FROM DL_FARINTER.dbo.DL_SAP_LLAVE_PRIMARIA LP

	*/