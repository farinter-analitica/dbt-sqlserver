
{# Add dwh_farinter_remove_incremental_temp_table to all incremental models #}
{# unique_key is accessible with config.get('unique_key') but it returns a string #}
{% set unique_key_list = ['Centro_Costo_Id','Sociedad_CO'] %}
{{ 
    config(
		as_columnstore=false,
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="sync_all_columns",
		pre_hook=[],
		post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
			"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
		]
		
) }}

with
staging as
(
SELECT --TOP 10 
ISNULL(CAST(A.[KOSTL] AS VARCHAR(10)),'X') COLLATE DATABASE_DEFAULT AS [Centro_Costo_Id] -- -- X-Centro de coste-Check:CSKS-Datatype:CHAR-Len:(10,0)
	, ISNULL(CAST(A.[KOKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [Sociedad_CO] --   X-Sociedad CO-Check:TKA01-Datatype:CHAR-Len:(4,0)
    , ISNULL(TRY_CAST(A.[DATBI] AS DATE),'99991231') AS [Fecha_Fin_Validez] --X-Fecha fin validez-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(A.[KTEXT] COLLATE DATABASE_DEFAULT AS VARCHAR(20)),'')  AS [Nombre_Corto] --  -Denominación general-Check: -Datatype:CHAR-Len:(20,0)
	, ISNULL(CAST(A.[LTEXT] COLLATE DATABASE_DEFAULT AS VARCHAR(40)),'')  AS [Nombre_Largo] --  -Descripción-Check: -Datatype:CHAR-Len:(40,0)
    , ISNULL(TRY_CAST(B.[DATAB] AS DATE),'19000101') AS [Fecha_Inicio_Validez] --Fecha inicio validez-Check: -Datatype:DATS-Len:(8,0)
	, ISNULL(CAST(B.[BUKRS] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [Sociedad_Id] --   -Sociedad-Check:T001-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(B.[GSBER] COLLATE DATABASE_DEFAULT AS VARCHAR(4)),'')  AS [Division_Id] --   -División-Check:TGSB-Datatype:CHAR-Len:(4,0)
	, ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS [Fecha_Actualizado]
	, ISNULL({{ dwh_farinter_concat_key_columns(columns=['KOKRS','KOSTL'], input_length=24, table_alias= 'A')}}, '') AS [CentroCosto_SocCo_Id]
	
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY KOKRS, KOSTL ORDER BY DATBI DESC) AS Fila 
	FROM {{ ref('DL_SAP_CSKT') }} WHERE SPRAS = 'S') A
INNER JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY KOKRS, KOSTL ORDER BY DATBI DESC) AS Fila 
	FROM {{ source ('DL_FARINTER', 'DL_SAP_CSKS') }}) B
ON A.KOKRS = B.KOKRS
AND A.KOSTL = B.KOSTL
--AND A.DATBI = B.DATBI
WHERE A.Fila = 1 AND B.Fila = 1
{% if is_incremental() %}
  --and A.Fecha_Actualizado >= coalesce((select max(Fecha_Actualizado) from {{ this }}), '19000101')
{% endif %}
)
select *
	, ISNULL({{ dwh_farinter_hash_column(unique_key_list) }},'') AS [HashStr_CentroCosto_SocCo] --IdUnicoPlanCuenta, no cambiar orden
from staging
