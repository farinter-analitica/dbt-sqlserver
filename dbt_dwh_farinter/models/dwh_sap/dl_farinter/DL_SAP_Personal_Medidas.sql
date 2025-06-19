
{%- set unique_key_list = ["Personal_Id", "Subtipo_Id","Objeto_Id", "Indicador_Bloqueo","Fecha_Desde", "Fecha_Hasta", "Numero_Clave" ] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
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

{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -30, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}


SELECT --TOP 10000
	--A.MANDT -- Mandante-Check:T000-Datatype:CLNT-Len:(3,0)
     A.PERNR Personal_Id-- Número de personal-Check:PA0003-Datatype:NUMC-Len:(8,0)
    , A.SUBTY Subtipo_Id-- Subtipo-Check: -Datatype:CHAR-Len:(4,0)
    , A.OBJPS Objeto_Id-- Identificación de objeto-Check: -Datatype:CHAR-Len:(2,0)
    , A.SPRPS Indicador_Bloqueo-- Indicador de bloqueo para registro de maestro de personal-Check: -Datatype:CHAR-Len:(1,0)
    , CAST(A.ENDDA AS DATE) Fecha_Hasta-- Fin de la validez-Check: -Datatype:DATS-Len:(8,0)
    , CAST(A.BEGDA AS DATE) Fecha_Desde-- Inicio de la validez-Check: -Datatype:DATS-Len:(8,0)
    , A.SEQNR Numero_Clave-- N?mero de un registro de infotipo para misma clave-Check: -Datatype:NUMC-Len:(3,0)
    , CAST(A.AEDTM AS DATE) Fecha_Modificacion-- Fecha ?ltima modificaci?n-Check: -Datatype:DATS-Len:(8,0)
    , A.UNAME Usuario_Modificacion-- Nombre del responsable que ha modificado el objeto-Check: -Datatype:CHAR-Len:(12,0)
    , A.MASSN Clase_Medida_Id-- Clase de medida-Check:T529A-Datatype:CHAR-Len:(2,0)
    , T.MNTXT Clase_Medida_Nombre--Medida Nombre
    , A.STAT2 Estado_Ocupacion_Id-- Status ocupaci?n-Check:T529U-Datatype:CHAR-Len:(1,0)
    , coalesce(STATN2.TEXT1,'') Estado_Ocupacion_Nombre--
    , A.STAT3 Estado_Pago_Extraordinario-- Status pagas extraordinarias-Check:T529U-Datatype:CHAR-Len:(1,0)
    , coalesce(STATN3.TEXT1,'') Estado_Pago_Extra_Nombre--
	, CAST(
					hashbytes('MD5',CONVERT([varchar](16), A.ENDDA+A.BEGDA)) 
				AS BIGINT) AS [Periodo_Hashkey]
	, CAST(
					hashbytes('MD5',CONVERT([varchar](24), A.PERNR+A.ENDDA+A.BEGDA)) 
				AS BIGINT) AS [Medidas_Hashkey]
    , GETDATE() as Fecha_Carga
    , GETDATE() AS Fecha_Actualizado
FROM SAPPRD.PRD.prd.PA0000 A
	INNER JOIN SAPPRD.PRD.prd.T529T T
		ON A.MANDT=T.MANDT AND A.MASSN= T.MASSN AND T.SPRSL = 'S'
	LEFT JOIN  SAPPRD.PRD.prd.T529U STATN2
		ON A.MANDT=STATN2.MANDT AND A.STAT2= STATN2.STATV  AND STATN2.SPRSL = 'S' AND STATN2.STATN=2
	LEFT JOIN  SAPPRD.PRD.prd.T529U STATN3
		ON A.MANDT=STATN3.MANDT AND A.STAT2= STATN3.STATV  AND STATN3.SPRSL = 'S' AND STATN3.STATN=3
WHERE A.MANDT = '300' 

{%- if is_incremental() %}
	AND (A.AEDTM >=  '{{last_date}}'
		OR A.ENDDA >=  '{{last_date}}')
{%- endif %}

	