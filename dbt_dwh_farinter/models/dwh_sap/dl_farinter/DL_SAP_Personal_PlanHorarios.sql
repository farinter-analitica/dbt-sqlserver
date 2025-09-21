
{%- set unique_key_list = ["Personal_Id", "Subtipo_Id", "Objeto_Id", "Indicador_Bloqueo","Fecha_Desde", "Fecha_Hasta", "Numero_Clave" ] -%}
{{ 
    config(
		tags=["automation/periodo_mensual_inicio"],
		materialized="view",
		post_hook=[
        	"{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
) }}

{# {% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -30, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %} #}
--Solo editable en Dagster-dbt
--TODO: Convertir a tabla incremental

SELECT --TOP 10000
    --A.MANDT -- X-Mandante-Check:T000-Datatype:CLNT-Len:(3,0)
    A.PERNR Personal_Id,-- X-N?mero de personal-Check:PA0003-Datatype:NUMC-Len:(8,0)
    A.SUBTY Subtipo_Id,-- X-Subtipo-Check: -Datatype:CHAR-Len:(4,0)
    A.OBJPS Objeto_Id,-- X-Identificaci?n de objeto-Check: -Datatype:CHAR-Len:(2,0)
    A.SPRPS Indicador_Bloqueo,-- X-Indicador de bloqueo para registro de maestro de personal-Check: -Datatype:CHAR-Len:(1,0)
    CAST(A.ENDDA AS DATE) Fecha_Hasta,-- X-Fin de la validez-Check: -Datatype:DATS-Len:(8,0)
    CAST(A.BEGDA AS DATE) Fecha_Desde,-- X-Inicio de la validez-Check: -Datatype:DATS-Len:(8,0)
    A.SEQNR Numero_Clave,-- X-N?mero de un registro de infotipo para misma clave-Check: -Datatype:NUMC-Len:(3,0)
    A.AEDTM Fecha_Modificacion,--  -Fecha ?ltima modificaci?n-Check: -Datatype:DATS-Len:(8,0)
    A.UNAME Usuario_Modificacion,--  -Nombre del responsable que ha modificado el objeto-Check: -Datatype:CHAR-Len:(12,0)
    A.SCHKZ Plan_Regla_Id,--  -Regla para plan horario trabajo-Check:*-Datatype:CHAR-Len:(8,0)
    A.ZTERF Estado_GestionTiempos_Id,--  -Status empleado para Gesti?n tiempos personal-Check:T555U-Datatype:NUMC-Len:(1,0)
    COALESCE(T.ZTEXT, '') AS Estado_GestionTiempos_Nombre,--  -Status empleado para Gesti?n tiempos personal-Check:T555U-Datatype:NUMC-Len:(1,0)
    CAST(A.EMPCT AS DECIMAL(5, 2)) AS Porcentaje_HorarioTrabajo,--  -Porcentaje del horario de trabajo-Check: -Datatype:DEC-Len:(5,2)
    A.MOSTD AS Horas_Mensuales,--  -Horas mensuales-Check: -Datatype:DEC-Len:(5,2)
    A.WOSTD AS Horas_Semanales,--  -Horas semanales-Check: -Datatype:DEC-Len:(5,2)
    A.ARBST AS Horas_Diarias,--  -Horas de trabajo diarias-Check: -Datatype:DEC-Len:(5,2)
    A.WKWDY AS Dias_Laborales_Semanales,--  -D?as laborables semanales-Check: -Datatype:DEC-Len:(4,2)
    A.JRSTD AS HorasTrabajo_Anuales,--  -Horas de trabajo anuales-Check: -Datatype:DEC-Len:(7,2)
    A.TEILK AS Indicador_TiempoParcial,--  -Indicador empleado a tiempo parcial-Check: -Datatype:CHAR-Len:(1,0)
    A.MINTA AS HorasMinimas_Diarias,--  -Horas de trabajo m?nimas por d?a-Check: -Datatype:DEC-Len:(5,2)
    A.MAXTA AS HorasMaximas_Diarias,--  -Horas de trabajo m?ximas por d?a-Check: -Datatype:DEC-Len:(5,2)
    A.MINWO AS HorasMinimas_Semanales,----  -Horas de trabajo m?nimas por semana-Check: -Datatype:DEC-Len:(5,2)
    A.MAXWO AS HorasMaximas_Semanales,--  -Horas de trabajo m?ximas por semana-Check: -Datatype:DEC-Len:(5,2)
    A.MINMO AS HorasMinimas_Mensuales,--  -Horas de trabajo m?nimas por mes-Check: -Datatype:DEC-Len:(5,2)
    A.MAXMO AS HorasMaximas_Mensuales,--  -Horas de trabajo m?ximas por mes-Check: -Datatype:DEC-Len:(5,2)
    A.MINJA AS HorasMinimas_Anuales,--  -Horas de trabajo m?nimas por a?o-Check: -Datatype:DEC-Len:(7,2)
    A.MAXJA AS HorasMaximas_Anuales,--  -Horas de trabajo m?ximas por a?o-Check: -Datatype:DEC-Len:(7,2)  
    CAST(
        hashbytes('MD5', CONVERT([VARCHAR](16), A.ENDDA + A.BEGDA))
        AS BIGINT
    ) AS [Periodo_Hashkey],
    CAST(
        hashbytes('MD5', CONVERT([VARCHAR](24), A.PERNR + A.ENDDA + A.BEGDA))
        AS BIGINT
    ) AS [PlanHorarios_Hashkey]
FROM SAPPRD.PRD.prd.PA0007 A
INNER JOIN SAPPRD.PRD.prd.T555V T
    ON A.MANDT = T.MANDT AND A.ZTERF = T.ZTERF AND T.SPRSL = 'S'
WHERE
    A.MANDT = '300'
    AND (
        A.BEGDA >= CAST(year(dateadd(YEAR, -2, getdate())) * 10000 + 01 * 100 + 01 AS NVARCHAR(8))
        OR A.ENDDA >= CAST(year(dateadd(YEAR, -2, getdate())) * 10000 + 01 * 100 + 01 AS NVARCHAR(8))
    )
    AND A.BEGDA <= CAST(year(dateadd(YEAR, 2, getdate())) * 10000 + 12 * 100 + 31 AS NVARCHAR(8))
