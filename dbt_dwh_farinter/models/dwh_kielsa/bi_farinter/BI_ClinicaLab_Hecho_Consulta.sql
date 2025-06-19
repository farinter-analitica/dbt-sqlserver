{% set unique_key_list = ["Consulta_Id"] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["automation/periodo_diario", "automation_only"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Consulta']) }}",
        ]
	) 
}}

{%- if is_incremental() %}
	{%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -15, max(Fecha_Consulta)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}

WITH 
Consulta AS (
SELECT
	-- IDs
	VC._id_oid AS Video_Id
	, CO._id_oid AS Consultado_Id
	, CO.postvideoconference_oid AS PostVideo_Id
	-- COALESCE en orden de prioridad (VC sobre CO)
	, COALESCE(VC.doctor_oid, CO.doctor_oid, PV.[doctor_oid]) AS Doctor_Usuario_Id
	, COALESCE(VC.patient_oid, CO.patient_oid, PV.[patient_oid]) AS Paciente_Usuario_Id
	, CAST(COALESCE(VC.created_at_date, CO.created_at_date, PV.[created_at_date]) AT TIME ZONE 'Central America Standard Time' AS datetime)  AS Fecha_Consulta
	, CAST(PV.created_at_date AT TIME ZONE 'Central America Standard Time' AS datetime)  AS Fecha_PostVideo
	, CAST(VC.created_at_date AT TIME ZONE 'Central America Standard Time' AS datetime)  AS Inicio_Video
	, CAST(VC.endcalldoctor_date AT TIME ZONE 'Central America Standard Time' AS datetime)  AS Fin_Video
	, VC.[type] AS Tipo_Video
	, COALESCE(VC.[type], CO.consultationtype, CASE WHEN PV._id_oid IS NOT NULL THEN 'follow-up-call' ELSE NULL END) AS Tipo_Consulta
	, CO.[status] AS Consulta_Estado
	, COALESCE(VT.[reasoncancel], CO.[reasoncancel]) AS Razon_Cancelacion
	, COALESCE(VT.[nursingnotes], PV.[nurse_note]) AS Notas_Enfermeria
	, CO.doctoroffice_oid AS Consultorio_Id
	, VC.followref_oid AS Video_Id_Referencia_Anterior
	-- Receta / Indicaciones
	{# , DI.Articulo_Id
	, DI.Dosis
	, DI.Frecuencia
	, DI.Indicaciones
	, DI.Fecha AS Fecha_Receta #}
	{# -- Suscripción
	, SU.FRegistro AS Fecha_Suscripcion #}
	, US.Identidad_Limpia AS Monedero_Id
	, CASE
		WHEN DI.Video_Id IS NULL
			THEN 'Sin receta'
		ELSE 'Con receta'
	END AS Receta
FROM	DL_FARINTER.dbo.DL_MDBKTMPRO_Clinicas_Videoconf_Cabecera AS VC --{{ source('DL_FARINTER', 'DL_MDBKTMPRO_Clinicas_Videoconf_Cabecera') }}
-- Conexión a Consultas (FULL JOIN a través de la tabla puente)
FULL JOIN DL_FARINTER.dbo.DL_MDBKTMPRO_Clinicas_Consultas AS CO --{{ source('DL_FARINTER', 'DL_MDBKTMPRO_Clinicas_Consultas') }}
	ON VC.consutdoctoroffice_oid = CO._id_oid
 -- Conexión a Postvideoconf (FULL JOIN para traer todo de VC y PV)
LEFT JOIN DL_FARINTER.dbo.DL_MDBKTMPRO_Clinicas_Postvideoconf AS PV --{{ source('DL_FARINTER', 'DL_MDBKTMPRO_Clinicas_Postvideoconf') }}
	ON CO.postvideoconference_oid = PV._id_oid 

-- Indicaciones (LEFT JOIN para conservar registros principales aunque no haya receta)
LEFT JOIN
	(SELECT
		VI._id_oid AS Video_Id
		{# , AR.Articulo_Id
		, VI.created_at_date AS Fecha
		, VI.dose AS Dosis
		, VI.frequency AS Frecuencia
		, VI.indication_number AS Indicaciones #}
	FROM	DL_FARINTER.dbo.DL_MDBKTMPRO_Clinicas_Videoconf_Indicaciones AS VI --{{ source('DL_FARINTER', 'DL_MDBKTMPRO_Clinicas_Videoconf_Indicaciones') }}
	LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_Articulo AS AR --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo') }}
		ON VI.medicine = AR.Articulo_Nombre
	WHERE AR.Emp_Id = 1 AND AR.Indicador_PadreHijo = 'P'
    GROUP BY VI._id_oid
    ) AS DI
	ON VC._id_oid = DI.Video_Id
LEFT JOIN DL_FARINTER.dbo.DL_MDBKTMPRO_Clinicas_Videoconf_Textos VT --{{ source('DL_FARINTER', 'DL_MDBKTMPRO_Clinicas_Videoconf_Textos') }}
	ON VT._id_oid = VC._id_oid

INNER JOIN BI_FARINTER.dbo.BI_ClinicaLab_Dim_Usuario AS US --{{ ref('BI_ClinicaLab_Dim_Usuario') }}
	ON COALESCE(VC.patient_oid, CO.patient_oid) = US.Id_Usuario

{# LEFT JOIN DL_FARINTER.dbo.DL_Kielsa_KPP_Suscripcion AS SU --{{ source('DL_FARINTER', 'DL_Kielsa_KPP_Suscripcion') }}
	ON US.Identidad_Limpia = SU.Identidad_Limpia; #}
)
SELECT 	ISNULL({{ dwh_farinter_hash_column( columns = ["Video_Id", "Consultado_Id"], table_alias="A") }},'') AS Consulta_Id --ISNULL(COALESCE(VC._id_oid, CO._id_oid, PV._id_oid),'') AS _Id
	, *
    , GETDATE() AS Fecha_Actualizado
FROM Consulta A
{% if is_incremental() %}
WHERE A.Fecha_Consulta > '{{ last_date }}'
{% endif %}