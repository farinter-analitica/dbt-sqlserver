{% set unique_key_list = ["Receta_Id"] -%}

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
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        ]
	) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}
with
Consulta_Rec as (
    select
        VC.[Consulta_Id],
        VC.[Video_Id],
        VC.[Doctor_Usuario_Id],
        VC.[Monedero_Id],
        DI.Fecha_Receta,
        -- Receta / Indicaciones
        DI.Articulo_Id,
        DI.Articulo_Nombre,
        DI.Dosis,
        DI.Frecuencia,
        DI.Numero_Indicacion,
	{# -- Suscripción
	, SU.FRegistro AS Fecha_Suscripcion #}
        [Fecha_Actualizado]
    from [BI_FARINTER].[dbo].[BI_ClinicaLab_Hecho_Consulta] VC --{{ ref('BI_ClinicaLab_Hecho_Consulta') }}
    -- Indicaciones (LEFT JOIN para conservar registros principales aunque no haya receta)
    inner join
        (
            select
                VI._id_oid as Video_Id,
                AR.Articulo_Id,
                VI.medicine as Articulo_Nombre,
                cast(VI.created_at_date at time zone 'Central America Standard Time' as datetime) as Fecha_Receta,
                VI.dose as Dosis,
                VI.frequency as Frecuencia,
                VI.indication_number as Numero_Indicacion
            from DL_FARINTER.dbo.DL_MDBKTMPRO_Clinicas_Videoconf_Indicaciones as VI --{{ source('DL_FARINTER', 'DL_MDBKTMPRO_Clinicas_Videoconf_Indicaciones') }}
            left join (
                select
                    Emp_Id,
                    Articulo_Nombre,
                    MAX(Articulo_Id) Articulo_Id
                from DL_FARINTER.dbo.DL_Kielsa_Articulo AR --{{ source('DL_FARINTER', 'DL_Kielsa_Articulo') }}
                where AR.Emp_Id = 1 and AR.Indicador_PadreHijo = 'P'
                group by Emp_Id, Articulo_Nombre
            ) as AR
                on VI.medicine = AR.Articulo_Nombre
        ) as DI
        on VC.Video_Id = DI.Video_Id
    {% if is_incremental() %}
        where VC.Fecha_Actualizado > '{{ last_date }}'
    {% endif %}
)

select
    ISNULL({{ dwh_farinter_hash_column( columns = ["Consulta_Id", "Numero_Indicacion"], table_alias="A") }}, '') as Receta_Id,
    *
from Consulta_Rec A
--where ISNULL({{ dwh_farinter_hash_column( columns = ["Consulta_Id", "Numero_Indicacion"], table_alias="A") }},'')='8030648FECDF225F4B05637FBF948110'
