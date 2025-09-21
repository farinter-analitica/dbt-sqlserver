{% set unique_key_list = ["Id_Usuario"] -%}

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
        "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
	) 
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -30, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}
WITH
Dim_Usuario AS (
    SELECT
        U._id_oid AS [Id_Usuario],
        U.dni AS [Identidad_Original],
        REPLACE(REPLACE(U.dni, '-', ''), ' ', '') AS [Identidad_Limpia],
        U.idtype AS [Tipo_Identidad],
        U.createdby_oid AS [Creado_Por_Id],
        -- Convert datetimeoffset to local datetime:
        CAST(U.created_at_date AT TIME ZONE 'Central America Standard Time' AS datetime) AS [Fecha_Creado],
        U.email AS [Correo_Electronico],
        U.cellphone AS [Telefono_Celular],
        UPPER(REPLACE(U.firstname, '  ', ' ')) AS [Nombre],
        UPPER(REPLACE(U.lastname, '  ', ' ')) AS [Apellido],
        U.gender AS [Genero],
        U.isdependent AS [Es_Dependiente],
        U.isregisterbydomain AS [Es_Registro_Por_Dominio],
        U.locked AS [Es_Bloqueado],
        U.registertype AS [Tipo_Registro],
        U.role AS [Rol],
        U.status AS [Estado]
    FROM [DL_FARINTER].[dbo].[DL_MDBKTMPRO_Clinicas_Usuarios] U --{{ source('DL_FARINTER', 'DL_MDBKTMPRO_Clinicas_Usuarios') }}
    {% if is_incremental() %}
        WHERE U.created_at_date >= CAST('{{ last_date }}' AS datetimeoffset(0)) AT TIME ZONE 'UTC'
    {% endif %}
)

SELECT
    *,
    GETDATE() AS Fecha_Actualizado
FROM Dim_Usuario
;
