{% set unique_key_list = ["Regalia_Id",'Suc_Id','Caja_Id','Emp_Id'] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "automation/periodo_por_hora"],
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
    {%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}

with
staging as (
    select
    -- Primary Keys and Foreign Keys
        Regalia_Id,
        Emp_Id,
        Suc_Id,
        Bodega_Id,
        Caja_Id,
        Consecutivo,
        Cliente_Id,
        Vendedor_Id,
        Usuario_Id,
        Cierre_Id,

        -- Transaction Identifiers
        Mov_Id,
        Operacion_Id,
        Preventa_Id,

        -- Customer Information
        Identificacion_Id as Identidad_Original,
        REPLACE(REPLACE(Identificacion_Id, ' ', ''), '-', '') as Identidad_Limpia,

        -- Date and Time
        Regalia_Fecha as Regalia_Momento,
        cast(Regalia_Fecha as DATE) as Regalia_Fecha,
        DATEPART(hour, Regalia_Fecha) as Regalia_Hora,

        -- Transaction Metrics
        --Regalia_Articulos as Cantidad_Articulos, --Esto suma cantidad en hijos y padres y no sirve
        Regalia_Costo as Valor_Costo,
        Regalia_Total as Valor_Total,

        -- Additional Information
        Regalia_Origen as Tipo_Origen,
        --    Regalia_Nota_Encabezado,
        --    Regalia_Nota_Pie
        Fecha_Actualizado

    from [DL_FARINTER].[dbo].[DL_Kielsa_Regalia_Encabezado] RE --{{ source('DL_FARINTER', 'DL_Kielsa_Regalia_Encabezado') }}
    {% if is_incremental() %}
        where RE.Fecha_Actualizado >= '{{ last_date }}'
    {% endif %}
)

select
    *,
    -- Concatenados
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id', 'Caja_Id', 'Regalia_Id'], input_length=49, table_alias='RE') }} as EmpSucCajReg_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Suc_Id'], input_length=49, table_alias='RE') }} as EmpSuc_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Cliente_Id'], input_length=49, table_alias='RE') }} as EmpCli_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Vendedor_Id'], input_length=49, table_alias='RE') }} as EmpVen_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Identidad_Limpia'], input_length=49, table_alias='RE') }} as EmpMon_Id
from staging RE
