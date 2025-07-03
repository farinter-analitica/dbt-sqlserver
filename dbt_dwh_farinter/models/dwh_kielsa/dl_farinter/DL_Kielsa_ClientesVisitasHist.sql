{%- set unique_key_list = ["Fecha_Id", "Cliente_Id", "Pais_Id", "AnioMes_Id"] -%}

{{
    config(
        as_columnstore=true,
        tags=["automation_only", "automation/periodo_diario", "periodo_unico/si"],
        materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="fail",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", 
            create_clustered=false, 
            is_incremental=is_incremental(), 
            if_another_exists_drop_it=true) }}",
        ]    
    )
}}

{%- if is_incremental() %}
	{%- set v_last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Id)), 112), '19000101') from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set v_last_date = '19000101' %}
{%- endif %}
{%- set v_anio_mes = v_last_date[:6] %}

{%- set v_today = modules.datetime.date.today() -%}
{%- set v_window_start = v_last_date -%}
{%- set v_window_end = (v_today - modules.datetime.timedelta(days=1)).strftime('%Y%m%d') -%}

{% if is_incremental() %}
with
-- 1. Clientes con actividad en la ventana de 7 días
ClientesVentana as (
    select distinct
        V.Pais_Id,
        V.Monedero_Id as Cliente_Id,
        V.Fecha_Id
    from {{ ref('DL_Acum_VentasHist_Kielsa') }} as V
    where V.Fecha_Id between cast('{{ v_window_start }}' as date) and cast('{{ v_window_end }}' as date)
    and V.AnioMes_Id >= {{ v_anio_mes }}
),

-- 2. Última visita anterior a la ventana para cada cliente
UltimaVisitaAnterior as (
    select
        V.Pais_Id,
        V.Monedero_Id as Cliente_Id,
        max(cast(V.Fecha_Id as date)) as Fecha_Id
    from {{ ref('DL_Acum_VentasHist_Kielsa') }} as V
    inner join ClientesVentana C
        on V.Pais_Id = C.Pais_Id and V.Monedero_Id = C.Cliente_Id
    where V.Fecha_Id < cast('{{ v_window_start }}' as date)
    and V.AnioMes_Id <= {{ v_anio_mes }}
    group by V.Pais_Id, V.Monedero_Id
),

{% else %}
with
{% endif %}
-- 3. Datos de la ventana y el último registro anterior (para contexto)
Base as (
    select
        isnull(V.Pais_Id, 0) as Pais_Id,
        isnull(V.Monedero_Id, '') as Cliente_Id,
        isnull(cast(V.Fecha_Id as date), '19000101') as Fecha_Id,
        max(V.Factura_Id) as Factura_Id,
        max(V.Sucursal_Id) as Sucursal_Id,
        cast(max(V.Empleado_Id) as nvarchar(50)) as Empleado_Id,
        row_number() over (
            partition by V.Pais_Id, V.Monedero_Id order by cast(V.Fecha_Id as date)
        ) as Transac_No,
        count(distinct V.Factura_Id) as Facturas,
        cast(avg(V.Venta_Bruta * 1.0 / nullif(V.Cantidad,0)) as money) as Valor_Precio_Promedio,
        avg(V.Acum_Monedero) as Valor_AcumulaMonedero,
        sum(V.Descuento) as Valor_Descuento,
        avg(V.Descuento_Monedero) as Valor_DescuentoMonedero,
        sum(V.Descuento_Cupon + V.Descuento_Financiero + V.Descuento_Monedero) as Valor_OtrosDescuentos,
        sum(V.Costo) as Valor_Costo,
        sum(V.Venta_Bruta) as Valor_Venta,
        sum(case when V.DeptoArt_Id in ('1-4', '1-5') then V.Venta_Neta else 0 end) as Valor_VentaNeta_MP,
        sum(case when V.SubCategoria1Art_Id = '1-1-1' then V.Venta_Neta else 0 end) as Valor_VentaNeta_Eticos,
        sum(case when V.SubCategoria1Art_Id = '1-1-2' then V.Venta_Neta else 0 end) as Valor_VentaNeta_OTC,
        sum(V.Venta_Neta - V.Descuento - V.Costo) as Valor_Utilidad
    from {{ ref('DL_Acum_VentasHist_Kielsa') }} as V
    {% if is_incremental() %}
    inner join (
        -- Todos los clientes con actividad en la ventana
        select Pais_Id, Cliente_Id, Fecha_Id from ClientesVentana
        union
        -- Y su última visita anterior (si existe)
        select Pais_Id, Cliente_Id, Fecha_Id from UltimaVisitaAnterior
    ) C
        on V.Pais_Id = C.Pais_Id and V.Monedero_Id = C.Cliente_Id and cast(V.Fecha_Id as date) = C.Fecha_Id
    {% else %}
    where V.Fecha_Id between cast('{{ v_window_start }}' as date) and cast('{{ v_window_end }}' as date)
    {% endif %}
    group by
        V.Pais_Id,
        V.Monedero_Id,
        cast(V.Fecha_Id as date)
),

-- 4. Calcular lag/lead sobre la ventana + contexto
ClientesVisitas as (
    select
        CF.*,
        year(CF.Fecha_Id)*100 + month(CF.Fecha_Id) as AnioMes_Id,
        datepart(day, CF.Fecha_Id) as Dia_Id,
        lag(CF.Fecha_Id) over (partition by CF.Pais_Id, CF.Cliente_Id order by CF.Fecha_Id) as Fecha_Id_Anterior,
        datediff(day, lag(CF.Fecha_Id) over (partition by CF.Pais_Id, CF.Cliente_Id order by CF.Fecha_Id), CF.Fecha_Id) as Dias_Desde_Anterior,
        lead(CF.Fecha_Id) over (partition by CF.Pais_Id, CF.Cliente_Id order by CF.Fecha_Id) as Fecha_Id_Siguiente,
        datediff(day, CF.Fecha_Id, lead(CF.Fecha_Id) over (partition by CF.Pais_Id, CF.Cliente_Id order by CF.Fecha_Id)) as Dias_Hasta_Siguiente,
        datepart(iso_week, CF.Fecha_Id) as Semana_ISO
    from Base CF
)

-- 5. Solo los registros de la ventana de 7 días (no el contexto anterior)
select
    [Pais_Id],
    [Cliente_Id],
    [Fecha_Id],
    [Transac_No],
    [Factura_Id],
    [Sucursal_Id],
    [Empleado_Id],
    [Facturas],
    [Valor_Precio_Promedio],
    [Valor_AcumulaMonedero],
    [Valor_Descuento],
    [Valor_DescuentoMonedero],
    [Valor_OtrosDescuentos],
    [Valor_Costo],
    [Valor_Venta],
    [Valor_VentaNeta_MP],
    [Valor_VentaNeta_Eticos],
    [Valor_VentaNeta_OTC],
    [Valor_Utilidad],
    [AnioMes_Id],
    [Dia_Id],
    [Semana_ISO],
    [Fecha_Id_Anterior],
    [Dias_Desde_Anterior],
    [Fecha_Id_Siguiente],
    [Dias_Hasta_Siguiente]
from ClientesVisitas
where Fecha_Id between cast('{{ v_window_start }}' as date) and cast('{{ v_window_end }}' as date)
