{% set unique_key_list = ["Suc_Id", "Emp_Id"] %}
{{- 
    config(
        as_columnstore=true,
        tags=["periodo/diario","periodo_unico/si"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
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

-- Tabla de rangos de categorías por empresa (puedes modificar los valores por empresa si es necesario)
-- Valores cerrados a ambos lados
-- Tabla de rangos de categorías por empresa usando la nueva estructura de regla y regla_categoria_sucursal
with CategoriaRangos as (
    select
        r.Emp_Id,
        cs.categoria as [Categoria],
        cs.valor_min as [Valor_Min],
        cs.valor_max as [Valor_Max],
        r.fecha_desde as [Fecha_Validez_Inicio],
        r.fecha_hasta as [Fecha_Validez_Fin],
        cs.regla_id
    from {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla_categoria_sucursal') }} as cs
    inner join {{ source('DL_FARINTER_nocodb_data_gf', 'kielsa_incentivo_regla') }} as r
        on cs.regla_id = r.id
),

ResumenFacturas as (
    select
        Emp_Id,
        Suc_Id,
        sum(Sum_Valor_Neto) as Valor_Neto,
        max(Factura_Fecha) as Fecha_Id
    from {{ ref('BI_Kielsa_Agr_Sucursal_FechaHora') }}
    where
    -- Para los primeros 10 días del mes, tomamos las facturas del mes anterior
        Factura_Fecha < (
            case
                when datepart(day, getdate()) < 10
                    then dateadd(day, 1, eomonth(getdate(), -1))
                else cast(getdate() as date)
            end
        )
        and Factura_Fecha >= (
            case
                when datepart(day, getdate()) < 10
                    then dateadd(day, 1, eomonth(getdate(), -2))
                else dateadd(day, 1, eomonth(getdate(), -1))
            end
        )
    group by Emp_Id, Suc_Id
),

Proyeccion as (
    select
        P.Emp_Id,
        P.Suc_Id,
        sum(P.Valor_Neto) as Valor_Neto
    from {{ ref('BI_Kielsa_Hecho_ProyeccionVenta_SucHora') }} as P
    inner join ResumenFacturas as R
        on
            P.Emp_Id = R.Emp_Id
            and P.Suc_Id = R.Suc_Id
            and P.Fecha_Id >= R.Fecha_Id
    where
    -- Para los primeros 10 días del mes, descartamos las proyecciones del mes actual
        datepart(day, getdate()) >= 10
        and P.Fecha_Id < eomonth(getdate())
    group by P.Emp_Id, P.Suc_Id
),

TotalMes as (
    select
        S.Emp_Id,
        S.Sucursal_Id as Suc_Id,
        R.Fecha_Id,
        isnull(R.Valor_Neto, 0) + isnull(P.Valor_Neto, 0) as Valor_Mes
    from {{ ref('BI_Kielsa_Dim_Sucursal') }} as S
    left join ResumenFacturas as R
        on
            S.Emp_Id = R.Emp_Id
            and S.Sucursal_Id = R.Suc_Id
    left join Proyeccion as P
        on
            S.Emp_Id = P.Emp_Id
            and S.Sucursal_Id = P.Suc_Id
),

ParetoClasificacionBase as (
    select
        T.Emp_Id,
        T.Suc_Id,
        T.Valor_Mes,
        CR.Categoria as Categoria_Personalizada,
        sum(T.Valor_Mes) over (partition by T.Emp_Id order by T.Valor_Mes desc rows between unbounded preceding and current row) as Acumulado,
        sum(T.Valor_Mes) over (partition by T.Emp_Id) as Total,
        -- Clasificación Pareto tradicional (A/B/C/D) por porcentaje acumulado
        cast(sum(T.Valor_Mes) over (partition by T.Emp_Id order by T.Valor_Mes desc rows between unbounded preceding and current row) as float)
        / nullif(sum(T.Valor_Mes) over (partition by T.Emp_Id), 0) as Porc_Acumulado,
        -- Clasificación personalizada por rango de valor de venta
        case
            when
                cast(sum(T.Valor_Mes) over (partition by T.Emp_Id order by T.Valor_Mes desc rows between unbounded preceding and current row) as float)
                / nullif(sum(T.Valor_Mes) over (partition by T.Emp_Id), 0) <= 0.7 then 'A'
            when
                cast(sum(T.Valor_Mes) over (partition by T.Emp_Id order by T.Valor_Mes desc rows between unbounded preceding and current row) as float)
                / nullif(sum(T.Valor_Mes) over (partition by T.Emp_Id), 0) <= 0.85 then 'B'
            when
                cast(sum(T.Valor_Mes) over (partition by T.Emp_Id order by T.Valor_Mes desc rows between unbounded preceding and current row) as float)
                / nullif(sum(T.Valor_Mes) over (partition by T.Emp_Id), 0) <= 0.95 then 'C'
            else 'D'
        end as Pareto_Clasificacion
    from TotalMes as T
    left join CategoriaRangos as CR
        on
            T.Emp_Id = CR.Emp_Id
            and T.Valor_Mes >= CR.Valor_Min
            and T.Valor_Mes <= CR.Valor_Max
            and T.Fecha_Id >= CR.Fecha_Validez_Inicio
            and (CR.Fecha_Validez_Fin is NULL or T.Fecha_Id <= CR.Fecha_Validez_Fin)
),

CategoriaFinal as (
    select
        *,
        coalesce(Categoria_Personalizada, Pareto_Clasificacion) as Categoria_Final
    from ParetoClasificacionBase
),

ClaseLimites as (
    select
        Emp_Id,
        Categoria_Final,
        min(Valor_Mes) as Categoria_Valor_Inicial,
        max(Valor_Mes) as Categoria_Valor_Final
    from CategoriaFinal
    group by Emp_Id, Categoria_Final
)

select --noqa: ST06
    isnull(P.Emp_Id, 0) as Emp_Id,
    isnull(P.Suc_Id, 0) as Suc_Id,
    isnull(P.Valor_Mes, 0.0) as Valor_Mes,
    P.Acumulado,
    P.Total,
    P.Porc_Acumulado,
    P.Pareto_Clasificacion,
    P.Categoria_Personalizada,
    P.Categoria_Final,
    C.Categoria_Valor_Inicial,
    C.Categoria_Valor_Final
from CategoriaFinal as P
left join ClaseLimites as C
    on
        P.Emp_Id = C.Emp_Id
        and P.Categoria_Final = C.Categoria_Final
