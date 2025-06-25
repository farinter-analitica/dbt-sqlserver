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
{%- set categoria_rangos_config = [
    {
        'Emp_Id': 5,
        'Fecha_Validez_Inicio': '20250601' if target.name == 'dev' else '20250701',
        'Fecha_Validez_Fin': null,
        'Categorias': {
            'A': {'Valor_Min': 88495.58, 'Valor_Max': 999999999},
            'B': {'Valor_Min': 57522.12, 'Valor_Max': 88495.57},
            'C': {'Valor_Min': 39823.01, 'Valor_Max': 57522.11},
            'D': {'Valor_Min': 0, 'Valor_Max': 39823.00}
        }
    },
    {
        'Emp_Id': 5,
        'Fecha_Validez_Inicio': '20240101',
        'Fecha_Validez_Fin': '20250531' if target.name == 'dev' else '20250630', 
        'Categorias': {
            'A': {'Valor_Min': 79646.02, 'Valor_Max': 999999999},
            'B': {'Valor_Min': 53097.35, 'Valor_Max': 79646.02},
            'C': {'Valor_Min': 30973.45, 'Valor_Max': 53097.35},
            'D': {'Valor_Min': 0, 'Valor_Max': 30973.45}
        }
    }
] -%}

{# Aplanamos la estructura para facilitar el bucle #}
{%- set rangos_aplanados = [] -%}
{%- for empresa in categoria_rangos_config -%}
    {%- for categoria, rango in empresa.Categorias.items() -%}
        {%- do rangos_aplanados.append({
            'Emp_Id': empresa.Emp_Id,
            'Categoria': categoria,
            'Valor_Min': rango.Valor_Min,
            'Valor_Max': rango.Valor_Max,
            'Fecha_Validez_Inicio': empresa.Fecha_Validez_Inicio,
            'Fecha_Validez_Fin': empresa.Fecha_Validez_Fin
        }) -%}
    {%- endfor -%}
{%- endfor -%}

-- Tabla de rangos de categorías por empresa generada dinámicamente
with CategoriaRangos as (
    select * from (
        values
        {% for rango in rangos_aplanados -%}
        (
            {{ rango.Emp_Id }},
            '{{ rango.Categoria }}',
            {{ rango.Valor_Min }},
            {{ rango.Valor_Max }},
            '{{ rango.Fecha_Validez_Inicio }}',
            {% if rango.Fecha_Validez_Fin %}'{{ rango.Fecha_Validez_Fin }}'{% else %}NULL{% endif %}
        ){% if not loop.last %},{% endif %}
        {%- endfor %}
    ) as t (Emp_Id, Categoria, Valor_Min, Valor_Max, Fecha_Validez_Inicio, Fecha_Validez_Fin)
),
ResumenFacturas AS
(
    SELECT Emp_Id, Suc_Id, sum(Sum_Valor_Neto) Valor_Neto, MAX(Factura_Fecha) Fecha_Id
    FROM {{ ref('BI_Kielsa_Agr_Sucursal_FechaHora') }}
    WHERE Factura_Fecha < cast(getdate() as date) 
    AND Factura_Fecha >= DATEADD(DAY, 1, EOMONTH(GETDATE(),-1))
    GROUP BY Emp_Id, Suc_Id
),
Proyeccion AS
(
    SELECT P.Emp_Id, P.Suc_Id, SUM(P.Valor_Neto) Valor_Neto
    FROM {{ ref('BI_Kielsa_Hecho_ProyeccionVenta_SucHora') }} P
    INNER JOIN ResumenFacturas R
    ON P.Emp_Id = R.Emp_Id
    AND P.Suc_Id = R.Suc_Id
    AND P.Fecha_Id >= R.Fecha_Id
    WHERE P.Fecha_Id < EOMONTH(GETDATE())
    GROUP BY P.Emp_Id, P.Suc_Id
),
TotalMes AS
(
    SELECT S.Emp_Id, S.Sucursal_Id as Suc_Id, ISNULL(R.Valor_Neto,0) + ISNULL(P.Valor_Neto,0) AS Valor_Mes, R.Fecha_Id
    FROM {{ ref('BI_Kielsa_Dim_Sucursal') }} S
    LEFT JOIN ResumenFacturas R
    ON S.Emp_Id = R.Emp_Id
    AND S.Sucursal_Id = R.Suc_Id
    LEFT JOIN Proyeccion P
    ON S.Emp_Id = P.Emp_Id
    AND S.Sucursal_Id = P.Suc_Id
),
ParetoClasificacionBase AS
(
    SELECT
        T.Emp_Id,
        T.Suc_Id,
        T.Valor_Mes,
        SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id ORDER BY T.Valor_Mes DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Acumulado,
        SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id) AS Total,
        CAST(SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id ORDER BY T.Valor_Mes DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FLOAT) 
            / NULLIF(SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id),0) AS Porc_Acumulado,
        -- Clasificación Pareto tradicional (A/B/C/D) por porcentaje acumulado
        CASE 
            WHEN CAST(SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id ORDER BY T.Valor_Mes DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FLOAT) 
                / NULLIF(SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id),0) <= 0.7 THEN 'A'
            WHEN CAST(SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id ORDER BY T.Valor_Mes DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FLOAT) 
                / NULLIF(SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id),0) <= 0.85 THEN 'B'
            WHEN CAST(SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id ORDER BY T.Valor_Mes DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FLOAT) 
                / NULLIF(SUM(T.Valor_Mes) OVER (PARTITION BY T.Emp_Id),0) <= 0.95 THEN 'C'
            ELSE 'D'
        END AS Pareto_Clasificacion,
        -- Clasificación personalizada por rango de valor de venta
        CR.Categoria as Categoria_Personalizada
    FROM TotalMes T
    LEFT JOIN CategoriaRangos CR
        ON T.Emp_Id = CR.Emp_Id
        AND T.Valor_Mes >= CR.Valor_Min
        AND T.Valor_Mes <= CR.Valor_Max
        AND T.Fecha_Id >= CR.Fecha_Validez_Inicio
        AND (CR.Fecha_Validez_Fin IS NULL OR T.Fecha_Id <= CR.Fecha_Validez_Fin)
),
CategoriaFinal AS
(
    SELECT
        *,
        COALESCE(Categoria_Personalizada, Pareto_Clasificacion) as Categoria_Final
    FROM ParetoClasificacionBase
),
ClaseLimites AS
(
    SELECT
        Emp_Id,
        Categoria_Final,
        MIN(Valor_Mes) AS Categoria_Valor_Inicial,
        MAX(Valor_Mes) AS Categoria_Valor_Final
    FROM CategoriaFinal
    GROUP BY Emp_Id, Categoria_Final
)
SELECT 
    ISNULL(P.Emp_Id, 0) AS Emp_Id,
    ISNULL(P.Suc_Id, 0) AS Suc_Id,
    ISNULL(P.Valor_Mes, 0.0) AS Valor_Mes,
    P.Acumulado,
    P.Total,
    P.Porc_Acumulado,
    P.Pareto_Clasificacion,
    P.Categoria_Personalizada,
    P.Categoria_Final,
    C.Categoria_Valor_Inicial,
    C.Categoria_Valor_Final
FROM CategoriaFinal P
LEFT JOIN ClaseLimites C
    ON P.Emp_Id = C.Emp_Id
    AND P.Categoria_Final = C.Categoria_Final