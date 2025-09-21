{%- set unique_key_list = ["PlanMonedero_Id", "Emp_Id"] -%}

{{
    config(
        as_columnstore=true,
        tags=["automation/eager", "automation_only"],
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
                if_another_exists_drop_it=true)
            }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
    )
}}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this,
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

WITH Dim_Monedero_Plan AS (
    SELECT --noqa: ST06
        ISNULL(Monedero_Id, 0) AS [PlanMonedero_Id],
        ISNULL(Emp_Id, 0) AS [Emp_Id],
        ISNULL(TRIM(UPPER(Monedero_Nombre)), 'N.D.') AS [PlanMonedero_Nombre],
        Monedero_Desde AS [PlanMonedero_Desde],
        Monedero_Hasta AS [PlanMonedero_Hasta],
        Monedero_Limite_Uso AS [PlanMonedero_Limite_Uso],
        Monedero_Estado AS [PlanMonedero_Estado],
        Monedero_Acum_Sig_Compra AS [PlanMonedero_Acum_Sig_Compra],
        Monedero_Activacion_Site AS [PlanMonedero_Activacion_Site],
        Monedero_Asigna_Puntos_Site AS [PlanMonedero_Asigna_Puntos_Site],
        Monedero_Monto_Minimo AS [PlanMonedero_Monto_Minimo],
        Monedero_Acumula_Punto_Venta_Monedero AS [PlanMonedero_Acumula_Punto_Venta_Monedero],
        Monedero_Fec_Actualizacion AS [PlanMonedero_Fec_Actualizacion],
        Monedero_Monto_Minimo_Canje AS [PlanMonedero_Monto_Minimo_Canje],
        Aplica_Cliente,
        Aplica_Validacion_Edad,
        Edad_Minima_Permitida,
        Valida_Numero_Tarjeta,
        Monedero_Monto_Maximo_Acumulado AS [PlanMonedero_Monto_Maximo_Acumulado],
        Dias_Inactividad_Limpieza_Saldo,
        Marca_Comercial_Id,
        Monedero_Acumula_FP_Monedero AS [PlanMonedero_Acumula_FP_Monedero],
        Monedero_Formula_Acum AS [PlanMonedero_Formula_Acum],
        Monedero_Hora_Inicio AS [PlanMonedero_Hora_Inicio],
        Monedero_Hora_Final AS [PlanMonedero_Hora_Final],
        Monedero_Dia1 AS [PlanMonedero_Dia1],
        Monedero_Dia2 AS [PlanMonedero_Dia2],
        Monedero_Dia3 AS [PlanMonedero_Dia3],
        Monedero_Dia4 AS [PlanMonedero_Dia4],
        Monedero_Dia5 AS [PlanMonedero_Dia5],
        Monedero_Dia6 AS [PlanMonedero_Dia6],
        Monedero_Dia7 AS [PlanMonedero_Dia7],
        Fecha_Actualizado AS Fecha_Carga,
        Fecha_Actualizado
    FROM {{ ref('DL_Kielsa_Monedero_Plan') }}
    {%- if is_incremental() %}
        WHERE Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
    {%- endif %}
)

SELECT
    PlanMonedero_Id,
    Emp_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'PlanMonedero_Id'], input_length=50) }} AS [EmpPlanMonedero_Id],
    PlanMonedero_Nombre,
    PlanMonedero_Desde,
    PlanMonedero_Hasta,
    PlanMonedero_Limite_Uso,
    PlanMonedero_Estado,
    PlanMonedero_Acum_Sig_Compra,
    PlanMonedero_Activacion_Site,
    PlanMonedero_Asigna_Puntos_Site,
    PlanMonedero_Monto_Minimo,
    PlanMonedero_Acumula_Punto_Venta_Monedero,
    PlanMonedero_Fec_Actualizacion,
    PlanMonedero_Monto_Minimo_Canje,
    Aplica_Cliente,
    Aplica_Validacion_Edad,
    Edad_Minima_Permitida,
    Valida_Numero_Tarjeta,
    PlanMonedero_Monto_Maximo_Acumulado,
    Dias_Inactividad_Limpieza_Saldo,
    Marca_Comercial_Id,
    PlanMonedero_Acumula_FP_Monedero,
    PlanMonedero_Formula_Acum,
    PlanMonedero_Hora_Inicio,
    PlanMonedero_Hora_Final,
    PlanMonedero_Dia1,
    PlanMonedero_Dia2,
    PlanMonedero_Dia3,
    PlanMonedero_Dia4,
    PlanMonedero_Dia5,
    PlanMonedero_Dia6,
    PlanMonedero_Dia7,
    Fecha_Carga,
    Fecha_Actualizado
FROM Dim_Monedero_Plan
