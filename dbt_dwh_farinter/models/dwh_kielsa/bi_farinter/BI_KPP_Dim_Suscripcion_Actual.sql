{% set unique_key_list = ["TarjetaKC_Id"] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
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
	{%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
	{%- set last_date = '19000101' %}
{%- endif %}


SELECT ISNULL(A.TarjetaKC_Id COLLATE DATABASE_DEFAULT,'') AS [TarjetaKC_Id],
    A.Cliente_Nombre COLLATE DATABASE_DEFAULT AS [Cliente_Nombre],
    CAST(A.FRegistro AS DATE) AS Fecha_Registro,
    CAST(A.FVigencia AS DATETIME) AS Momento_Registro,
    CAST(A.FVigencia AS DATE) AS Fecha_Vigencia,
    CAST(A.FActivacion AS DATE) AS Fecha_Activacion,
    CAST(A.FDesactivacion AS DATE) AS Fecha_Desactivacion,
    A.Cobro COLLATE DATABASE_DEFAULT AS Cobro_Realizado,
    A.Origen,
    A.Usuario_Registro COLLATE DATABASE_DEFAULT AS Usuario_Registro,
    /*
    C.Monedero_Celular AS Celular,
    C.Monedero_Telefono AS Telefono,
    C.Monedero_Email AS Correo,
    */
    CASE
        WHEN A.Transaccion_Tarjeta_Credito = 1 THEN 'Tarjeta Credito'
        WHEN E.TarjetaKC_Id IS NOT NULL THEN 'Kielsa Plus Anual'
        ELSE 'Kielsa Cash'
    END Tipo_Pago,
    A.TipoPlan AS [Plan_Id],
    B.Nombre AS [Plan_Nombre],
    --C.MonederoTarj_Saldo AS SaldoKC,
    /*
    F.Sucursal_Nombre,
    F.Zona_Nombre,
    F.Ciudad_Nombre,
    F.JOB AS JOP,
    F.Supervisor,
    */
    --D.Monedero_Nombre AS Plan_Monedero_Nombre,
    A.Suscripcion_Id,
    A.Identidad_Limpia AS Monedero_Id,
    CASE
        WHEN C.Ingreso BETWEEN dateadd(DAY, -1, A.FRegistro)
        AND dateadd(DAY, 1, A.FRegistro) THEN 'Auto'
        ELSE 'Organica'
    END AS Tipo_Sucripcion,
    datediff(year, C.Nacimiento, A.FRegistro) AS Edad_En_Suscripcion,
    {# dbo.fnc_ProperCase(C.Monedero_Primer_Nombre) + ' ' +
        dbo.fnc_ProperCase(C.Monedero_Segundo_Nombre) + ' ' +
        dbo.fnc_ProperCase(C.Monedero_Primer_Apellido) + ' ' +
        dbo.fnc_ProperCase(C.Monedero_Segundo_Apellido) AS Monedero_Nombre_Completo, #}
    A.Fecha_Actualizado
FROM DL_FARINTER.dbo.DL_Kielsa_KPP_Suscripcion AS A -- {{ source('DL_FARINTER','DL_Kielsa_KPP_Suscripcion') }}
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_KPP_Plan_Suscripcion AS B  -- {{ ref('DL_Kielsa_KPP_Plan_Suscripcion') }}
    ON A.TipoPlan = B.Plan_Id
    INNER JOIN [BI_Kielsa_Dim_Monedero] AS C  -- {{ ref('BI_Kielsa_Dim_Monedero') }}
    ON A.Identidad_Limpia = C.Monedero_Id
    AND C.Emp_Id = 1
    {# INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Monedero_Plan AS D -- {{ source('DL_FARINTER','DL_Kielsa_Monedero_Plan') }}
    ON C.Emp_Id = D.Emp_Id
    AND C.Monedero_Id = D.Monedero_Id #}
    LEFT JOIN (
        SELECT DISTINCT TarjetaKC_Id
        FROM DL_FARINTER.dbo.DL_Kielsa_KPP_Saldo_x_Cliente -- {{ ref('DL_Kielsa_KPP_Saldo_x_Cliente') }}
    ) AS E ON A.TarjetaKC_Id = E.TarjetaKC_Id COLLATE DATABASE_DEFAULT
    INNER JOIN DL_FARINTER.dbo.DL_Kielsa_Sucursal AS F -- {{ source('DL_FARINTER', 'DL_Kielsa_Sucursal') }}
    ON A.Sucursal_Registro = F.Sucursal_Id
    AND F.Emp_Id = C.Emp_Id
WHERE C.Emp_Id = 1 AND (A.Indicador_Borrado <> 1 OR A.Indicador_Borrado IS NULL)
{%- if is_incremental() %}
    AND A.Fecha_Actualizado >= '{{ last_date }}'
{%- endif %}