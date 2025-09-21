{%- set nombre_esquema_particion = "ps_" + this.identifier + "_fecha" -%}
{%- set on_clause = nombre_esquema_particion ~ "([Factura_Fecha])" -%}
{%- set unique_key_list = ["Factura_Id","Suc_Id","Emp_Id","TipoDoc_Id","Caja_Id","Factura_Fecha"] -%}

{{
    config(
		as_columnstore=false,
		tags=["periodo/diario"],
		materialized="incremental",
        incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		on_clause_filegroup = on_clause,
        merge_insert_only=true,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		pre_hook=[
        "{%- if is_incremental() -%}
            {% set sql_inicializar_particion= '' %}
        {%- else -%}
            EXEC ADM_FARINTER.dbo.pa_inicializar_particiones
                @p_base_datos = '{{ this.database }}',
                @p_nombre_esquema_particion = 'ps_{{ this.identifier }}_fecha',
                @p_nombre_funcion_particion = 'pf_{{ this.identifier }}_fecha',
                @p_periodo_tipo = 'Anual', --'Anual' o 'Mensual'
                @p_tipo_datos = 'Fecha', --'Fecha' o 'AnioMes'
                @p_fecha_base = '2018-01-01'
        {%- endif -%}"
        ],
		post_hook=[
		"{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_clustered_columnstore_index(is_incremental=is_incremental(),
			if_another_exists_drop_it=true) }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", 
			create_clustered=false, 
			is_incremental=is_incremental(), 
			if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado'], included_columns=['Factura_Fecha']) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Factura_Fecha']) }}",
		"EXEC ADM_FARINTER.dbo.pa_comprimir_indices_particiones_anteriores 
			@p_base_datos = '{{this.database}}',
		 	@p_esquema_tabla = '{{this.schema}}', 
			@p_nombre_tabla = '{{this.identifier}}', 
			@p_tipo_datos = 'Fecha';"		
			]
		
) }}

{% if is_incremental() %}
    {% set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101') as fecha_a from  """ ~ this,
        relation_not_found_value='19000101'|string)|string %}
{% else %}
    {% set last_date = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=180)).strftime('%Y%m%d') %}
{% endif %}

-- Encabezado histórico: agrupa por factura y extrae columnas necesarias para requisitos 2, 3, 4
-- Req 2: Zona_Id, Departamento_Id, Municipio_Id, Ciudad_Id, TipoSucursal_Id para sucursal_hist
-- Req 3: Plan_Id para monedero_hist
-- Req 4: TipoCliente_Id para clientes_hist
-- Claves: EmpSucDocCajFac_Id ya viene concatenado en Factura_Id

WITH agregado_factura AS (
    SELECT -- noqa: ST06 
        CAST(Fecha_Id AS DATE) AS Factura_Fecha,
        CAST(Pais_Id AS INT) AS Emp_Id,
        CASE
            WHEN Sucursal_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Sucursal_Id, CHARINDEX('-', Sucursal_Id) + 1, 50) AS INT)
            ELSE TRY_CAST(Sucursal_Id AS INT)
        END AS Suc_Id,
        Sucursal_Id AS EmpSuc_Id,
        CASE
            WHEN Documento_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Documento_Id, CHARINDEX('-', Documento_Id) + 1, 50) AS INT)
            ELSE TRY_CAST(Documento_Id AS INT)
        END AS TipoDoc_Id,
        Documento_Id AS EmpTipoDoc_Id,
        --TRY_CAST(SubDocumento_Id AS INT) AS SubDoc_Id,
        SubDocumento_Id AS SubDoc_Id,
        CASE
            WHEN Factura_Id LIKE '%-%' THEN TRY_CAST(
                SUBSTRING(
                    Factura_Id,
                    CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1,
                    CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1)
                    - CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1)
                    - 1
                ) AS INT
            ) ELSE TRY_CAST(Factura_Id AS INT)
        END AS Caja_Id,
        CASE
            WHEN Factura_Id LIKE '%-%' THEN TRY_CAST(
                SUBSTRING(
                    Factura_Id,
                    CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1) + 1,
                    LEN(Factura_Id) - CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1)
                ) AS VARCHAR(50)
            ) ELSE TRY_CAST(Factura_Id AS INT)
        END AS Factura_Id,
        Factura_Id AS EmpSucDocCajFac_Id,
        CAST(Fecha_Id AS DATE) AS Fecha_Id,
        -- Requisito 2: Sucursal histórica
        CASE
            WHEN Zona_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Zona_Id, CHARINDEX('-', Zona_Id) + 1, 50) AS INT)
            ELSE TRY_CAST(Zona_Id AS INT)
        END AS Zona_Id,
        Zona_Id AS EmpZona_Id,
        CASE
            WHEN Departamento_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Departamento_Id, CHARINDEX('-', Departamento_Id) + 1, 50) AS INT)
            ELSE TRY_CAST(Departamento_Id AS INT)
        END AS Departamento_Id,
        Departamento_Id AS EmpDep_Id,
        CASE
            WHEN Municipio_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(Municipio_Id), 1, CHARINDEX('-', REVERSE(Municipio_Id)) - 1)) AS INT)
            ELSE TRY_CAST(Municipio_Id AS INT)
        END AS Municipio_Id,
        Municipio_Id AS EmpDepMun_Id,
        CASE
            WHEN Ciudad_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(Ciudad_Id), 1, CHARINDEX('-', REVERSE(Ciudad_Id)) - 1)) AS INT)
            ELSE TRY_CAST(Ciudad_Id AS INT)
        END AS Ciudad_Id,
        Ciudad_Id AS EmpDepMunCiu_Id,
        CASE
            WHEN TipoSucursal_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(TipoSucursal_Id, CHARINDEX('-', TipoSucursal_Id) + 1, 50) AS INT)
            ELSE TRY_CAST(TipoSucursal_Id AS INT)
        END AS TipoSucursal_Id,
        TipoSucursal_Id AS EmpTipoSucursal_Id,
        CASE
            WHEN Plan_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Plan_Id, CHARINDEX('-', Plan_Id) + 1, 50) AS INT)
            ELSE TRY_CAST(Plan_Id AS INT)
        END AS Plan_Id,
        Plan_Id AS EmpPlan_Id,
        -- Requisito 4: Tipo cliente histórico
        CASE
            WHEN TipoCliente_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(TipoCliente_Id, CHARINDEX('-', TipoCliente_Id) + 1, 50) AS INT)
            ELSE TRY_CAST(TipoCliente_Id AS INT)
        END AS TipoCliente_Id,
        TipoCliente_Id AS EmpTipoCliente_Id,
        -- Campos adicionales de control
        Cliente_Id,
        Monedero_Id,
        CanalVenta_Id,
        Anio_Id,
        Mes_Id
        -- Agregaciones de métricas a nivel factura
    FROM {{ source('BI_FARINTER', 'BI_Hecho_VentasHist_Kielsa') }} WITH (NOLOCK)
    WHERE
        CAST(Fecha_Id AS DATE) >= CAST('{{ last_date }}' AS DATE)
        AND Anio_Id >= {{ last_date[0:4] }}
        AND (Mes_Id >= {{ last_date[4:6] }} OR Anio_Id > {{ last_date[0:4] }})
)

SELECT --noqa: ST06
    ISNULL(MAX(Factura_Fecha), '19000101') AS Factura_Fecha,
    ISNULL(Emp_Id, 0) AS Emp_Id,
    ISNULL(MAX(Suc_Id), 0) AS Suc_Id,
    ISNULL(EmpSuc_Id, 'X') AS EmpSuc_Id,
    ISNULL(MAX(TipoDoc_Id), 0) AS TipoDoc_Id,
    ISNULL(MAX(EmpTipoDoc_Id), 'X') AS EmpTipoDoc_Id,
    ISNULL(MAX(SubDoc_Id), 0) AS SubDoc_Id,
    ISNULL(MAX(Caja_Id), 0) AS Caja_Id,
    ISNULL(MAX(Factura_Id), 0) AS Factura_Id,
    ISNULL(MAX(EmpSucDocCajFac_Id), 'X') AS EmpSucDocCajFac_Id,
    MAX(Fecha_Id) AS Fecha_Id,
    ISNULL(MAX(Zona_Id), 0) AS Zona_Id,
    MAX(EmpZona_Id) AS EmpZona_Id,
    ISNULL(MAX(Departamento_Id), 0) AS Departamento_Id,
    MAX(EmpDep_Id) AS EmpDep_Id,
    ISNULL(MAX(Municipio_Id), 0) AS Municipio_Id,
    MAX(EmpDepMun_Id) AS EmpDepMun_Id,
    ISNULL(MAX(Ciudad_Id), 0) AS Ciudad_Id,
    MAX(EmpDepMunCiu_Id) AS EmpDepMunCiu_Id,
    ISNULL(MAX(TipoSucursal_Id), 0) AS TipoSucursal_Id,
    MAX(EmpTipoSucursal_Id) AS EmpTipoSucursal_Id,
    ISNULL(MAX(Plan_Id), 0) AS Plan_Id,
    MAX(EmpPlan_Id) AS EmpPlan_Id,
    ISNULL(MAX(TipoCliente_Id), 0) AS TipoCliente_Id,
    MAX(EmpTipoCliente_Id) AS EmpTipoCliente_Id,
    MAX(Cliente_Id) AS Cliente_Id,
    MAX(Monedero_Id) AS Monedero_Id,
    MAX(CanalVenta_Id) AS CanalVenta_Id,
    CASE WHEN CAST(MAX(Fecha_Id) AS DATETIME) > GETDATE() - 7 THEN GETDATE() ELSE CAST(ISNULL(MAX(Fecha_Id), '19000101') AS DATETIME) END AS Fecha_Actualizado
FROM agregado_factura
GROUP BY
    Anio_Id,
    Mes_Id,
    Fecha_Id,
    Emp_Id,
    EmpSuc_Id,
    EmpSucDocCajFac_Id
