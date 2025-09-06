{%- set nombre_esquema_particion = "ps_" + this.identifier + "_fecha" -%}
{%- set on_clause = nombre_esquema_particion ~ "([Factura_Fecha])" -%}
{%- set unique_key_list = ["Factura_Id","Suc_Id","Emp_Id","TipoDoc_Id","Caja_Id","Factura_Fecha","Articulo_Id"] -%}

{{
    config(
		as_columnstore=false,
		tags=["periodo/diario","automation/periodo_por_hora"],
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
                @p_periodo_tipo = 'Mensual', --'Anual' o 'Mensual'
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
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this,
        relation_not_found_value='19000101'|string)|string %}
{% else %}
    {% set last_date = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=180)).strftime('%Y%m%d') %}
{% endif %}


SELECT -- noqa: ST06 
    ISNULL(CAST(Fecha_Id AS DATE), '19000101') AS Factura_Fecha,
    ISNULL(CAST(Pais_Id AS INT), 0) AS Emp_Id,
    ISNULL(CASE
        WHEN Sucursal_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Sucursal_Id, CHARINDEX('-', Sucursal_Id) + 1, 50) AS INT)
        ELSE TRY_CAST(Sucursal_Id AS INT)
    END, 0) AS Suc_Id,
    ISNULL(CASE
        WHEN Documento_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Documento_Id, CHARINDEX('-', Documento_Id) + 1, 50) AS INT)
        ELSE TRY_CAST(Documento_Id AS INT)
    END, 0) AS TipoDoc_Id,
    ISNULL(Documento_Id, 'X') AS EmpTipoDoc_Id,
    ISNULL(CASE
        WHEN Factura_Id LIKE '%-%' THEN TRY_CAST(
            SUBSTRING(
                Factura_Id,
                CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1,
                CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1)
                - CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1)
                - 1
            ) AS INT
        ) ELSE TRY_CAST(Factura_Id AS INT)
    END, 0) AS Caja_Id,
    ISNULL(CASE
        WHEN Factura_Id LIKE '%-%' THEN TRY_CAST(
            SUBSTRING(
                Factura_Id,
                CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1) + 1,
                LEN(Factura_Id) - CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id, CHARINDEX('-', Factura_Id) + 1) + 1) + 1)
            ) AS VARCHAR(50)
        ) ELSE TRY_CAST(Factura_Id AS INT)
    END, 0) AS Factura_Id,
    Factura_Id AS EmpSucDocCajFac_Id,
    CAST(Fecha_Id AS DATE) AS Fecha_Id,
    ISNULL(CASE
        WHEN Articulo_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Articulo_Id, CHARINDEX('-', Articulo_Id) + 1, 50) AS NVARCHAR(50))
        ELSE TRY_CAST(Articulo_Id AS NVARCHAR(50))
    END, 0) AS Articulo_Id,
    Articulo_Id AS EmpArt_Id,
    CASE
        WHEN Casa_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Casa_Id, CHARINDEX('-', Casa_Id) + 1, 50) AS INT)
        ELSE TRY_CAST(Casa_Id AS INT)
    END AS Casa_Id,
    Casa_Id AS EmpCasa_Id,
    CASE
        WHEN Marca1_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(Marca1_Id, CHARINDEX('-', Marca1_Id) + 1, 50) AS INT)
        ELSE TRY_CAST(Marca1_Id AS INT)
    END AS Marca1_Id,
    Marca1_Id AS EmpMarca1_Id,
    CASE
        WHEN CategoriaArt_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(CategoriaArt_Id, CHARINDEX('-', CategoriaArt_Id) + 1, 50) AS INT)
        ELSE TRY_CAST(CategoriaArt_Id AS INT)
    END AS CategoriaArt_Id,
    CategoriaArt_Id AS EmpCategoriaArt_Id,
    CASE
        WHEN DeptoArt_Id LIKE '%-%' THEN TRY_CAST(SUBSTRING(DeptoArt_Id, CHARINDEX('-', DeptoArt_Id) + 1, 50) AS INT)
        ELSE TRY_CAST(DeptoArt_Id AS INT)
    END AS DeptoArt_Id,
    DeptoArt_Id AS EmpDeptoArt_Id,
    CASE
        WHEN SubCategoria1Art_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(SubCategoria1Art_Id), 1, CHARINDEX('-', REVERSE(SubCategoria1Art_Id)) - 1)) AS INT)
        ELSE TRY_CAST(SubCategoria1Art_Id AS INT)
    END AS SubCategoria1Art_Id,
    SubCategoria1Art_Id AS EmpCatSubCategoria1Art_Id,
    CASE
        WHEN SubCategoria2Art_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(SubCategoria2Art_Id), 1, CHARINDEX('-', REVERSE(SubCategoria2Art_Id)) - 1)) AS INT)
        ELSE TRY_CAST(SubCategoria2Art_Id AS INT)
    END AS SubCategoria2Art_Id,
    SubCategoria2Art_Id AS EmpCatSubCategoria1_2Art_Id,
    CASE
        WHEN SubCategoria3Art_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(SubCategoria3Art_Id), 1, CHARINDEX('-', REVERSE(SubCategoria3Art_Id)) - 1)) AS INT)
        ELSE TRY_CAST(SubCategoria3Art_Id AS INT)
    END AS SubCategoria3Art_Id,
    SubCategoria3Art_Id AS EmpCatSubCategoria1_2_3Art_Id,
    CASE
        WHEN SubCategoria4Art_Id LIKE '%-%' THEN TRY_CAST(REVERSE(SUBSTRING(REVERSE(SubCategoria4Art_Id), 1, CHARINDEX('-', REVERSE(SubCategoria4Art_Id)) - 1)) AS INT)
        ELSE TRY_CAST(SubCategoria4Art_Id AS INT)
    END AS SubCategoria4Art_Id,
    SubCategoria4Art_Id AS EmpCatSubCategoria1_2_3_4Art_Id,
    CAST(Proveedor_Id AS INT) AS Proveedor_Id,
    CONCAT(Pais_Id, '-', Proveedor_Id) AS EmpProv_Id,
    CAST(Cuadro_Id AS INT) AS Cuadro_Id,
    CAST(Mecanica_Id AS NVARCHAR(50)) AS Mecanica_Id,
    CASE WHEN CAST([Fecha_Id] AS DATETIME) > GETDATE() - 7 THEN GETDATE() ELSE CAST(ISNULL([Fecha_Id], '19000101') AS DATETIME) END AS Fecha_Actualizado
FROM {{ source('BI_FARINTER', 'BI_Hecho_VentasHist_Kielsa') }} WITH (NOLOCK)
--order by Fecha_Id desc
WHERE
    CAST(Fecha_Id AS DATE) >= CAST('{{ last_date }}' AS DATE)
    AND Anio_Id >= {{ last_date[0:4] }}
    AND (Mes_Id >= {{ last_date[4:6] }} OR Anio_Id > {{ last_date[0:4] }})
