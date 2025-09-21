{%- set unique_key_list = ["Proveedor_Id","Emp_Id"] -%}
{{ 
    config(
        as_columnstore=false,
        tags=["periodo/diario"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="append_new_columns",
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        merge_soft_delete_column="Indicador_Borrado",
        merge_soft_delete_filter_columns=['Emp_Id'],
        post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
      "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
      "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
		
) }}

{%- set query_empresas -%}
SELECT Empresa_Id, Empresa_Id_Original, Pais_Id
	--, LS_LDCOM_RepLocal, D_LDCOM_RepLocal 
	--,LS_LDCOM, D_LDCOM
	,LS_LDCOM_Replica AS Servidor_Vinculado, D_LDCOM_Replica as Base_Datos
FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Empresa WITH (NOLOCK)
WHERE LS_LDCOM_RepLocal IS NOT NULL and Es_Empresa_Principal = 1
{%- endset -%}
{%- set empresas = run_query_and_return(query_empresas) -%} {# Returns: [{Empresa_Id,Emp_Id_Original,Pais_Id,LS_LDCOM_Replica,D_LDCOM_Replica}] #}

{# Verificar cuales estan accesibles #}
{%- set valid_empresas = [] -%}
{%- for item in empresas -%}
    {%- if check_linked_server(item['Servidor_Vinculado']) -%}
        {%- do valid_empresas.append(item) -%}
    {%- endif -%}
{%- endfor -%}

WITH DatosBase AS (
{%- for item in valid_empresas -%}
        {%- if not loop.first %}
            UNION ALL{%- endif %}
        SELECT --noqa: ST06
            ISNULL({{ item['Empresa_Id'] }}, 0) AS [Emp_Id],
            ISNULL(CAST(A.Prov_Id AS INT), 0) AS Proveedor_Id,
            CAST(A.Prov_Nombre AS NVARCHAR(120)) COLLATE DATABASE_DEFAULT AS [Proveedor_Nombre], --noqa: RF03
            ISNULL(CAST(A.Categoria_Id AS SMALLINT), 0) AS Categoria_Id,
            ISNULL(CAST(A.SubCategoria_Id AS SMALLINT), 0) AS SubCategoria_Id,
            ISNULL(CAST(A.Pais_Id AS SMALLINT), 0) AS Pais_Id,
            CAST(ISNULL(A.Prov_Cedula, '') AS VARCHAR(25)) COLLATE DATABASE_DEFAULT AS Cedula, --noqa: RF03
            CAST(ISNULL(A.Prov_Direccion, '') AS VARCHAR(255)) COLLATE DATABASE_DEFAULT AS Direccion, --noqa: RF03
            CAST(ISNULL(A.Prov_Email, '') AS VARCHAR(100)) COLLATE DATABASE_DEFAULT AS Email, --noqa: RF03
            CAST(ISNULL(A.Prov_Fax, '') AS VARCHAR(15)) COLLATE DATABASE_DEFAULT AS Fax, --noqa: RF03
            CAST(ISNULL(A.Prov_Telefono, '') AS VARCHAR(15)) COLLATE DATABASE_DEFAULT AS Telefono, --noqa: RF03
            CAST(ISNULL(A.Prov_Razon_Social, '') AS VARCHAR(120)) COLLATE DATABASE_DEFAULT AS Razon_Social, --noqa: RF03
            CAST(ISNULL(A.Prov_Extranjero, 0) AS BIT) AS Indicador_Extranjero,
            CAST(ISNULL(A.Prov_Fec_Actualizacion, '1900-01-01') AS DATETIME) AS Prov_Fec_Actualizacion,
            CAST(ISNULL(A.Prov_Distribuye, 0) AS BIT) AS Indicador_Distribuye,
            CAST(ISNULL(A.Prov_Descuento_financiero, 0) AS DECIMAL(16, 4)) AS Descuento_financiero,
            CAST(ISNULL(A.Prov_Dias_reposicion, 0) AS SMALLINT) AS Dias_reposicion,
            CAST(ISNULL(A.Proveedor_Activo, 0) AS BIT) AS Indicador_Activo,
            CAST(ISNULL(A.Prov_Clasificacion_Id, 0) AS SMALLINT) AS Clasificacion_Id,
            CAST(ISNULL(A.Prov_Tipo_Compra, 0) AS SMALLINT) AS Tipo_Compra,
            CAST(ISNULL(A.Prov_Dias_Vencimiento_Compra, 0) AS SMALLINT) AS Dias_Vencimiento_Compra,
            ABS(CAST(CAST(HASHBYTES('SHA2_256', CONCAT(A.Prov_Id, '-', {{ item['Empresa_Id'] }})) AS INT) AS BIGINT)) AS Hash_ProveedorEmp
        FROM {{ item['Servidor_Vinculado'] }}.{{ item['Base_Datos'] }}.dbo.Proveedor AS A
        WHERE
            A.Emp_Id = {{ item['Empresa_Id_Original'] }}
        {%- if is_incremental() %}
            --AND A.Prov_Fec_Actualizacion >= CAST('{{ last_date }}' AS DATETIME)
            --Para poder marcar soft delete no debe filtrarse aqui (o se marcaran como borrados los que no se actualicen)
        {%- endif %}
    {% endfor -%}
),

ND AS (
    SELECT --noqa: ST06
        E.Empresa_Id AS Emp_Id,
        0 AS Proveedor_Id,
        CAST('No Definido' AS NVARCHAR(120)) AS Proveedor_Nombre,
        CAST(0 AS SMALLINT) AS Categoria_Id,
        CAST(0 AS SMALLINT) AS SubCategoria_Id,
        CAST(E.Pais_Id AS SMALLINT) AS Pais_Id,
        CAST('' AS VARCHAR(25)) AS Cedula,
        CAST('' AS VARCHAR(255)) AS Direccion,
        CAST('' AS VARCHAR(100)) AS Email,
        CAST('' AS VARCHAR(15)) AS Fax,
        CAST('' AS VARCHAR(15)) AS Telefono,
        CAST('' AS VARCHAR(120)) AS Razon_Social,
        CAST(0 AS BIT) AS Indicador_Extranjero,
        CAST('1900-01-01' AS DATETIME) AS Prov_Fec_Actualizacion,
        CAST(0 AS BIT) AS Indicador_Distribuye,
        CAST(0 AS DECIMAL(16, 4)) AS Descuento_financiero,
        CAST(0 AS SMALLINT) AS Dias_reposicion,
        CAST(0 AS BIT) AS Indicador_Activo,
        CAST(0 AS SMALLINT) AS Clasificacion_Id,
        CAST(0 AS SMALLINT) AS Tipo_Compra,
        CAST(0 AS SMALLINT) AS Dias_Vencimiento_Compra,
        ABS(CAST(CAST(HASHBYTES('SHA2_256', CONCAT(0, '-', E.Empresa_Id)) AS INT) AS BIGINT)) AS Hash_ProveedorEmp
    FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Empresa AS E WITH (NOLOCK)
    WHERE E.Empresa_Id IN (SELECT DISTINCT db.Emp_Id FROM DatosBase AS db)
)

SELECT --noqa: ST06
    X.Emp_Id,
    X.Proveedor_Id,
    X.Proveedor_Nombre,
    X.Categoria_Id,
    X.SubCategoria_Id,
    X.Pais_Id,
    X.Cedula,
    X.Direccion,
    X.Email,
    X.Fax,
    X.Telefono,
    X.Razon_Social,
    X.Indicador_Extranjero,
    X.Prov_Fec_Actualizacion,
    X.Indicador_Distribuye,
    X.Descuento_financiero,
    X.Dias_reposicion,
    X.Indicador_Activo,
    X.Clasificacion_Id,
    X.Tipo_Compra,
    X.Dias_Vencimiento_Compra,
    X.Hash_ProveedorEmp,
    CAST(0 AS BIT) AS [Indicador_Borrado],
    GETDATE() AS [Fecha_Carga],
    GETDATE() AS [Fecha_Actualizado]
FROM (
    SELECT * FROM DatosBase
    UNION ALL
    SELECT * FROM ND
) AS X
