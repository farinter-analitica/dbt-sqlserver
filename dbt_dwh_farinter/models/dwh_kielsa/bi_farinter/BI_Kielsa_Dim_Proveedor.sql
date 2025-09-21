{%- set unique_key_list = ["Proveedor_Id", "Emp_Id"] -%}

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

WITH Dim_Proveedor AS (
    SELECT --noqa: ST06
        ISNULL(Proveedor_Id, 0) AS [Proveedor_Id],
        ISNULL(Emp_Id, 0) AS [Emp_Id],
        TRIM(UPPER(ISNULL(Proveedor_Nombre, 'N.D.'))) AS [Proveedor_Nombre],
        ISNULL(Categoria_Id, 0) AS Categoria_Id,
        ISNULL(SubCategoria_Id, 0) AS SubCategoria_Id,
        ISNULL(Pais_Id, 0) AS Pais_Id,
        TRIM(UPPER(ISNULL(Cedula, ''))) AS Cedula,
        TRIM(UPPER(ISNULL(Direccion, ''))) AS Direccion,
        TRIM(UPPER(ISNULL(Email, ''))) AS Email,
        TRIM(UPPER(ISNULL(Fax, ''))) AS Fax,
        TRIM(UPPER(ISNULL(Telefono, ''))) AS Telefono,
        TRIM(UPPER(ISNULL(Razon_Social, ''))) AS Razon_Social,
        ISNULL(Indicador_Extranjero, 0) AS Indicador_Extranjero,
        ISNULL(Indicador_Distribuye, 0) AS Indicador_Distribuye,
        ISNULL(Descuento_financiero, 0) AS Descuento_financiero,
        ISNULL(Dias_reposicion, 0) AS Dias_reposicion,
        ISNULL(Indicador_Activo, 0) AS Indicador_Activo,
        ISNULL(Clasificacion_Id, 0) AS Clasificacion_Id,
        ISNULL(Tipo_Compra, 0) AS Tipo_Compra,
        ISNULL(Dias_Vencimiento_Compra, 0) AS Dias_Vencimiento_Compra,
        Fecha_Actualizado AS Fecha_Carga,
        Fecha_Actualizado
    FROM {{ ref('DL_Kielsa_Proveedor') }}
    {%- if is_incremental() %}
        WHERE Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
    {%- endif %}
)

SELECT
    Proveedor_Id,
    Emp_Id,
    {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Proveedor_Id'], input_length=50) }} AS [EmpProveedor_Id],
    Proveedor_Nombre,
    Categoria_Id,
    SubCategoria_Id,
    Pais_Id,
    Cedula,
    Direccion,
    Email,
    Fax,
    Telefono,
    Razon_Social,
    Indicador_Extranjero,
    Indicador_Distribuye,
    Descuento_financiero,
    Dias_reposicion,
    Indicador_Activo,
    Clasificacion_Id,
    Tipo_Compra,
    Dias_Vencimiento_Compra,
    Fecha_Carga,
    Fecha_Actualizado
FROM Dim_Proveedor
