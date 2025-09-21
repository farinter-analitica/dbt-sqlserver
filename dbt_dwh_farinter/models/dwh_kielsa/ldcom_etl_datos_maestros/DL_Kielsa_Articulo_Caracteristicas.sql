
{%- set unique_key_list = ["Articulo_Id","Emp_Id","Caract_Id"] -%}
{{
    config(
        as_columnstore=true,
        tags=["periodo/diario"],
        materialized="table",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Caract_Fec_Actualizacion"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ",
                create_clustered=false,
                is_incremental=is_incremental(),
                if_another_exists_drop_it=true) }}"
        ]
    )
}}

{%- set query_empresas -%}
SELECT Empresa_Id, Empresa_Id_Original, Pais_Id,
       LS_LDCOM_Replica AS Servidor_Vinculado, D_LDCOM_Replica as Base_Datos
FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Empresa WITH (NOLOCK)
WHERE LS_LDCOM_RepLocal IS NOT NULL and Es_Empresa_Principal = 1
{%- endset -%}
{%- set empresas = run_query_and_return(query_empresas) -%} 

{# Verificar cuales estan accesibles #}
{%- set valid_empresas = [] -%}
{%- for item in empresas -%}
    {%- if check_linked_server(item['Servidor_Vinculado']) -%}
        {%- do valid_empresas.append(item) -%}
    {%- endif -%}
{%- endfor -%}

{%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Ult_Fec_Actualizacion)), 112), '19000101') as fecha_a from  """ ~ this,
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

WITH DatosBase AS (
{%- for item in valid_empresas -%}
        {%- if not loop.first %}UNION ALL{%- endif %}
        SELECT
            ISNULL({{ item['Empresa_Id'] }}, 0) AS Emp_Id,
            Articulo_Id COLLATE DATABASE_DEFAULT AS Articulo_Id,
            ISNULL(CAST(Caract_Id AS INT), 0) AS Caract_Id,
            Caract_Nombre COLLATE DATABASE_DEFAULT AS Caract_Nombre,
            Caract_Descripcion COLLATE DATABASE_DEFAULT AS Caract_Descripcion,
            ISNULL(CAST(Caract_Orden AS INT), 0) AS Caract_Orden,
            Caract_Fec_Actualizacion
        FROM {{ item['Servidor_Vinculado'] }}.{{ item['Base_Datos'] }}.dbo.Articulo_Caracteristicas WITH (NOLOCK)
        WHERE Emp_Id = {{ item['Empresa_Id_Original'] }}
        {% if is_incremental() -%}
        AND Caract_Fec_Actualizacion >= {{ last_date }}
        {%- endif %}
    {%- endfor -%}
)

SELECT
    *,
    Caract_Fec_Actualizacion AS Fecha_Actualizado,
    ABS(CAST(CAST(HASHBYTES('SHA2_256', CONCAT(Emp_Id, '-', Articulo_Id, '-', Caract_Id)) AS INT) AS BIGINT)) AS Hash_ArticuloCaractEmp,
    GETDATE() AS Fecha_Carga
FROM DatosBase
