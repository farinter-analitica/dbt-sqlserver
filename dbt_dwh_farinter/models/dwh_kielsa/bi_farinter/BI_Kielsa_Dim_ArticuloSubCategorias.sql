{%- set unique_key_list = [
    "Emp_Id",
    "CategoriaArt_Id",
    "SubCategoria1Art_Id",
    "SubCategoria2Art_Id",
    "SubCategoria3Art_Id",
    "SubCategoria4Art_Id"
] -%}

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
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -1, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this,
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %}

WITH SC AS (
    SELECT
        SC1.Emp_Id,
        SC1.CategoriaArt_Id,
        SC1.SubCategoria1Art_Id,
        SC1.SubCategoria1Art_Nombre,
        SC2.SubCategoria2Art_Id,
        SC2.SubCategoria2Art_Nombre,
        SC3.SubCategoria3Art_Id,
        SC3.SubCategoria3Art_Nombre,
        SC4.SubCategoria4Art_Id,
        SC4.SubCategoria4Art_Nombre,
        COALESCE(SC4.Hash_CatSubCat1_2_3_4Emp, SC3.Hash_CatSubCat1_2_3Emp, SC2.Hash_CatSubCat1_2Emp, SC1.Hash_CatSubCat1Emp) AS Hash_SubCatsEmp,
        -- Timestamps: take the latest across available levels
        COALESCE(SC4.Fecha_Actualizado, SC3.Fecha_Actualizado, SC2.Fecha_Actualizado, SC1.Fecha_Actualizado) AS Fecha_Actualizado,
        COALESCE(SC4.Fecha_Carga, SC3.Fecha_Carga, SC2.Fecha_Carga, SC1.Fecha_Carga) AS Fecha_Carga
    FROM {{ ref('DL_Kielsa_SubCategoria1_Articulo') }} AS SC1
    LEFT JOIN {{ ref('DL_Kielsa_SubCategoria2_Articulo') }} AS SC2
        ON
            SC1.CategoriaArt_Id = SC2.CategoriaArt_Id
            AND SC1.SubCategoria1Art_Id = SC2.SubCategoria1Art_Id
            AND SC1.Emp_Id = SC2.Emp_Id
    LEFT JOIN {{ ref('DL_Kielsa_SubCategoria3_Articulo') }} AS SC3
        ON
            SC2.CategoriaArt_Id = SC3.CategoriaArt_Id
            AND SC2.SubCategoria1Art_Id = SC3.SubCategoria1Art_Id
            AND SC2.SubCategoria2Art_Id = SC3.SubCategoria2Art_Id
            AND SC2.Emp_Id = SC3.Emp_Id
    LEFT JOIN {{ ref('DL_Kielsa_SubCategoria4_Articulo') }} AS SC4
        ON
            SC3.CategoriaArt_Id = SC4.CategoriaArt_Id
            AND SC3.SubCategoria1Art_Id = SC4.SubCategoria1Art_Id
            AND SC3.SubCategoria2Art_Id = SC4.SubCategoria2Art_Id
            AND SC3.SubCategoria3Art_Id = SC4.SubCategoria3Art_Id
            AND SC3.Emp_Id = SC4.Emp_Id
    {%- if is_incremental() %}
        WHERE
            SC1.Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
            OR SC2.Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
            OR SC3.Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
            OR SC4.Fecha_Actualizado >= CAST('{{ last_date }}' AS datetime)
    {%- endif %}
)

SELECT
    ISNULL(Emp_Id, 0) AS Emp_Id,
    ISNULL(CategoriaArt_Id, 0) AS CategoriaArt_Id,
    ISNULL(SubCategoria1Art_Id, 0) AS SubCategoria1Art_Id,
    ISNULL(SubCategoria1Art_Nombre, 'N.D.') AS SubCategoria1Art_Nombre,
    ISNULL(SubCategoria2Art_Id, 0) AS SubCategoria2Art_Id,
    ISNULL(SubCategoria2Art_Nombre, 'N.D.') AS SubCategoria2Art_Nombre,
    ISNULL(SubCategoria3Art_Id, 0) AS SubCategoria3Art_Id,
    ISNULL(SubCategoria3Art_Nombre, 'N.D.') AS SubCategoria3Art_Nombre,
    ISNULL(SubCategoria4Art_Id, 0) AS SubCategoria4Art_Id,
    ISNULL(SubCategoria4Art_Nombre, 'N.D.') AS SubCategoria4Art_Nombre,
    {{ dwh_farinter_concat_key_columns(
        columns=['Emp_Id','CategoriaArt_Id','SubCategoria1Art_Id','SubCategoria2Art_Id','SubCategoria3Art_Id','SubCategoria4Art_Id'],
        input_length=120) }}
        AS [EmpCatSubCategoria1_2_3_4Art_Id],
    Hash_SubCatsEmp,
    Fecha_Carga,
    Fecha_Actualizado
FROM SC;
