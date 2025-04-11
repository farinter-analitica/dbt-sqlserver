{% set unique_key_list = ["Fecha_Id", "ArticuloPadre_Id", "Sucursal_Id", "Emp_Id", "Articulo_Id"] %}
{% set current_month_day = modules.datetime.datetime.now().day %}
{%- if is_incremental() %}
    {%- set v_min_date = run_single_value_query_on_relation_and_return(query=
        """select ISNULL(CONVERT(VARCHAR,min(Fecha_Id), 112), '19000101')  
        from  """ ~ this, relation_not_found_value='19000101'|string)|string
        -%}
{% endif %}

{%- set v_limit_min_date= (modules.datetime.datetime.now() - modules.datetime.timedelta(days=120)).replace(day=1).strftime("%Y%m%d") %}

{{ 
    config(
        as_columnstore=true,
        materialized="incremental",
        tags=["periodo/diario"],
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
		full_refresh=true if not is_incremental() or (current_month_day > 10 and current_month_day < 20 and v_min_date < v_limit_min_date) else false,
        on_schema_change="fail",
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
    ) 
}}
{%- if is_incremental() %}
	{%- set v_last_date = run_single_value_query_on_relation_and_return(query=
        """select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Id)), 112), '19000101')  from  """ ~ this, 
            relation_not_found_value='19000101'|string)|string 
    -%}
{%- else %}
	{%- set v_last_date = '19000101' %}
{%- endif %}


WITH source_data AS (
SELECT
        V.Emp_Id,
        V.Emp_Id AS Pais_Id,
        V.Suc_Id Sucursal_Id, -- Added based on requirement
        V.Articulo_Id, -- Added based on requirement
        A.Articulo_Codigo_Padre ArticuloPadre_Id,
        V.Factura_Fecha AS Fecha_Id,
        YEAR(V.Factura_Fecha) Anio_Id,
        MONTH(V.Factura_Fecha) Mes_Id,
        SUM(V.Cantidad_Padre) AS Cantidad_Padre
        --V.Factura_Id
        FROM {{ ref('BI_Kielsa_Hecho_FacturaPosicion') }} V
        -- Joins are not strictly necessary if the required columns (Sucursal_Id, Articulo_Id) exist in the source fact table
        -- INNER JOIN {{ ref('BI_Kielsa_Dim_Sucursal') }} S
        -- ON V.Suc_Id = S.Suc_Id -- Assuming V.Sucursal_Id is the correct FK
        -- AND V.Pais_Id    = S.Emp_Id
        INNER JOIN {{ ref('BI_Kielsa_Dim_Articulo') }} A
        ON V.Pais_Id    = A.Emp_Id
        AND V.Articulo_Id = A.Articulo_Id -- Assuming V.Articulo_Id is the correct FK
        {% if is_incremental() %}
        WHERE V.Factura_Fecha >= '{{v_last_date}}'
        {% else %}
        WHERE V.Factura_Fecha >= '{{v_limit_min_date}}'
        {% endif %}
        GROUP BY
            V.Emp_Id,
            V.Suc_Id,
            V.Articulo_Id,
            A.Articulo_Codigo_Padre,
            V.Factura_Fecha,
            YEAR(V.Factura_Fecha),
            MONTH(V.Factura_Fecha)
)
SELECT
        Emp_Id,
        Pais_Id,
        Sucursal_Id,
        Articulo_Id,
        ArticuloPadre_Id,
        Fecha_Id,
        Anio_Id,
        Mes_Id,
        Cantidad_Padre
        --Factura_Id
FROM source_data
