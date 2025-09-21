{% set unique_key_list = ["Factura_Id", "Articulo_Id_Original"] -%}

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
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Articulo_Id', 'Organizacion_Id']) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Vendedor_Id', 'Cliente_Id']) }}",
        ]
    ) 
}}

{%- if is_incremental() %}
    {%- set v_fecha_desde = run_single_value_query_on_relation_and_return(
        query="""select ISNULL(CONVERT(VARCHAR,DATEADD(YEAR, -1, GETDATE()), 112), '19000101') as fecha_a from  """ ~ this, 
        relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set v_fecha_desde = '19000101' %}
{%- endif %}
{%- set v_aniomes_desde = v_fecha_desde[:6] %}
{%- set v_anio_desde = v_fecha_desde[:4] %}

WITH staging AS (
    SELECT
        A.Factura_Id,
        K.Articulo_Id AS Articulo_Id_Original,
        MAX(D.Sociedad_Id) AS Organizacion_Id,
        MAX(D.Sociedad_Nombre) AS Organizacion_Nombre,
        MAX(E.Centro_Id) AS Centro_Id,
        MAX(E.Centro_Nombre) AS Centro_Nombre,
        MAX(F.Zona_Id) AS Zona_Id,
        MAX(F.Zona_Nombre) AS Zona_Nombre,
        MAX(G.Vendedor_Id) AS Vendedor_Id,
        MAX(G.Vendedor_Nombre) AS Vendedor_Nombre,
        MAX(G.C2) AS TipoVendedor,
        MAX(H.GrupoCliente_Id) AS TipoCliente_Id,
        MAX(H.GrupoCliente_Nombre) AS TipoCliente_Nombre,
        MAX(I.Cliente_Id) AS Cliente_Id,
        MAX(I.Cliente_Nombre) AS Cliente_Nombre,
        MAX(J.Casa_Id) AS Casa_Id,
        MAX(J.Casa_Nombre) AS Casa_Nombre,
        MAX(COALESCE(LN.Articulo_Nuevo_Id, K.Articulo_Id)) AS Articulo_Id,
        MAX(COALESCE(LNA.Articulo_Nombre, K.Articulo_Nombre)) AS Articulo_Nombre,
        MAX(K.Marca_Id) AS Marca,
        MAX(K.Marca_Nombre) AS Marca_Nombre,
        MAX(B.GrupoMaterial_Nombre) AS ClasificacionArt,
        MAX(A.ClaseFactura_Id) AS TipoFactura_Id,
        MAX(M.TipoFactura_Nombre) AS TipoFactura_Nombre,
        MAX(A.CanalDistribucion_Id) AS TipoArt2_Id,
        MAX(L.CanalDist_Nombre) AS TipoArt2_Nombre,
        MAX(A.CanalDistribucion_Id) AS CanalDistribucion_Id,
        MAX(L.CanalDist_Nombre) AS CanalDist_Nombre,
        MAX(A.Fecha_Id) AS Fecha_Id,
        MAX(A.Dia_Calendario) AS Dia_Id,
        MAX(A.Anio_Calendario) AS Anio_Calendario,
        MAX(A.AnioMesCreado_Id) AS AnioMesCreado_Id,
        SUM(A.Cantidad_SKU) AS Cantidad,
        SUM(A.Venta) AS Venta_Bruta,
        SUM(A.Descuento) AS Descuento,
        SUM(A.Costo) AS Costo,
        GETDATE() AS Fecha_Actualizado,
        GETDATE() AS Fecha_Carga
    FROM {{ ref('BI_SAP_Mixto_Facturas') }} A
    INNER JOIN {{ source('BI_FARINTER','BI_SAP_Dim_GrupoMaterial') }} B
        ON A.Grupo_Materiales_Id = B.GrupoMaterial_Id
    INNER JOIN {{ source('BI_FARINTER','BI_Dim_Sociedad_SAP') }} D
        ON A.Sociedad_Id = D.Sociedad_Id
    INNER JOIN {{ source('BI_FARINTER','BI_Dim_Centro_SAP') }} E
        ON A.Centro_Id = E.Centro_Id
    INNER JOIN {{ source('BI_FARINTER','BI_Dim_Zona_SAP') }} F
        ON A.Zona_Id = F.Zona_Id
    INNER JOIN {{ source('BI_FARINTER','BI_Dim_Vendedor_SAP') }} G
        ON A.Vendedor_Id = G.Vendedor_Id
    INNER JOIN {{ source('BI_FARINTER','BI_SAP_Dim_GrupoCliente') }} H
        ON A.GrupoCliente_Id = H.GrupoCliente_Id
    INNER JOIN {{ source('BI_FARINTER','BI_Dim_Cliente_SAP') }} I
        ON A.Cliente_Id = I.Cliente_Id
    INNER JOIN {{ source('BI_FARINTER','BI_Dim_Casa_SAP') }} J
        ON A.Casa_Id = J.Casa_Id
    INNER JOIN {{ source('BI_FARINTER','BI_Dim_Articulo_SAP') }} K
        ON A.Articulo_Id = K.Articulo_Id
    INNER JOIN {{ source('BI_FARINTER','BI_SAP_Dim_CanalDist') }} L
        ON A.CanalDistribucion_Id = L.CanalDist_Id
    INNER JOIN {{ source('BI_FARINTER','BI_Dim_TipoFactura_SAP') }} M
        ON A.ClaseFactura_Id = M.TipoFactura_Id
    LEFT JOIN {{ source('DL_FARINTER','DL_Planning_ListaNegra') }} LN
        ON
            A.Articulo_Id = LN.Articulo_Id
            AND LN.fecha_desde <= CAST(GETDATE() AS DATE) --Activo actualmente
            AND LN.fecha_hasta >= CAST(GETDATE() AS DATE)
            AND LN.condicion_id = 'CP' --Solo cambios de presentación
    LEFT JOIN {{ source('BI_FARINTER','BI_Dim_Articulo_SAP') }} LNA
        ON LN.Articulo_Nuevo_Id = LNA.Articulo_Id
    WHERE
        A.Sociedad_Id IN ('1200', '1300', '1301', '1700', '2500')
        {% if is_incremental() %}
            AND A.Fecha_Id >= '{{ v_fecha_desde }}'
            AND A.Anio_Calendario >= {{ v_anio_desde }}
            AND A.AnioMesCreado_Id >= {{ v_aniomes_desde }}
        {% else %}
        AND A.Anio_Calendario >= YEAR(GETDATE()) - 4
        AND A.AnioMesCreado_Id >= (YEAR(GETDATE()) - 4) * 100 + MONTH(GETDATE())
        {% endif %}
    GROUP BY
        A.Factura_Id,
        K.Articulo_Id
)

SELECT *
FROM staging
