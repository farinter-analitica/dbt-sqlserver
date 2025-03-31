{% set unique_key_list = ["Factura_Id"] %}
{{ 
    config(
        as_columnstore=true,
        tags=["periodo/por_hora"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha']) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Cliente_Id']) }}",
            "{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
    ) 
}}

{% if is_incremental() %}
    {% set last_date = run_single_value_query_on_relation_and_return(
            query="""select ISNULL(CONVERT(VARCHAR, MAX(Fecha), 112), '19000101') from """ ~ this,
            relation_not_found_value='19000101'|string)|string %}
    {% set v_fecha_corte = (modules.datetime.datetime.now()-modules.datetime.timedelta(days=7)).strftime('%Y%m%d') %}
    {% set last_date = last_date if last_date<=v_fecha_corte else v_fecha_corte %}
{% else %}
    {% set last_date = '19000101'|string %}
{% endif %}

-- First source: Direct from SAP tables
WITH origen_sap AS 
(
    SELECT 
        ISNULL(CAST(A.VBELN AS NVARCHAR(50)),'') COLLATE DATABASE_DEFAULT AS Factura_Id,
        CAST(A.FKDAT AS DATE) AS Fecha,
        CAST(A.BUKRS AS NVARCHAR(50)) COLLATE DATABASE_DEFAULT AS Sociedad_Id,
        CAST(A.BZIRK AS NVARCHAR(50)) COLLATE DATABASE_DEFAULT AS Zona_Id,
        CAST(A.FKART AS NVARCHAR(50)) COLLATE DATABASE_DEFAULT AS TipoFactura_Id,
        CAST(A.KUNRG AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Cliente_Id,
        ISNULL(CAST(B.STCD1 AS NVARCHAR(100)), 'X') COLLATE DATABASE_DEFAULT AS Cliente_Identidad, 
        ISNULL(CAST(D.NAME1 AS NVARCHAR(100)), 'No aplica') COLLATE DATABASE_DEFAULT AS Cliente_Nombre, 
        ISNULL(CAST(D.TEL_NUMBER AS NVARCHAR(100)), 'No aplica') COLLATE DATABASE_DEFAULT AS Telefono, 
        ISNULL(CAST(D.REGION AS NVARCHAR(100)), 'No aplica') COLLATE DATABASE_DEFAULT AS Region_Id, 
        ISNULL(CAST(D.CITY1 AS NVARCHAR(100)), 'No aplica') COLLATE DATABASE_DEFAULT AS Ciudad,
        ISNULL(CAST(E.TEL_NUMBER AS NVARCHAR(100)), 'No aplica') COLLATE DATABASE_DEFAULT AS Celular1,
        ISNULL(MAX(CAST(F.TEL_NUMBER AS NVARCHAR(100))), 'No aplica') COLLATE DATABASE_DEFAULT AS Celular2,
        ISNULL(CAST(G.REMARK AS NVARCHAR(100)), 'No aplica') COLLATE DATABASE_DEFAULT AS Comentario
    FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'VBRK') }} A WITH (NOLOCK)
    LEFT JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'VBPA3') }} B WITH (NOLOCK)
        ON A.MANDT = B.MANDT AND A.VBELN = B.VBELN AND B.PARVW = 'AG' -- DATOS DEL PAGADOR
    LEFT JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'VBPA') }} C WITH (NOLOCK)
        ON A.MANDT = C.MANDT AND A.VBELN = C.VBELN AND C.PARVW = 'AG' -- DATOS DEL PAGADOR
    LEFT JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'ADRC') }} D WITH (NOLOCK)
        ON A.MANDT = D.CLIENT AND C.ADRNR = D.ADDRNUMBER
    LEFT JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'ADR2') }} E WITH (NOLOCK)
        ON A.MANDT = E.CLIENT AND C.ADRNR = E.ADDRNUMBER AND E.R3_USER = 2
    LEFT JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'ADR2') }} F WITH (NOLOCK)
        ON A.MANDT = F.CLIENT AND C.ADRNR = F.ADDRNUMBER AND F.R3_USER = 3
    LEFT JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'ADRCT') }} G WITH (NOLOCK)
        ON A.MANDT = G.CLIENT AND C.ADRNR = G.ADDRNUMBER AND G.LANGU = 'S'
    WHERE A.MANDT = '300' 
    AND A.FKDAT> '{{ last_date }}'

    GROUP BY
    A.VBELN, A.FKDAT, A.BUKRS, A.BZIRK, A.FKART, A.KUNRG, B.STCD1, D.NAME1,
    D.TEL_NUMBER, D.REGION, D.CITY1, E.TEL_NUMBER, G.REMARK
)

SELECT *,
    ISNULL(CAST(GETDATE() AS DATETIME), '19000101') AS Fecha_Actualizado 
FROM origen_sap


UNION ALL
-- Second source: From existing dimension tables
SELECT
    ISNULL(CAST(V.Factura_Id AS NVARCHAR(50)),'') COLLATE DATABASE_DEFAULT AS Factura_Id,
    CAST(V.Fecha_Id AS DATE) Fecha,
    CAST(V.Sociedad_Id AS NVARCHAR(50)) COLLATE DATABASE_DEFAULT AS Sociedad_Id,
    CAST(V.Zona_Id AS NVARCHAR(50)) COLLATE DATABASE_DEFAULT AS Zona_Id,
    CAST(V.ClaseFactura_Id AS NVARCHAR(50)) COLLATE DATABASE_DEFAULT AS TipoFactura_Id,
    CAST(V.Cliente_Id AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Cliente_Id,
    CAST(V.Referencia_Id AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Cliente_Identidad,
    CAST(C.Cliente_Nombre AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Cliente_Nombre,
    CAST(C.Telefono AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Telefono,
    CAST(V.Region_Id AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Region_Id,
    CAST('' AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Ciudad,
    CAST('No aplica' AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Celular1,
    CAST('No aplica' AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Celular2,
    CAST('No aplica' AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT AS Comentario,
    ISNULL(CAST(GETDATE() AS DATETIME), '19000101') AS Fecha_Actualizado
FROM {{ ref('BI_SAP_Dim_Facturas') }} V
INNER JOIN {{ source('BI_FARINTER','BI_Dim_Cliente_SAP') }} C
    ON C.Cliente_Id = V.Cliente_Id
WHERE V.FechaCreado_Id > CAST('{{ last_date }}' AS DATE)
AND V.Factura_Id NOT IN (SELECT Factura_Id FROM {{ this }})
AND V.Factura_Id NOT IN (SELECT Factura_Id FROM origen_sap)

