{% set unique_key_list = ["MATNR", "SPRAS"] %}

{{ 
    config(
        as_columnstore=False,
        tags=["periodo/diario", "periodo/por_hora","sap_modulo/mm"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        on_schema_change="fail",
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        pre_hook=[
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        ],
        post_hook=[
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=true, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
        ]
    ) 
}}

{% if is_incremental() %}
    {% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -3, max(Fecha_Actualizado)), 112), '19000101')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
    {% set last_date = '00000000'|string %}
{% endif %}

SELECT
    ISNULL(CAST(A.MATNR COLLATE DATABASE_DEFAULT AS VARCHAR(18)),'') AS MATNR
    , ISNULL(CAST(A.SPRAS COLLATE DATABASE_DEFAULT AS VARCHAR(2)),'') AS SPRAS
    , ISNULL(CAST(A.MAKTX COLLATE DATABASE_DEFAULT AS VARCHAR(50)),'') AS MAKTX
    , ISNULL(CAST(A.MAKTG COLLATE DATABASE_DEFAULT AS VARCHAR(50)),'') AS MAKTG
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS Fecha_Carga
    , ISNULL(CAST(GETDATE() AS DATETIME),'19000101') AS Fecha_Actualizado
FROM {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'MAKT')}} A
INNER JOIN {{ var('P_SAPPRD_LS') }}.{{ source('SAPPRD', 'MARA')}} M 
    ON A.MANDT = M.MANDT 
    AND A.MATNR = M.MATNR
WHERE A.MANDT = '300'
{% if is_incremental() and modules.datetime.datetime.today().isoweekday() != 7  %}
    AND (M.LAEDA >= '{{last_date}}'
    OR M.ERSDA >= '{{last_date}}'
    )
{% endif %}
