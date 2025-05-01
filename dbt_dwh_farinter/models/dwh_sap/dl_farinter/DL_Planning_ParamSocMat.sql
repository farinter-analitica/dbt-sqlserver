{%- set unique_key_list = ["Sociedad_Id", "Articulo_Id"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "automation/periodo_mensual_inicio", "periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Gpo_Obs_Id','Sociedad_Id','Articulo_Id']) }}",
            "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Gpo_Plan_Id','Sociedad_Id','Articulo_Id']) }}"
        ]
		
) }}

-- Modelo incremental para la tabla DL_Planning_ParamSocMat de DL_FARINTER.
-- Respeta el esquema, claves y tipos del modelo fuente.

WITH TempMateriales AS (
    SELECT
        Material_Id,
        Sub_Gpo_Id
    FROM {{ ref('DL_Planning_SAP_Materiales') }}
),
TempParamSocGpo AS (
    SELECT
        GP.Sociedad_Id,
        G.Gpo_Art_Id,
        G.Sub_Gpo_Id,
        GP.Gpo_Plan_Id,
        GP.Gpo_Plan_Nombre,
        G.Gpo_Abc_Id,
        GOBS.Gpo_Obs_Id,
        GOBS.Gpo_Obs_Nombre,
        GOBS.Gpo_Obs_Nombre_Corto
    FROM {{ ref('DL_Planning_SAP_ParamSocGpo') }} G
    INNER JOIN {{ ref('DL_Planning_SAP_GposPlan') }} GP
        ON G.Sociedad_Id = GP.Sociedad_Id
        AND G.Gpo_Plan_Id = GP.Gpo_Plan_Id
    INNER JOIN {{ ref('DL_Planning_SAP_GposObs') }} GOBS
        ON G.Gpo_Obs_Id = GOBS.Gpo_Obs_Id
    WHERE G.Planificado = 'S'
)

SELECT
    -- NOT NULL
    ISNULL(CAST(S.Sociedad_Id AS nvarchar(4)), '') AS Sociedad_Id,
    ISNULL(CAST(A.Articulo_Id AS nvarchar(50)), '') AS Articulo_Id,
    ISNULL(CAST(C.Casa_Id AS nvarchar(50)), '') AS Casa_Id,
    ISNULL(C.Casa_Nombre, '') AS Casa_Nombre,
    ISNULL(CAST(G.Gpo_Plan_Id AS varchar(10)), '') AS Gpo_Plan_Id,
    ISNULL(G.Gpo_Plan_Nombre, '') AS Gpo_Plan_Nombre,
    ISNULL(CAST(ISNULL(P.Sub_Gpo_Id, 'ST') AS varchar(5)), '') AS Sub_Gpo_Id,
    -- NULLABLES
    CAST(G.Gpo_ABC_Id AS varchar(5)) AS Gpo_ABC_Id,
    ISNULL(CAST(G.Gpo_Obs_Id AS varchar(5)), '') AS Gpo_Obs_Id,
    ISNULL(G.Gpo_Obs_Nombre, '') AS Gpo_Obs_Nombre,
    CAST(G.Gpo_Obs_Nombre_Corto AS varchar(30)) AS Gpo_Obs_Nombre_Corto,
    GETDATE() AS Fecha_Actualizado
FROM (
    SELECT DISTINCT Sociedad_Id, Articulo_Id
    FROM {{ ref('BI_Hecho_Inventarios_SAP') }}
) I
INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Sociedad_SAP') }} S
    ON I.Sociedad_Id = S.Sociedad_Id
INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Articulo_SAP') }} A
    ON I.Articulo_Id = A.Articulo_Id
INNER JOIN {{ source('BI_FARINTER', 'BI_Dim_Casa_SAP') }} C
    ON A.Casa_Id = C.Casa_Id
LEFT JOIN TempMateriales P
    ON A.Material_Id = P.Material_Id
INNER JOIN TempParamSocGpo G
    ON S.Sociedad_Id = G.Sociedad_Id
    AND C.Casa_Id = G.Gpo_Art_Id
    AND ISNULL(P.Sub_Gpo_Id, 'ST') = G.Sub_Gpo_Id

