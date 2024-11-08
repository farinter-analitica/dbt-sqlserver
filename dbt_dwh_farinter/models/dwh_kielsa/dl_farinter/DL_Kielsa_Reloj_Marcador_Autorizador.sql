{% set unique_key_list = ["Pais","Tienda"] %}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario", "periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

SELECT --TOP (1000) 
    ISNULL(RMA.[Pais] COLLATE DATABASE_DEFAULT,'') AS [Pais]
    ,COALESCE(P.[Pais_ISO2],P2.[Pais_ISO2],'X') AS [Pais_ISO2]
    ,COALESCE(P.[Pais_Id],P2.[Pais_Id],0) AS [Pais_Id]
      ,ISNULL(RMA.[Tienda] COLLATE DATABASE_DEFAULT,'') AS [Tienda]
      ,RMA.[Autorizador] COLLATE DATABASE_DEFAULT AS [Autorizador]
      ,RMA.[Movil] COLLATE DATABASE_DEFAULT AS [Movil]
      ,RMA.[Autorizador_JO] COLLATE DATABASE_DEFAULT AS [Autorizador_JO]
      ,RMA.[Movil_JO] COLLATE DATABASE_DEFAULT AS [Movil_JO]
  FROM {{ var('P_SQLLDSUBS_LS') }}.{{ source('SITEPLUS', 'Kielsa_Reloj_Marcador_Autorizador') }} RMA
LEFT JOIN {{ source('BI_FARINTER','BI_Dim_Pais') }} P 
ON LOWER(RMA.Pais) = LOWER(P.Pais_Nombre) COLLATE DATABASE_DEFAULT
LEFT JOIN {{ source('BI_FARINTER','BI_Dim_Pais') }} P2
ON P2.Pais_Id IS NULL
AND LOWER(RMA.Pais) LIKE '%'+LOWER(P2.Pais_ISO2)+'%' COLLATE DATABASE_DEFAULT