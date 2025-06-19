{% set unique_key_list = ["Usuario_Id"] %}
{{ 
    config(
        as_columnstore=true,
        tags=["periodo/diario"],
        materialized="incremental",
        incremental_strategy="farinter_merge",
        unique_key=unique_key_list,
        merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
        merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
        post_hook=[
            "{{ dwh_farinter_remove_incremental_temp_table() }}",
            "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}"
        	"{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizacion']) }}",
      		"{{ dwh_farinter_create_dummy_data(unique_key=" ~ unique_key_list | tojson ~ ", is_incremental=0) }}"
        ]
    ) 
}}
{% if is_incremental() %}
	{% set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -7, max(Fecha_Actualizacion)), 112), '19000101')  from  """ ~ this, relation_not_found_value='00000000'|string)|string %}
{% else %}
	{% set last_date = '00000000'|string %}
{% endif %}

SELECT 
    U.[BNAME] COLLATE DATABASE_DEFAULT AS Usuario_Id,
    dbo.fnc_ProperCase(U.[NAME_FIRST]) COLLATE DATABASE_DEFAULT AS Nombre,
    dbo.fnc_ProperCase(U.[NAME_LAST]) COLLATE DATABASE_DEFAULT AS Apellido, 
    dbo.fnc_ProperCase(U.[NAME_TEXTC]) COLLATE DATABASE_DEFAULT AS Nombre_Completo,
    U.[TEL_EXTENS] COLLATE DATABASE_DEFAULT AS Telefono,
    U.[KOSTL] COLLATE DATABASE_DEFAULT AS CentroCosto_Id,
    dbo.fnc_ProperCase(U.[BUILDING]) COLLATE DATABASE_DEFAULT AS Edificio,
    U.[ROOMNUMBER] COLLATE DATABASE_DEFAULT AS Numero_Habitacion,
    dbo.fnc_ProperCase(U.[DEPARTMENT]) COLLATE DATABASE_DEFAULT AS Departamento,
    U.[INHOUSE_ML] COLLATE DATABASE_DEFAULT AS Correo_Interno,
    U.[NAME1] COLLATE DATABASE_DEFAULT AS Nombre_1,
    U.[CITY1] COLLATE DATABASE_DEFAULT AS Ciudad,
    U.[POST_CODE1] COLLATE DATABASE_DEFAULT AS Codigo_Postal,
	ISNULL(UV.VKGRP,'X') COLLATE DATABASE_DEFAULT AS GrupoVendedores_Id,
    U.Fecha_Actualizado AS Fecha_Actualizacion
FROM [DL_FARINTER].[dbo].[DL_SAP_USER_ADDR] U --{{ ref('DL_SAP_USER_ADDR') }} 
LEFT JOIN [DL_FARINTER].[dbo].[DL_SAP_ZFRM_EPT_0001] UV
ON U.[BNAME] = UV.[BNAME] 
{% if is_incremental() %}
WHERE U.Fecha_Actualizado > '{{last_date}}'
	OR UV.Fecha_Actualizado > '{{last_date}}'
{% endif %}
