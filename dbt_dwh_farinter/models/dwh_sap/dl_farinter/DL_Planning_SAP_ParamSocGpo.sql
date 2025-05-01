{%- set unique_key_list = ["Sociedad_Id", "Gpo_Art_Id", "Sub_Gpo_Id"] -%}

{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		on_schema_change="append_new_columns",
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
      "{{ dwh_farinter_remove_incremental_temp_table() }}",
      "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
		]
		
) }}

SELECT 
    ISNULL(sociedad_id COLLATE DATABASE_DEFAULT, '') AS Sociedad_Id,
    ISNULL(gpo_art_id COLLATE DATABASE_DEFAULT, '') AS Gpo_Art_Id,
    ISNULL(sub_gpo_id COLLATE DATABASE_DEFAULT, '') AS Sub_Gpo_Id,
    ISNULL(gpo_art_nombre_ref COLLATE DATABASE_DEFAULT, '') AS Gpo_Art_Nombre_Ref,
    ISNULL(gpo_plan_id COLLATE DATABASE_DEFAULT, '') AS Gpo_Plan_Id,
    ISNULL(resp_compras COLLATE DATABASE_DEFAULT, '') AS Resp_Compras,
    ISNULL(asist_compras COLLATE DATABASE_DEFAULT, '') AS Asist_Compras,
    ISNULL(planificado COLLATE DATABASE_DEFAULT, 'S') AS Planificado, -- Default 'S' from DDL
    ISNULL(gpo_plan_ent_id COLLATE DATABASE_DEFAULT, '') AS Gpo_Plan_Ent_Id,
    ISNULL(plan_centro COLLATE DATABASE_DEFAULT, 'N') AS Plan_Centro, -- Default 'N' from DDL
    ISNULL(excluir_gpos_cliente_id COLLATE DATABASE_DEFAULT, '') AS Excluir_Gpos_Cliente_Id,
    ISNULL(incluir_sociedades_id COLLATE DATABASE_DEFAULT, '') AS Incluir_Sociedades_Id,
    ISNULL(nutricional COLLATE DATABASE_DEFAULT, 'N') AS Nutricional, -- Assuming 'N' as default if NULL
    ISNULL(marca_propia COLLATE DATABASE_DEFAULT, 'N') AS Marca_Propia, -- Assuming 'N' as default if NULL
    ISNULL(exist_farmacia COLLATE DATABASE_DEFAULT, 'N') AS Exist_Farmacia, -- Default 'N' from DDL
    ISNULL(transito_sinventa COLLATE DATABASE_DEFAULT, 'N') AS Transito_Sinventa, -- Default 'N' from DDL
    ISNULL(gpo_abc_id COLLATE DATABASE_DEFAULT, '') AS Gpo_Abc_Id,
    ISNULL(devolutivo, 0.0) AS Devolutivo,
    ISNULL(dia_limite, 0) AS Dia_Limite,
    ISNULL(gpo_obs_id COLLATE DATABASE_DEFAULT, 'SD') AS Gpo_Obs_Id, -- Default 'SD' from DDL
    ISNULL(proceso_id COLLATE DATABASE_DEFAULT, '') AS Proceso_Id,
    ISNULL(tiempo_estimado, 0.0) AS Tiempo_Estimado,
    ISNULL(piso_a, 0.0) AS Piso_A,
    ISNULL(piso_b, 0.0) AS Piso_B,
    ISNULL(piso_c, 0.0) AS Piso_C,
    ISNULL(deficit_kiel_max, 0.0) AS Deficit_Kiel_Max,
    ISNULL(tiempo_transito, 0.0) AS Tiempo_Transito,
    ISNULL(procedencia_id COLLATE DATABASE_DEFAULT, '') AS Procedencia_Id,
    ISNULL(gpo_nsp_id COLLATE DATABASE_DEFAULT, 'SD') AS Gpo_Nsp_Id, -- Default 'SD' from DDL
    ISNULL(periodo, 0.0) AS Periodo,
    ISNULL(dia_planificado, 0) AS Dia_Planificado,
    ISNULL(distribucion_ref COLLATE DATABASE_DEFAULT, '') AS Distribucion_Ref,
    ISNULL(notas COLLATE DATABASE_DEFAULT, '') AS Notas,
    ISNULL(centro_id_aprov COLLATE DATABASE_DEFAULT, '') AS Centro_Id_Aprov,
    ISNULL(planificado_niv COLLATE DATABASE_DEFAULT, 'S') AS Planificado_Niv, -- Default 'S' from DDL
    ISNULL(stock_dur_rest_min_d, 0.0) AS Stock_Dur_Rest_Min_D, -- Default 0 from DDL
    registro_id AS Registro_Id, -- Keep original identity value
    ISNULL(prov_id COLLATE DATABASE_DEFAULT, '') AS Prov_Id,
    GETDATE() AS Fecha_Actualizado -- Add timestamp for dbt processing
FROM {{ var('P_SQLLDSUBS_LS') }}.{{ source('PLANNING_DB', 'ParamSocGpo') }}
