
{%- set unique_key_list = ["ID"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="table",
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

  select --top 10 
  ISNULL(convert(nvarchar(20),A.ID), '') as ID, 
  MAX(A.Nombre) as Nombre, 
	MAX(A.Antiguedad) as Antiguedad, 
	MAX(A.Cargo) as Cargo, 
    ISNULL(MAX(B.Celular), 'No definido') as Celular, 
	isnull(MAX(B.eMail), 'Sin registro') as eMail, 
    'No' as Empleado_Confianza,
	GETDATE() as Fecha_Carga,
    GETDATE() AS Fecha_Actualizado
from(
		select A.Personal_Id, 
                        MAX(A.Nombre_Completo) AS Nombre, 
                        MAX(A.Identidad) as ID,
                        datediff(year, MIN(B.Fecha_Desde), getdate()) as Antiguedad,
                        MAX(A.Posicion_Nombre) as Cargo
		from [AN_FARINTER].[dbo].[AN_SAP_Personal_DatosActuales] A --{{ref('AN_SAP_Personal_DatosActuales')}} AS A 
		LEFT JOIN [DL_FARINTER].[dbo].[DL_SAP_Personal_Medidas] B ---{{ ref ( 'DL_SAP_Personal_Medidas') }}AS B
        on A.Personal_Id = B.Personal_Id  and B.Clase_Medida_Id in ('H1', 'H0', '01')
			where A.Area_Nomina_Id in ('K1', 'F1', 'F2', 'FU', 'K2', 'TF', 'BL', 'MT', 'MS') 
			AND A.Estado_Ocupacion_Id =3
			group by A.Personal_Id) as A
LEFT JOIN (
		select A.Personal_Id, A.Celular, isnull(B.eMail,'Sin registro') as eMail 
			from(
                 select A.PERNR as PersonaL_Id, 
						A.SUBTY, 
						A.USRTY, 
						max(A.USRID) as Celular
                 from {{ var('P_SAPPRD_LS') }}.[PRD].[prd].[PA0105] as A-- SAPPRD.PRD.prd.PA0105 as A
                 WHERE --PERNR = '00500217' AND 
                     '20250404' BETWEEN AEDTM AND ENDDA
                      and USRTY = 'AUCE'
                 group by A.PERNR, A.SUBTY, A.USRTY) as A
           LEFT JOIN (
					select A.PERNR as PersonaL_Id, 
							A.SUBTY, 
							A.USRTY, 
							min(A.USRID) as eMail
                    from {{ var('P_SAPPRD_LS') }}.[PRD].[prd].[PA0105] as A--SAPPRD.PRD.prd.PA0105 as A
                    WHERE --PERNR = '00500217' AND 
						'20250404' BETWEEN AEDTM AND ENDDA
                               and USRTY = 'AUEP'
                                group by A.PERNR, A.SUBTY, A.USRTY) AS B
                                           on A.Personal_Id = B.Personal_Id) AS B
                on A.Personal_Id = B.PersonaL_Id
GROUP BY A.ID
   