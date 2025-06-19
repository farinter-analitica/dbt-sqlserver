{% set unique_key_list = ["Personal_Id","Subtipo_Id","Objeto_Id","Indicador_Bloqueo","Numero_Clave"] %}
{{ 
    config(
		tags=["periodo/diario"],
		materialized="view",
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        ]
	) 
}}


SELECT --TOP (10) 
	DP.[Personal_Id]
      ,DP.[Subtipo_Id]
      ,DP.[Objeto_Id]
      ,DP.[Indicador_Bloqueo]
      ,DP.[Fecha_Hasta]
      ,DP.[Fecha_Desde]
      ,DP.[Numero_Clave]
      ,DP.[Fecha_Modificacion]
      ,DP.[Usuario_Modificacion]
      ,DP.[Iniciales]
      ,DP.[Apellido]
      ,DP.[Apellido_Soltera]
      ,DP.[Segundo_Apellido]
      ,DP.[Nombre_Pila]
      ,DP.[Nombre_Completo]
      ,DP.[Titulo]
      ,DP.[Tratamiento_Id]
      ,DP.[Sexo_Id]
      ,DP.[Fecha_Nacimiento]
      ,DP.[Pais_Nacimiento_Id]
      ,DP.[Lugar_Nacimiento]
      ,DP.[Pais_Nacionalidad_Id]
      ,DP.[Idioma_Preferido]
      ,DP.[Estado_Civil_Id]
      ,DP.[Cantidad_Hijos]
      ,DP.[Anio_Nacimiento_Id]
      ,DP.[Mes_Nacimiento_Id]
      ,DP.[Dia_Nacimiento_Id]
      ,DP.[Periodo_Hashkey]
      ,DP.[DatosPersonales_Hashkey]
      ,ORG.[Division_Personal_Id]
      ,ORG.[Division_Personal_Nombre]
      ,ORG.[Grupo_Personal_Id]
      ,ORG.[Grupo_Personal_Nombre]
      ,ORG.[Area_Personal_Id]
      ,ORG.[Area_Personal_Nombre]
      ,ORG.[Clave_Organizacion_Id]
      ,ORG.[Clave_Organizacion_Nombre]
      ,ORG.[Division_Id]
      ,ORG.[Division_Nombre]
      ,ORG.[Subdivision_Personal_Id]
      ,ORG.[Subdivision_Personal_Nombre]
      ,ORG.[Area_Nomina_Id]
      ,ORG.[Area_Nomina_Nombre]
      ,ORG.[Relacion_Laboral_Id]
      ,ORG.[Relacion_Laboral_Nombre]
      ,ORG.[Centro_Coste_Id]
      ,ORG.[Centro_Coste_Nombre]
      ,ORG.[Unidad_Organizativa_Id]
      ,ORG.[Unidad_Organizativa_Nombre]
      ,ORG.[Posicion_Id]
      ,ORG.[Posicion_Nombre]
      ,ORG.[Funcion_Id]
      ,ORG.[Funcion_Nombre]
      ,ORG.[Tipo_Objeto_Id]
      ,ORG.[Tipo_Objeto_Nombre]
      ,ORG.[Grupo_Encargados]
	  ,ORG.Sociedad_Id
      ,PH.[Plan_Regla_Id]
      ,PH.[Estado_GestionTiempos_Id]
      ,PH.[Estado_GestionTiempos_Nombre]
      ,PH.[Porcentaje_HorarioTrabajo]
      ,PH.[Horas_Mensuales]
      ,PH.[Horas_Semanales]
      ,PH.[Horas_Diarias]
      ,PH.[Dias_Laborales_Semanales]
      ,PH.[HorasTrabajo_Anuales]
      ,PH.[Indicador_TiempoParcial]
      ,PH.[HorasMinimas_Diarias]
      ,PH.[HorasMaximas_Diarias]
      ,PH.[HorasMinimas_Semanales]
      ,PH.[HorasMaximas_Semanales]
      ,PH.[HorasMinimas_Mensuales]
      ,PH.[HorasMaximas_Mensuales]
      ,PH.[HorasMinimas_Anuales]
      ,PH.[HorasMaximas_Anuales]
	  ,MED.[Clase_Medida_Id]
      ,MED.[Clase_Medida_Nombre]
      ,MED.[Estado_Ocupacion_Id]
      ,MED.[Estado_Ocupacion_Nombre]
      ,MED.[Estado_Pago_Extraordinario]
      ,MED.[Estado_Pago_Extra_Nombre]
	  , DP.Identidad 
FROM [DL_FARINTER].[dbo].[DL_SAP_Personal_DatosPersonales] DP   ---{{ source ('DL_FARINTER', 'DL_SAP_Personal_DatosPersonales') }}
	LEFT JOIN [DL_FARINTER].[dbo].[DL_SAP_Personal_PlanHorarios] PH ---{{ source ('DL_FARINTER', 'DL_SAP_Personal_PlanHorarios') }}
		ON PH.Personal_Id=DP.Personal_Id 
		AND PH.[Subtipo_Id]=DP.[Subtipo_Id] 
		AND PH.[Objeto_Id]=DP.[Objeto_Id] 
		AND PH.[Indicador_Bloqueo]=DP.[Indicador_Bloqueo] 
		AND CAST(GETDATE() AS DATE) BETWEEN PH.Fecha_Desde AND PH.Fecha_Hasta
	LEFT JOIN [DL_FARINTER].[dbo].[DL_SAP_Personal_Organizacional] ORG ---{{ source ('DL_FARINTER', 'DL_SAP_Personal_Organizacional') }}
		ON ORG.Personal_Id=DP.Personal_Id 
		AND ORG.[Subtipo_Id]=DP.[Subtipo_Id] 
		AND ORG.[Objeto_Id]=DP.[Objeto_Id] 
		AND ORG.[Indicador_Bloqueo]=DP.[Indicador_Bloqueo] 
		AND CAST(GETDATE() AS DATE) BETWEEN ORG.Fecha_Desde AND ORG.Fecha_Hasta
	LEFT JOIN [DL_FARINTER].[dbo].[DL_SAP_Personal_Medidas] MED  ---{{ ref ( 'DL_SAP_Personal_Medidas') }}
		ON MED.Personal_Id=DP.Personal_Id 
		AND MED.[Subtipo_Id]=DP.[Subtipo_Id] 
		AND MED.[Objeto_Id]=DP.[Objeto_Id] 
		AND MED.[Indicador_Bloqueo]=DP.[Indicador_Bloqueo] 
		AND CAST(GETDATE() AS DATE) BETWEEN MED.Fecha_Desde AND MED.Fecha_Hasta
WHERE CAST(GETDATE() AS DATE) BETWEEN DP.Fecha_Desde AND DP.Fecha_Hasta