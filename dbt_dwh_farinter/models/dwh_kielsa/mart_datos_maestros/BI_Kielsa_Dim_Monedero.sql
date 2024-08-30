
{{ 
    config(
		materialized="view",
		tags=["periodo/diario", "periodo/por_hora"],
	) 
}}
--dbt dagster
SELECT [Monedero_Id]
      ,[Emp_Id]
      ,[Version_Id]
      ,[Version_Fecha]
      ,[Monedero_Nombre]
      ,[Tipo_Plan]
      ,[Identificacion]
      ,[Identificacion_Formato]
      ,[Telefono]
      ,[Celular]
      ,[Nacimiento]
      ,[Edad]
      ,[RangoEdad]
      ,[Correo]
      ,[Activo_Indicador]
      ,[Acumula_Indicador]
      ,[Principal_Indicador]
      ,[Genero]
      ,[Saldo_Puntos]
      ,[Ingreso]
      ,[MonederoTarj_Id_Original]
      ,[Nombre]
      ,[Apellido]
      ,[UltimaCompra]
      ,[Fecha_Modificado]
      ,[Hash_MonederoEmp]
      ,[Hash_MonederoEmpVersion]
      ,[HashStr_MonEmp]
      ,[HashStr_MonEmpVer]
	    , {{ dwh_farinter_concat_key_columns(columns=['Emp_Id', 'Monedero_Id'], input_length=49, table_alias='')}} [EmpMon_Id]
  FROM {{ source('DL_FARINTER', 'DL_Kielsa_Monedero') }} 