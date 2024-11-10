
{% snapshot DL_Kielsa_Suscripcion_scd %}

    {{
        config(
            as_columnstore=false,
            tags=["periodo/diario"],
            target_schema='dbt_snapshot',
            strategy='timestamp',
            unique_key="Suscripcion_Id" ,
            updated_at='FVigencia',
        )
    }}
--Source SCRIPT cannot have identity columns
SELECT --TOP (1000) 
    ISNULL(CAST([Suscripcion_Id] AS INT),0) AS [Suscripcion_Id]
      ,[TarjetaKC_Id] COLLATE DATABASE_DEFAULT AS [TarjetaKC_Id]
      ,[Cliente_Nombre] COLLATE DATABASE_DEFAULT AS [Cliente_Nombre]
      ,[Usuario_Registro] COLLATE DATABASE_DEFAULT AS [Usuario_Registro]
      ,[Sucursal_Registro]
      ,[TipoPlan]
      ,[FRegistro]
      ,[FVigencia]
      ,[Activacion]
      ,[FActivacion]
      ,[FDesactivacion]
      ,[CodPlanKielsaClinica] COLLATE DATABASE_DEFAULT AS [CodPlanKielsaClinica]
      ,[Cobro]
      ,[Transaccion_Tarjeta_Credito]
      ,[Celular] COLLATE DATABASE_DEFAULT AS [Celular]
      ,[Origen]
      ,[LogMovId]
  FROM [{{ var('P_SQLLDSUBS_LS') }}].[KPP_DB].[dbo].[Suscripcion] -- {{ source('KPP_DB', 'Suscripcion') }}
{% endsnapshot %}

