{% set unique_key_list = [] %}

{{ 
    config(
        tags=["automation/periodo_mensual_inicio"],
        materialized="view",
    )
}}

WITH
Matriz AS
(
SELECT 
CONCAT(Emp_Id,'_',[Tipo_Nombre],'_',[Rol_Jerarquia_Empleado], '_' , [Categoria_LLave], '_' ,[Regla_Numero]) AS Llave
,CONCAT(Emp_Id,'_',[Tipo_Nombre],'_',[Rol_Jerarquia_Empleado], '_' , [Categoria_LLave], '_' ) AS Grupo
,[Emp_Id]
      ,[Tipo_Nombre]
      ,[Rol_Jerarquia_Empleado]
      ,[Rol_Nombre]
      ,[Categoria_LLave]
      ,[Regla_Numero]
      ,CASE WHEN [Sucursal_Categoria_Venta_Meta] = '' THEN NULL 
        ELSE [Sucursal_Categoria_Venta_Meta] END AS Sucursal_Categoria_Venta_Meta
      ,[Alcance_MetaGeneral_Minimo_Cerrado]
      ,[Alcance_MetaXMarcaPropia_Minimo_Cerrado]
      ,[Alcance_MetaXMPA_Minimo_Cerrado]
      ,[Alcance_MetaXAlerta_Minimo_Cerrado]
      ,[Margen_Meta_Minimo_Abierto]
      ,[Moneda_Id]
      ,[Incentivo_General_Valor]
      ,[Incentivo_General_Porcentaje]
      ,[Incentivo_MPA_Valor]
      ,[Incentivo_MPA_Porcentaje]
      ,[Incentivo_Recomendacion_Valor]
      ,[Incentivo_Recomendacion_Porcentaje]
      ,[Condicion_Combinada_MetaGeneral]
      ,[Condicion_Combinada_XAlerta]
      
  FROM [ADM_FARINTER].[dbo].[ADM_Kielsa_MatrizIncentivos_Empleado]
  WHERE GETDATE()>= Fecha_Desde AND GETDATE()< Fecha_Hasta
),
Grupos AS
(
SELECT *
    ,DENSE_RANK() OVER (PARTITION BY Grupo ORDER BY Alcance_MetaGeneral_Minimo_Cerrado) AS Regla_Numero_Alcance_MetaGeneral
    ,DENSE_RANK() OVER (PARTITION BY Grupo ORDER BY Alcance_MetaXMarcaPropia_Minimo_Cerrado) AS Regla_Numero_Alcance_MetaXMarcaPropia
    ,DENSE_RANK() OVER (PARTITION BY Grupo ORDER BY Alcance_MetaXMPA_Minimo_Cerrado) AS Regla_Numero_Alcance_MetaXMPA
    ,DENSE_RANK() OVER (PARTITION BY Grupo ORDER BY Alcance_MetaXAlerta_Minimo_Cerrado) AS Regla_Numero_Alcance_MetaXAlerta
    ,DENSE_RANK() OVER (PARTITION BY Grupo ORDER BY Margen_Meta_Minimo_Abierto) AS Regla_Numero_Margen_Meta
FROM Matriz),
Final AS
(
SELECT TOP 100 PERCENT Grupos.* 
    ,MetaGeneralSiguiente.Alcance_MetaGeneral_Minimo_Cerrado AS Alcance_MetaGeneral_Maximo_Abierto
    ,MetaXMarcaPropiaSiguiente.Alcance_MetaXMarcaPropia_Minimo_Cerrado AS Alcance_MetaXMarcaPropia_Maximo_Abierto
    ,MetaXMPASiguiente.Alcance_MetaXMPA_Minimo_Cerrado AS Alcance_MetaXMPA_Maximo_Abierto
    ,MetaXAlertaSiguiente.Alcance_MetaXAlerta_Minimo_Cerrado AS Alcance_MetaXAlerta_Maximo_Abierto
    ,MargenMetaSiguiente.Margen_Meta_Minimo_Abierto AS Margen_Meta_Maximo_Cerrado
FROM Grupos
LEFT JOIN (
    SELECT Grupo
        , Regla_Numero_Alcance_MetaGeneral-1 AS Regla_Numero_Alcance_MetaGeneral_Anterior  
        , Alcance_MetaGeneral_Minimo_Cerrado
    FROM Grupos 
    GROUP BY Grupo, Regla_Numero_Alcance_MetaGeneral, Alcance_MetaGeneral_Minimo_Cerrado 
    ) MetaGeneralSiguiente 
    ON MetaGeneralSiguiente.Grupo = Grupos.Grupo
    AND MetaGeneralSiguiente.Regla_Numero_Alcance_MetaGeneral_Anterior = Grupos.Regla_Numero_Alcance_MetaGeneral
LEFT JOIN (
    SELECT Grupo
        , Regla_Numero_Alcance_MetaXMarcaPropia-1 AS Regla_Numero_Alcance_MetaXMarcaPropia_Anterior
        , Alcance_MetaXMarcaPropia_Minimo_Cerrado
    FROM Grupos 
    GROUP BY Grupo, Regla_Numero_Alcance_MetaXMarcaPropia, Alcance_MetaXMarcaPropia_Minimo_Cerrado
    ) MetaXMarcaPropiaSiguiente 
    ON MetaXMarcaPropiaSiguiente.Grupo = Grupos.Grupo
    AND MetaXMarcaPropiaSiguiente.Regla_Numero_Alcance_MetaXMarcaPropia_Anterior = Grupos.Regla_Numero_Alcance_MetaXMarcaPropia
LEFT JOIN (
    SELECT Grupo
        , Regla_Numero_Alcance_MetaXMPA-1 AS Regla_Numero_Alcance_MetaXMPA_Anterior
        , Alcance_MetaXMPA_Minimo_Cerrado
    FROM Grupos 
    GROUP BY Grupo, Regla_Numero_Alcance_MetaXMPA, Alcance_MetaXMPA_Minimo_Cerrado
    ) MetaXMPASiguiente 
    ON MetaXMPASiguiente.Grupo = Grupos.Grupo
    AND MetaXMPASiguiente.Regla_Numero_Alcance_MetaXMPA_Anterior = Grupos.Regla_Numero_Alcance_MetaXMPA
LEFT JOIN (
    SELECT Grupo
        , Regla_Numero_Alcance_MetaXAlerta-1 AS Regla_Numero_Alcance_MetaXAlerta_Anterior
        , Alcance_MetaXAlerta_Minimo_Cerrado
    FROM Grupos 
    GROUP BY Grupo, Regla_Numero_Alcance_MetaXAlerta, Alcance_MetaXAlerta_Minimo_Cerrado
    ) MetaXAlertaSiguiente 
    ON MetaXAlertaSiguiente.Grupo = Grupos.Grupo
    AND MetaXAlertaSiguiente.Regla_Numero_Alcance_MetaXAlerta_Anterior = Grupos.Regla_Numero_Alcance_MetaXAlerta
LEFT JOIN (
    SELECT Grupo
        , Regla_Numero_Margen_Meta-1 AS Regla_Numero_Margen_Meta_Anterior  
        , Margen_Meta_Minimo_Abierto
    FROM Grupos 
    GROUP BY Grupo, Regla_Numero_Margen_Meta, Margen_Meta_Minimo_Abierto
    ) MargenMetaSiguiente 
    ON MargenMetaSiguiente.Grupo = Grupos.Grupo
    AND MargenMetaSiguiente.Regla_Numero_Margen_Meta_Anterior = Grupos.Regla_Numero_Margen_Meta
ORDER BY Llave
)
SELECT * FROM Final