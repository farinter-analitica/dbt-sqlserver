{% set unique_key_list = [] %}

{{ 
    config(
        tags=["automation/periodo_mensual"],
        materialized="view",
    )
}}

SELECT
    -- Primary Keys and Foreign Keys
    RD.Regalia_Id,
    RD.Emp_Id,
    RD.Suc_Id,
    RD.Caja_Id,
    RD.Detalle_Id,
    RD.Articulo_Id, 
    RD.Articulo_Padre_Id,
    RD.Bodega_Id,
    RD.Consecutivo,
    
    -- Encabezado
    RD.Cliente_Id,
    RD.Vendedor_Id,
    RD.Identidad_Limpia,
    RD.EmpSucCajReg_Id,
    RD.EmpSuc_Id,
    RD.EmpCli_Id,
    RD.EmpVen_Id,
    RD.EmpMon_Id,
    RD.Tipo_Origen,
    RD.Operacion_Id,

    -- Date and Time
    RD.Detalle_Momento,
    RD.Detalle_Fecha,
    RD.Detalle_Hora,

    -- Transaction Details
    RD.Cantidad_Original,
    RD.Cantidad_Padre,
    RD.Valor_Costo_Unitario,
    RD.Precio_Unitario,
    RD.Valor_Costo_Total,
    RD.Valor_Total,
    
    -- Product Information
    RD.Porcentaje_Impuesto,
    RD.Valor_Impuesto,
    
    -- Incentivos
    ISNULL(INC.regalia_aplica_incentivo, 0) AS Regalia_Aplica_Incentivo,
    ISNULL(INC.regalia_valor_incentivo_unitario, 0.0) AS Regalia_Valor_Incentivo_Unitario,
    ISNULL(INC.regalia_valor_incentivo_total, 0.0) AS Regalia_Valor_Incentivo_Total,
    
    -- Audit Fields
    RD.Fecha_Carga,
    RD.Fecha_Actualizado,
    
    -- Concatenated Keys
    RD.EmpSucCajRegDet_Id,
    RD.EmpArt_Id

FROM {{ ref('BI_Kielsa_Hecho_Regalia_Detalle') }} RD
LEFT JOIN {{ source('DL_FARINTER', 'DL_Kielsa_Regalia_Incentivo') }} INC
    ON RD.Emp_Id = INC.Emp_Id
    AND RD.Suc_Id = INC.Suc_Id
    AND RD.Caja_Id = INC.Caja_Id
    AND RD.Regalia_Id = INC.Regalia_Id
    AND RD.Detalle_Id = INC.Detalle_Id