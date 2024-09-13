
{% snapshot DL_Kielsa_FacturaPosicion_Diferencia_Snapshot %}

    {{
        config(
            tags=["periodo/diario"],
            target_schema='dbt_snapshot',
            strategy='check',
            unique_key="id" ,
            check_cols=['Cantidad_Padre','Valor_Neto','Valor_Total'],
        )
    }}
--Source SCRIPT cannot have identity columns
    SELECT
        {{ dwh_farinter_concat_key_columns(columns=["Factura_Id","Suc_Id","Emp_Id","TipoDoc_Id","Caja_Id","Articulo_Id"], input_length=19, table_alias='FP')}} [id],
        FP.Emp_Id,
        FP.Factura_Id,
        FP.Suc_Id,
        FP.Caja_Id,
        FP.TipoDoc_Id,
        FE.Factura_Fecha,
        FP.Articulo_Id,
        FP.Cantidad_Padre,
        FP.Valor_Neto,
        FE.Valor_Total
    FROM  {{ ref('BI_Kielsa_Hecho_FacturaPosicion') }} FP
    INNER JOIN {{ ref('BI_Kielsa_Hecho_FacturaEncabezado') }} FE
    	ON FE.Emp_Id = FP.Emp_Id
        AND FE.Suc_Id = FP.Suc_Id
        AND FE.TipoDoc_id = FP.TipoDoc_id
        AND FE.Caja_Id = FP.Caja_Id
        AND FE.Factura_Id = FP.Factura_Id
        AND FE.AnioMes_Id = FP.AnioMes_Id
    WHERE FE.Factura_Fecha > CAST(GETDATE()-3 AS DATE) AND FE.Factura_Fecha < CAST('20241003' AS DATE) 
{% endsnapshot %}

