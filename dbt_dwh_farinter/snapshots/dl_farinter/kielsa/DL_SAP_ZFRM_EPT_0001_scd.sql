
{% snapshot DL_SAP_ZFRM_EPT_0001_scd %}

    {{
        config(
            as_columnstore=true,
            tags=["periodo/diario", "automation/si"],
            target_schema='dbt_snapshot',
            strategy='timestamp',
            unique_key="BNAME" ,
            updated_at='Fecha_Actualizado',
        )
    }}
--Source SCRIPT cannot have identity columns
SELECT --TOP (1000) 
    ISNULL([BNAME] ,0) AS [BNAME]
      ,[VKGRP]
      ,[Fecha_Actualizado]
  FROM [DL_SAP_ZFRM_EPT_0001] -- {{ ref('DL_SAP_ZFRM_EPT_0001') }}
{% endsnapshot %}

