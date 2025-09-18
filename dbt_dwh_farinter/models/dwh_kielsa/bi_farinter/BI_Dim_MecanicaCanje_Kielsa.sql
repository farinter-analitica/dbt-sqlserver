{{ config(
        tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
) }}

SELECT
    Emp_Id,
    Mecanica_Nombre,
    Mecanica_Tipo,
    EmpMecanica_Id AS Mecanica_Id,
    Fecha_Actualizado
FROM {{ source('BI_FARINTER', 'BI_Kielsa_Dim_MecanicaCanje') }}
