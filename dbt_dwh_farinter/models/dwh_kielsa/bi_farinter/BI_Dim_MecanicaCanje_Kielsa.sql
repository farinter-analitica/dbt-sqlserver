{{ config(
        tags=["automation/periodo_mensual_inicio", "automation_only"],
        materialized="view",
) }}

SELECT
    Emp_Id,
    MecanicaCanje_Nombre AS Mecanica_Nombre,
    MecanicaCanje_Tipo AS Mecanica_Tipo,
    EmpMecanicaCanje_Id AS Mecanica_Id,
    Fecha_Actualizado
FROM {{ ref('BI_Kielsa_Dim_MecanicaCanje') }}
