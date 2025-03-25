{%- set unique_key_list = ["Cliente_Id","Emp_Id","Contactar_El"]  -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario","periodo_unico/si"],
		materialized="table",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
        "{{ dwh_farinter_remove_incremental_temp_table() }}",
        "{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        ]
	) 
}}

WITH 
-- Calendario para fechas
Calendario_Filtrado AS (
    SELECT Fecha_Calendario,
        CONVERT(INT, CONVERT(VARCHAR(8), Fecha_Calendario, 112)) AS Semilla
    FROM BI_FARINTER.dbo.BI_Dim_Calendario  -- {{ source('BI_FARINTER', 'BI_Dim_Calendario') }}
    WHERE Fecha_Calendario BETWEEN DATEADD(MONTH, -1, GETDATE()) AND DATEADD(MONTH, 6, GETDATE())
),
-- Vendedores por sucursal
VendedorSucursal_Filtrado AS (
    SELECT Emp_Id, Suc_Id, Vendedor_Id
    FROM DL_FARINTER.dbo.DL_Kielsa_VendedorSucursal  -- {{ source('DL_FARINTER', 'DL_Kielsa_VendedorSucursal') }}
    WHERE Vendedor_Id <> '10000001' AND Bit_Activo = 1
),
-- Base de vendedores con fechas
VSBase AS (
    SELECT
        VS.Emp_Id,
        VS.Suc_Id,
        VS.Vendedor_Id,
        CAL.Fecha_Calendario,
        COUNT(*) OVER (
            PARTITION BY VS.Emp_Id, VS.Suc_Id, CAL.Fecha_Calendario
        ) AS Vendedores_Sucursal,
        ROW_NUMBER() OVER (
            PARTITION BY VS.Emp_Id, VS.Suc_Id, CAL.Fecha_Calendario
            ORDER BY CHECKSUM(VS.Vendedor_Id, CAL.Fecha_Calendario)
        ) AS Vendedor_Sucursal_Orden
    FROM VendedorSucursal_Filtrado VS
    CROSS JOIN Calendario_Filtrado CAL
),
-- Agrupación de vendedores por sucursal
VSAgrupado AS (
    SELECT Emp_Id, Suc_Id, Fecha_Calendario, Vendedores_Sucursal
    FROM VSBase
    GROUP BY Emp_Id, Suc_Id, Fecha_Calendario, Vendedores_Sucursal
),
ClientesBase AS (
    SELECT --TOP 100 
        M.Monedero_Id,
        M.Emp_Id,
        M.Dias_Con_Compra,
        M.Meses_Con_Compra,
        M.Promedio_Dias_Entre_Compra,
        M.Fecha_Primera_Factura,
        M.Fecha_Ultima_Factura,
        M.Dia_Semana_Iso_Preferido,
        M.Dia_Semana_Validez_Estadistica,
        M.Dia_Minimo_Mes,
        M.Dia_Maximo_Mes,
        M.Dia_Mes_Validez_Estadistica,
        M.Sucursal_Id_Preferido Sucursal_Id,
        M.Sucursal_Validez_Estadistica,
        M.Hora_Id_Preferido,
        M.Hora_Validez_Estadistica,
        M.EmpMon_Id,
        S.Departamento_Id,
        S.Municipio_Id,
        S.Ciudad_Id,
        -- Guardar el tipo de preferencia para ciclos posteriores
        CASE 
            WHEN (M.Dia_Semana_Validez_Estadistica = 1 AND M.Promedio_Dias_Entre_Compra < 10)
            THEN 'SEMANAL'
            ELSE 'MENSUAL'
        END AS Tipo_Preferencia,
        -- Guardar la descripción
        CASE 
            WHEN (M.Dia_Semana_Validez_Estadistica = 1 AND M.Promedio_Dias_Entre_Compra < 10) 
            THEN 'Día anterior al preferido de semana'
            ELSE 'Día mínimo del mes'
        END AS Criterio_Seleccion
    FROM {{ ref("BI_Kielsa_Agr_Monedero_Ventana")}} M
    INNER JOIN {{ ref('BI_Kielsa_Dim_Sucursal') }} S
    ON M.Sucursal_Id_Preferido = S.Sucursal_Id
    AND M.Emp_Id = S.Emp_Id
    INNER JOIN {{ ref('BI_Kielsa_Dim_Monedero') }} MON
    ON M.Emp_Id = MON.Emp_Id
    AND M.Monedero_Id = MON.Monedero_Id
    AND MON.Monedero_Id <> '0' AND MON.Activo_Indicador = 1
	LEFT JOIN (
		SELECT Emp_Id, Identidad
		FROM DL_FARINTER.[dbo].[DL_Kielsa_RecetasCalculos] RC --{{ ref('DL_Kielsa_RecetasCalculos') }}
		WHERE RC.Contactar_El >= DATEADD(DAY, -15, GETDATE()) AND RC.Contactar_El <= DATEADD(DAY, 31, GETDATE()) 
		GROUP BY Emp_Id, Identidad
	) RC
	ON M.Emp_Id = RC.Emp_Id AND M.Monedero_Id = RC.Identidad
    WHERE M.Meses_Con_Compra>=6 AND RC.Identidad IS NULL
    AND ((M.Dia_Semana_Validez_Estadistica = 1 AND M.Promedio_Dias_Entre_Compra < 10) OR 
        (M.Dia_Mes_Validez_Estadistica = 1))

),
-- Primero calculamos las fechas base de contacto para cada cliente
FechasBaseContacto AS (
    SELECT 
        CB.*,
        -- Para preferencia semanal: encontrar el día ANTES de su día preferido de la semana
        CASE WHEN CB.Tipo_Preferencia = 'SEMANAL' THEN
            -- Calcular el día anterior a su día preferido esta semana
            DATEADD(DAY, 
                -- Si el día preferido es Lunes (1), usar Domingo (7)
                CASE WHEN CB.Dia_Semana_Iso_Preferido = 1 THEN 
                    -- Calcular días hasta el Domingo
                    (7 - DATEPART(WEEKDAY, GETDATE()) + 7) % 7
                ELSE 
                    -- Calcular días hasta el día anterior al preferido
                    ((CB.Dia_Semana_Iso_Preferido - 1) - DATEPART(WEEKDAY, GETDATE()) + 7) % 7
                END - 7, -- Resta 7 días para ir a la semana anterior
                CAST(GETDATE() AS DATE)
            )
        -- Para preferencia mensual: encontrar su día mínimo del mes
        ELSE
            -- Si hoy es antes o igual a su día mínimo, usar este mes
            CASE WHEN DAY(GETDATE()) <= CB.Dia_Minimo_Mes THEN
                DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), CB.Dia_Minimo_Mes)
            -- De lo contrario usar el próximo mes
            ELSE
                DATEFROMPARTS(
                    YEAR(DATEADD(MONTH, 1, GETDATE())), 
                    MONTH(DATEADD(MONTH, 1, GETDATE())), 
                    CB.Dia_Minimo_Mes
                )
            END
        END AS Fecha_Base_Contacto
        
    FROM ClientesBase CB
),

-- Ahora generamos los 3 ciclos usando CTE recursiva
ClientesFechasContacto AS (
    -- Caso base: Ciclo 1
    SELECT
        FBC.Monedero_Id,
        FBC.Emp_Id,
        FBC.Sucursal_Id,
        FBC.Fecha_Base_Contacto,
        FBC.Tipo_Preferencia,
        FBC.Criterio_Seleccion,
        Fecha_Base_Contacto AS Contactar_El,
        1 AS Ciclo
    FROM FechasBaseContacto FBC
    
    UNION ALL
    
    -- Caso recursivo: Ciclos 2 y 3
    SELECT
        CFC.Monedero_Id,
        CFC.Emp_Id,
        CFC.Sucursal_Id,
        CFC.Fecha_Base_Contacto,
        CFC.Tipo_Preferencia,
        CFC.Criterio_Seleccion,
        CASE 
            WHEN CFC.Tipo_Preferencia = 'SEMANAL' THEN DATEADD(WEEK, 1, CFC.Contactar_El)
            ELSE DATEADD(MONTH, 1, CFC.Contactar_El)
        END AS Contactar_El,
        CFC.Ciclo + 1 AS Ciclo
    FROM ClientesFechasContacto CFC
    WHERE CFC.Ciclo < 3  -- Limitar a 3 ciclos
),
-- Agrupar clientes por sucursal y fecha
ClientesAgrupados AS (
    SELECT 
        Emp_Id, 
        Sucursal_Id, 
        Contactar_El,
        COUNT(*) AS Clientes_Sucursal
    FROM ClientesFechasContacto
    GROUP BY Emp_Id, Sucursal_Id, Contactar_El
),
-- Asignar orden a cada cliente dentro de su sucursal
ClientesOrdenados AS (
    SELECT
        CFC.*,
        CA.Clientes_Sucursal,
        ROW_NUMBER() OVER (
            PARTITION BY CFC.Emp_Id, CFC.Sucursal_Id, CFC.Contactar_El
            ORDER BY CHECKSUM(CFC.Monedero_Id, CFC.Contactar_El)
        ) AS Cliente_Sucursal_Orden
    FROM ClientesFechasContacto CFC
    INNER JOIN ClientesAgrupados CA
        ON CFC.Emp_Id = CA.Emp_Id
        AND CFC.Sucursal_Id = CA.Sucursal_Id
        AND CFC.Contactar_El = CA.Contactar_El
),
-- Asignar vendedor a cada cliente
ClientesVendedores AS (
    SELECT
        CO.*,
        VSA.Vendedores_Sucursal,
        FLOOR((CO.Cliente_Sucursal_Orden - 1) * VSA.Vendedores_Sucursal / CO.Clientes_Sucursal) + 1 AS Vendedor_Sucursal_Orden
    FROM ClientesOrdenados CO
    INNER JOIN VSAgrupado VSA
        ON CO.Sucursal_Id = VSA.Suc_Id
        AND CO.Emp_Id = VSA.Emp_Id
        AND CAST(CO.Contactar_El AS DATE) = VSA.Fecha_Calendario
),
-- Obtener el vendedor específico para cada cliente
ClientesConVendedor AS (
    SELECT
        CV.*,
        VS.Vendedor_Id,
        CASE WHEN VS.Vendedor_Sucursal_Orden >= VS.Vendedores_Sucursal
            THEN 1 ELSE 0 END AS Ultimo_Vendedor_Sucursal
    FROM ClientesVendedores CV
    LEFT JOIN VSBase VS
        ON CV.Sucursal_Id = VS.Suc_Id
        AND CV.Emp_Id = VS.Emp_Id
        AND CAST(CV.Contactar_El AS DATE) = VS.Fecha_Calendario
        AND CV.Vendedor_Sucursal_Orden = VS.Vendedor_Sucursal_Orden
)
SELECT 
    ISNULL(CV.Emp_Id,0) AS Emp_Id,
    ISNULL(CV.Sucursal_Id,0) AS Sucursal_Id,
    ISNULL(CV.Monedero_Id,0) AS Cliente_Id,
    ISNULL(CV.Tipo_Preferencia,'') AS Tipo_Preferencia,
    ISNULL(CV.Criterio_Seleccion,'') AS Criterio_Seleccion,
    ISNULL(CV.Contactar_El, '19000101') AS Contactar_El,
    ISNULL(CV.Ciclo,0) AS Ciclo,
    ISNULL(FBC.Departamento_Id,0) AS Departamento_Id,
    ISNULL(FBC.Municipio_Id,0) AS Municipio_Id,
    ISNULL(FBC.Ciudad_Id,0) AS Ciudad_Id,
    ISNULL(FBC.Hora_Id_Preferido,0) AS Hora_Id_Preferido,
    ISNULL(FBC.Promedio_Dias_Entre_Compra, 0) AS Promedio_Dias_Entre_Compra,
    ISNULL(FBC.Dia_Semana_Iso_Preferido,0) AS Dia_Semana_Iso_Preferido,
    ISNULL(FBC.Dia_Minimo_Mes,0) AS Dia_Minimo_Mes,
    ISNULL(FBC.Dia_Maximo_Mes,0) AS Dia_Maximo_Mes,
    ISNULL(FBC.Dia_Mes_Validez_Estadistica,0) AS Dia_Mes_Validez_Estadistica,
    ISNULL(FBC.Sucursal_Validez_Estadistica,0) AS Sucursal_Validez_Estadistica,
    ISNULL(FBC.Hora_Validez_Estadistica,0) AS Hora_Validez_Estadistica,
    ISNULL(CV.Vendedor_Id, 0) AS Vendedor_Id,
    ISNULL(CV.Clientes_Sucursal, 0) AS Clientes_Sucursal,
    ISNULL(CV.Vendedores_Sucursal, 0) AS Vendedores_Sucursal,
    ISNULL(CV.Cliente_Sucursal_Orden, 0) AS Cliente_Sucursal_Orden,
    ISNULL(CV.Vendedor_Sucursal_Orden, 0) AS Vendedor_Sucursal_Orden,
    ISNULL(CV.Ultimo_Vendedor_Sucursal, 0) AS Ultimo_Vendedor_Sucursal,
    E.Empleado_Nombre AS Vendedor_Nombre,
    GETDATE() AS Fecha_Actualizado
FROM ClientesConVendedor CV
INNER JOIN FechasBaseContacto FBC
    ON CV.Emp_Id = FBC.Emp_Id
    AND CV.Monedero_Id = FBC.Monedero_Id
LEFT JOIN BI_FARINTER.dbo.BI_Kielsa_Dim_Empleado E
    ON CV.Vendedor_Id = E.Vendedor_Id
    AND CV.Emp_Id = E.Emp_Id
