
{%- set unique_key_list = ["Suc_Id","Rol_Sucursal","Emp_Id"] -%}
{{ 
    config(
		as_columnstore=true,
		tags=["periodo/diario"],
		materialized="incremental",
		incremental_strategy="farinter_merge",
		unique_key=unique_key_list,
		merge_exclude_columns=unique_key_list + ["Fecha_Carga"],
		merge_check_diff_exclude_columns=unique_key_list + ["Fecha_Carga","Fecha_Actualizado"],
		post_hook=[
		"{{ dwh_farinter_remove_incremental_temp_table() }}",
		"{{ dwh_farinter_create_primary_key(columns=" ~ unique_key_list | tojson ~ ", create_clustered=false, is_incremental=is_incremental(), if_another_exists_drop_it=true) }}",
        "{{ dwh_farinter_create_index(is_incremental=is_incremental(), columns=['Fecha_Actualizado']) }}",
		]
		
) }}

{%- set query_empresas -%}
SELECT Empresa_Id, Empresa_Id_Original, Pais_Id
	--, LS_LDCOM_RepLocal, D_LDCOM_RepLocal 
	--,LS_LDCOM, D_LDCOM
	,LS_LDCOM_Replica AS Servidor_Vinculado, D_LDCOM_Replica as Base_Datos
FROM BI_FARINTER.dbo.BI_Kielsa_Dim_Empresa WITH (NOLOCK)
WHERE LS_LDCOM_RepLocal IS NOT NULL and Es_Empresa_Principal = 1
{%- endset -%}
{%- set empresas = run_query_and_return(query_empresas) -%} {# Returns: [{Empresa_Id,Emp_Id_Original,Pais_Id,LS_LDCOM_Replica,D_LDCOM_Replica}] #}

{# {%- if is_incremental() %}
    {%- set last_date = run_single_value_query_on_relation_and_return(query="""select ISNULL(CONVERT(VARCHAR,DATEADD(DAY, -365, max(SxU_Fec_Actualizacion)), 112), '19000101')  from  """ ~ this, relation_not_found_value='19000101'|string)|string %}
{%- else %}
    {%- set last_date = '19000101' %}
{%- endif %} #}

WITH rol_supervisor AS (
    SELECT
        *,
        (
            CASE
                WHEN
                    Emp_Id = 5
                    AND Rol_Nombre IN ('Supervisor de Zona', 'Supervisor de Operaciones') THEN 'Supervisor'
                WHEN
                    Emp_Id = 3
                    AND Rol_Nombre LIKE 'Supervisor de Operaciones' THEN 'Supervisor'
                WHEN Rol_Nombre LIKE 'Supervisor de Operaciones' THEN 'Supervisor'
                ELSE ''
            END
        ) AS Rol_Sucursal
    FROM DL_FARINTER.DBO.DL_Kielsa_Seg_Rol -- {{ source('DL_FARINTER', 'DL_Kielsa_Seg_Rol') }}
    WHERE (
        CASE
            WHEN
                Emp_Id = 5
                AND Rol_Nombre LIKE 'Supervisor de Zona' THEN 'Supervisor'
            WHEN
                Emp_Id = 3
                AND Rol_Nombre LIKE 'Supervisor de Operaciones' THEN 'Supervisor'
            WHEN Rol_Nombre LIKE 'Supervisor de Operaciones' THEN 'Supervisor'
            ELSE ''
        END
    ) = 'Supervisor'
),

rol_jop AS (
    SELECT
        *,
        (
            CASE
                WHEN
                    Emp_Id = 5
                    AND Rol_Nombre LIKE 'Gerente de Operaciones' THEN 'JOP'
                WHEN
                    Emp_Id = 3
                    AND Rol_Nombre LIKE 'Coordinador de Operaciones' THEN 'JOP'
                WHEN Rol_Nombre LIKE 'Jefe de Operaciones' THEN 'JOP'
                ELSE ''
            END
        ) AS Rol_Sucursal
    FROM DL_FARINTER.DBO.DL_Kielsa_Seg_Rol -- {{ source('DL_FARINTER', 'DL_Kielsa_Seg_Rol') }}
    WHERE --Emp_Id=3 AND Rol_Padre =7 and Rol_Nivel <10 --and Rol_Nombre LIKE '%operac%'
        (
            CASE
                WHEN
                    Emp_Id = 5
                    AND Rol_Nombre LIKE 'Gerente de Operaciones' THEN 1
                WHEN
                    Emp_Id = 3
                    AND Rol_Nombre LIKE 'Coordinador de Operaciones' THEN 1
                WHEN Rol_Nombre LIKE 'Jefe de Operaciones' THEN 1
                ELSE 0
            END
        ) = 1
),

usuario_jop AS (
    SELECT
        RJ.Emp_Id,
        RJ.Rol_Id,
        RJ.Rol_Nombre,
        RJ.Rol_Sucursal,
        U.Usuario_Email,
        U.Usuario_Login,
        U.Usuario_Nombre,
        U.Usuario_Id
    FROM rol_jop AS RJ
    INNER JOIN DL_FARINTER.DBO.DL_Kielsa_Seg_Usuario_x_Rol AS UR -- {{ ref("DL_Kielsa_Seg_Usuario_x_Rol") }}
        ON
            RJ.Rol_Id = UR.Rol_Id
            AND RJ.Emp_Id = UR.Emp_Id
    INNER JOIN DL_FARINTER.DBO.DL_Kielsa_Seg_Usuario AS U -- {{ ref("DL_Kielsa_Seg_Usuario") }}
        ON
            UR.Emp_Id = U.Emp_Id
            AND UR.Usuario_Id = U.Usuario_Id
    WHERE
        U.Usuario_Eliminado = 0
        AND U.Usuario_Cuenta_Deshabilitada = 0
),

usuario_supervisor AS (
    SELECT
        RS.Emp_Id,
        RS.Rol_Id,
        RS.Rol_Nombre,
        RS.Rol_Sucursal,
        U.Usuario_Email,
        U.Usuario_Login,
        U.Usuario_Nombre,
        U.Usuario_Id
    FROM rol_supervisor AS RS
    INNER JOIN DL_FARINTER.DBO.DL_Kielsa_Seg_Usuario_x_Rol AS UR -- {{ ref("DL_Kielsa_Seg_Usuario_x_Rol") }}
        ON
            RS.Rol_Id = UR.Rol_Id
            AND RS.Emp_Id = UR.Emp_Id
    INNER JOIN DL_FARINTER.DBO.DL_Kielsa_Seg_Usuario AS U
        ON
            UR.Emp_Id = U.Emp_Id
            AND UR.Usuario_Id = U.Usuario_Id
    WHERE
        U.Usuario_Eliminado = 0
        AND U.Usuario_Cuenta_Deshabilitada = 0
),

usuario_supervisor_sucursal AS (
    SELECT
        SU.Emp_Id,
        SU.Suc_Id,
        US.Rol_Sucursal,
        SU.Usuario_Id,
        US.Rol_Id,
        US.Rol_Nombre,
        US.Usuario_Nombre,
        SU.SxU_Fec_Actualizacion,
        ROW_NUMBER() OVER (
            PARTITION BY SU.Emp_Id,
            SU.Suc_Id
            ORDER BY SU.SxU_Fec_Actualizacion DESC
        ) AS Orden_Asignacion
    FROM usuario_supervisor AS US
    INNER JOIN DL_FARINTER.DBO.DL_Kielsa_Seg_Sucursal_x_Usuario AS SU -- {{ ref('DL_Kielsa_Seg_Sucursal_x_Usuario') }}
        ON
            US.Emp_Id = SU.Emp_id
            AND US.Usuario_Id = SU.Usuario_Id
),

usuario_jop_sucursal AS (
    SELECT
        SU.Emp_Id,
        SU.Suc_Id,
        US.Rol_Sucursal,
        SU.Usuario_Id,
        US.Rol_Id,
        US.Rol_Nombre,
        US.Usuario_Nombre,
        SU.SxU_Fec_Actualizacion,
        ROW_NUMBER() OVER (
            PARTITION BY SU.Emp_Id,
            SU.Suc_Id
            ORDER BY SU.SxU_Fec_Actualizacion DESC
        ) AS Orden_Asignacion
    FROM usuario_jop AS US
    INNER JOIN DL_FARINTER.DBO.DL_Kielsa_Seg_Sucursal_x_Usuario AS SU -- {{ ref('DL_Kielsa_Seg_Sucursal_x_Usuario') }}
        ON
            US.Emp_Id = SU.Emp_id
            AND US.Usuario_Id = SU.Usuario_Id
),

datos_finales AS (
    SELECT *
    FROM usuario_supervisor_sucursal
    WHERE Orden_Asignacion = 1
    UNION ALL
    SELECT *
    FROM usuario_jop_sucursal
    WHERE Orden_Asignacion = 1
)

SELECT
    *,
    ISNULL(
        {{ dwh_farinter_hash_column(unique_key_list) }},
        ''
    ) AS Hash_SucRolEmp,
    GETDATE() AS [Fecha_Carga],
    GETDATE() AS [Fecha_Actualizado]
FROM datos_finales
