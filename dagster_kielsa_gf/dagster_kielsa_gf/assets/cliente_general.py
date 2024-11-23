import unittest
import warnings
from datetime import datetime, timedelta
from typing import Sequence

import polars as pl
import polars.testing as pltest
from dagster import (
    AssetChecksDefinition,
    AssetKey,
    AssetsDefinition,
    In,
    GraphIn,
    Nothing,
    OpExecutionContext,
    Out,
    asset,
    build_last_update_freshness_checks,
    graph,
    instance_for_test,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    materialize,
    op,
)

from dagster_shared_gf.automation import automation_daily_delta_2_cron
from dagster_shared_gf.resources.smb_resources import (
    SMBResource,
    smb_resource_staging_dagster_dwh,
)
from dagster_shared_gf.resources.sql_server_resources import (
    SQLServerResource,
    dwh_farinter_bi,
    dwh_farinter_dl,
)
from dagster_shared_gf.shared_constants import (
    EMAIL_REGEX_PATTERN_RUST_CRATES,
    EMAIL_REGEX_INVALID_DOTS_PATTERN,
)
from dagster_shared_gf.shared_functions import (
    SQLScriptGenerator,
    filter_assets_by_tags,
    get_for_current_env,
)
from dagster_shared_gf.shared_variables import tags_repo
from dagster_shared_gf.shared_variables import env_str

if __name__ == "__main__":
    from dagster_polars import PolarsParquetIOManager

top_clause = get_for_current_env(
    {"local": "TOP 100", "dev": "--TOP 100", "prd": "--TOP 100"}
)

warnings.filterwarnings("ignore", message="PolarsParquetIOManager", append=True)

DL_MDBECOMM_Usuarios = AssetKey(["DL_FARINTER", "dbo", "DL_MDBECOMM_Usuarios"])
DL_Kielsa_Monedero = AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Monedero"])
DL_Kielsa_Libros_Cliente = AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Libros_Cliente"])
DL_Kielsa_Libros_Tipo = AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Libros_Tipo"])
DL_Kielsa_Cliente = AssetKey(["DL_FARINTER", "dbo", "DL_Kielsa_Cliente"])
BI_Kielsa_Dim_Empresa = AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Empresa"])
BI_Kielsa_Dim_Pais = AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_Pais"])


@op(
    ins={DL_MDBECOMM_Usuarios.to_python_identifier(): In(Nothing)},
    out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
)
def get_df_ecommerce(dwh_farinter_dl: SQLServerResource) -> pl.DataFrame:
    # Define the SQL query to select data from the database

    sql_query = f"""
    SELECT 
        profile_idnumber,
        1 as Emp_Id,
        profile_fullname,
        profile_telephone,
        profile_birthday_date,
        email_principal,
        profile_gender
    FROM (
        SELECT {top_clause}
            [_id]
            ,1 as Emp_Id
            ,[created_at_date]
            ,[profile_fullname]
            ,[profile_role]
            ,[profile_idnumber]
            ,[profile_telephone]
            ,[profile_country]
            ,[profile_termsandconditionsaccepted]
            ,[profile_issenior]
            ,[profile_validatedsenior]
            ,[profile_callingcode]
            ,[profile_gender]
            ,[profile_defaultaddress]
            ,[profile_zoneid]
            ,[profile_packagingaccepted]
            ,[profile_isregisterkielsakash]
            ,[profile_affiliate]
            ,[profile_affiliatenumberlicense]
            ,[profile_agreementid]
            ,[profile_bloodtype]
            ,[profile_height]
            ,[profile_weight]
            ,[profile_birthday_date]
            ,[profile_idimage]
            ,[profile_idcityselected]
            ,[profile_rtn]
            ,[profile_rtnname]
            ,[profile_cityselectedname]
            ,[username]
            ,[profile_repartidor_id]
            ,[last_logintoken_date]
            ,[last_email_verified]
            ,[email_principal]
            ,[email_es_verificado]
            ,[roles]
            ,[profile_listzoneid]
            ,[fecha_actualizado]
            ,ROW_NUMBER() OVER (PARTITION BY [profile_idnumber] ORDER BY [last_logintoken_date] DESC) Fila
        FROM [DL_FARINTER].[dbo].[DL_MDBECOMM_Usuarios]
        WHERE profile_role = 'client'
        AND profile_idnumber IS NOT NULL
    ) EC
    WHERE Fila = 1
    """

    # Execute the SQL query and store the result in a Polars DataFrame
    df_ecommerce = pl.read_database(
        sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string()
    ).with_columns(
        pl.col("Emp_Id").cast(pl.Int32),
        pl.col("profile_idnumber").cast(pl.String).str.to_uppercase().str.strip_chars(),
    )

    return df_ecommerce


@op(
    ins={DL_Kielsa_Monedero.to_python_identifier(): In(Nothing)},
    out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
)
def get_df_monederos(dwh_farinter_dl: SQLServerResource) -> pl.DataFrame:
    # Define the SQL query to select data from the database
    sql_query = f"""
        SELECT {top_clause}
            M.[Monedero_Id] 
            , M.[Emp_Id]
            , M.[Monedero_Nombre] 
            , M.[Tipo_Plan]
            , M.[Identificacion]
            , M.[Identificacion_Formato]
            , M.[Telefono]
            , M.[Correo]
            , M.[Celular]
            , M.[Nacimiento] 
            , M.[Activo_Indicador]
            , M.[Acumula_Indicador]
            , M.[Principal_Indicador]
            , M.[Genero]
            , M.[Saldo_Puntos]
            , M.[Ingreso] 
            , M.[MonederoTarj_Id_Original]
            , M.[UltimaCompra] 
            , M.[Hash_MonederoEmp] 
            , M.[Nombre] 
            , M.[Apellido] 
            , M.[Segundo_Nombre]
            , M.[Segundo_Apellido] 
            , M.[Departamento_Id]
            , M.[Municipio_Id]
            , M.[Ciudad_Id]
            , M.[Barrio_Id]
        FROM [DL_FARINTER].[dbo].[DL_Kielsa_Monedero] M
    """

    # Execute the SQL query and store the result in a Polars DataFrame
    df_monedero = pl.read_database(
        sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string()
    ).with_columns(
        pl.col("Emp_Id").cast(pl.Int32),
        pl.col("Monedero_Id").str.to_uppercase().str.strip_chars(),
        #pl.col("Monedero_Id").str.to_uppercase().alias("Identidad_Limpia"),
    )

    return df_monedero


@op(
    ins={DL_Kielsa_Libros_Cliente.to_python_identifier(): In(Nothing)},
    out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
)
def get_df_libros_cliente(dwh_farinter_dl: SQLServerResource) -> pl.DataFrame:
    # Define the SQL query to select data from the database
    sql_query = f"""
        SELECT {top_clause} 
            Identidad_Limpia,
            Pais_Id,
            Nombre,
            Identidad,
            Telefono,
            Fecha_Nacimiento,
            Fecha_Creacion,
            Tipo_Cliente,
            Medipack,
            Departamento_Id,
            Municipio_Id
        FROM [DL_FARINTER].[dbo].[DL_Kielsa_Libros_Cliente]
    """

    # Execute the SQL query and store the result in a Polars DataFrame
    df_libros_cliente = pl.read_database(
        sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string()
    ).with_columns(
        pl.col("Tipo_Cliente").cast(pl.Int32),
        pl.col("Pais_Id").cast(pl.Int32),
        pl.col("Pais_Id").cast(pl.Int32).alias("Emp_Id"),
        pl.col("Identidad_Limpia").str.to_uppercase().str.strip_chars(),
    )

    return df_libros_cliente


@op(
    ins={DL_Kielsa_Libros_Tipo.to_python_identifier(): In(Nothing)},
    out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
)
def get_df_libros_tipo(dwh_farinter_dl: SQLServerResource) -> pl.DataFrame:
    # Define the SQL query to select data from the database
    sql_query = f"""
        SELECT {top_clause}
                * 
        FROM [DL_FARINTER].[dbo].[DL_Kielsa_Libros_Tipo]
    """

    # Execute the SQL query and store the result in a Polars DataFrame
    df_libros_tipo = pl.read_database(
        sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string()
    ).with_columns(pl.col("Tipo_Id").cast(pl.Int32))

    return df_libros_tipo


@op(
    ins={DL_Kielsa_Cliente.to_python_identifier(): In(Nothing)},
    out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
)
def get_df_clientes(dwh_farinter_dl: SQLServerResource) -> pl.DataFrame:
    # Define the SQL query to select data from the database
    sql_query = f"""
        SELECT {top_clause}
            Cedula,
            Emp_Id,
            Cliente_Nombre,
            Correo,
            Estado,
            Tipo_Cliente
        FROM [DL_FARINTER].[dbo].[DL_Kielsa_Cliente]
        WHERE Indicador_Filtro_Unico = 1
    """

    # Execute the SQL query and store the result in a Polars DataFrame
    df_cliente = pl.read_database(
        sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string()
    ).with_columns(pl.col("Emp_Id").cast(pl.Int32), pl.col("Cedula").str.to_uppercase().str.strip_chars())
    print(df_cliente.columns)

    return df_cliente


@op(
    ins={BI_Kielsa_Dim_Empresa.to_python_identifier(): In(Nothing)},
    out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
)
def get_df_empresas(dwh_farinter_bi: SQLServerResource) -> pl.DataFrame:
    sql_query = f"""
        SELECT {top_clause} 
            E.[Empresa_Id] AS [Emp_Id]
            --,E.[Empresa_Nombre]
            , E.[Pais_Id]
        FROM [BI_FARINTER].[dbo].[BI_Kielsa_Dim_Empresa] E
    """

    # Execute the SQL query and store the result in a Polars DataFrame
    df_empresas = pl.read_database(
        sql_query, dwh_farinter_bi.get_arrow_odbc_conn_string()
    )

    return df_empresas


@op(out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"))
def process_dfs_clientes(
    context: OpExecutionContext,
    df_monedero: pl.DataFrame,
    df_libros_cliente: pl.DataFrame,
    df_libros_tipo: pl.DataFrame,
    df_ecommerce: pl.DataFrame,
    df_cliente: pl.DataFrame,
    df_empresas: pl.DataFrame,
    df_paises_patterns: pl.DataFrame,
) -> pl.DataFrame:
    # Get phone patterns by Emp_Id on df_empresas
    df_empresas = df_empresas.join(df_paises_patterns, on="Pais_Id", how="left").select(
        ["Emp_Id", "Pais_Id", "Pais_ISO2", "pattern"]
    )

    # Preprocess the dataframes
    df_monedero = (
        df_monedero.join(df_empresas, on="Emp_Id", how="left")
        .with_columns(
            pl.col("Telefono")
            .str.contains(pl.col("pattern"))
            .alias("Telefono_Valido")
            .cast(pl.Boolean),
            pl.col("Celular")
            .str.contains(pl.col("pattern"))
            .alias("Celular_Valido")
            .cast(pl.Boolean),
            pl.col("Correo").alias("Correo"),
            (
                pl.col("Correo").str.contains(EMAIL_REGEX_PATTERN_RUST_CRATES)
                & pl.col("Correo").str.contains(EMAIL_REGEX_INVALID_DOTS_PATTERN).not_()
            )
            .cast(pl.Boolean)
            .alias("Correo_Valido"),
        )
        .drop(["pattern"])
    )

    df_libros_cliente = (
        df_libros_cliente.join(df_empresas, on="Emp_Id", how="left")
        .with_columns(
            pl.col("Telefono")
            .str.contains(pl.col("pattern"))
            .alias("Telefono_Valido")
            .cast(pl.Boolean),
            pl.col("Telefono").alias("Celular"),
            pl.col("Telefono")
            .str.contains(pl.col("pattern"))
            .alias("Celular_Valido")
            .cast(pl.Boolean),
        )
        .drop(["pattern"])
    )

    df_ecommerce = (
        df_ecommerce.join(df_empresas, on="Emp_Id", how="left")
        .with_columns(
            pl.col("profile_telephone").alias("Celular"),
            pl.col("profile_telephone").alias("Telefono"),
            pl.col("profile_telephone")
            .str.contains(pl.col("pattern"))
            .cast(pl.Boolean)
            .alias("Telefono_Valido"),
            pl.col("profile_telephone")
            .str.contains(pl.col("pattern"))
            .cast(pl.Boolean)
            .alias("Celular_Valido"),
            pl.col("email_principal").alias("Correo"),
            (
                pl.col("email_principal").str.contains(EMAIL_REGEX_PATTERN_RUST_CRATES)
                & pl.col("email_principal").str.contains(EMAIL_REGEX_INVALID_DOTS_PATTERN).not_()
            )
            .cast(pl.Boolean)
            .alias("Correo_Valido"),
            pl.when(pl.col("profile_gender") == "mujer")
            .then(pl.lit("M"))
            .when(pl.col("profile_gender") == "hombre")
            .then(pl.lit("H"))
            .otherwise(pl.lit("N"))
            .alias("Genero"),
        )
        .drop(["pattern"])
    )

    df_cliente = df_cliente.join(df_empresas, on="Emp_Id", how="left").with_columns(
        pl.lit(None).alias("Telefono"),
        pl.lit(0).cast(pl.Boolean).alias("Telefono_Valido"),
        pl.lit(None).alias("Celular"),
        pl.lit(0).cast(pl.Boolean).alias("Celular_Valido"),
        pl.col("Correo").alias("Correo"),
        (
            pl.col("Correo").str.contains(EMAIL_REGEX_PATTERN_RUST_CRATES)
            & pl.col("Correo").str.contains(EMAIL_REGEX_INVALID_DOTS_PATTERN).not_()
        )
        .cast(pl.Boolean)
        .alias("Correo_Valido"),
    )

    # Process the dataframes
    df_clientes1 = (
        df_monedero.join(
            df_libros_cliente,
            left_on=["Monedero_Id", "Emp_Id"],
            right_on=["Identidad_Limpia", "Emp_Id"],
            how="left",
            suffix="_Libros",
        )
        .join(df_libros_tipo, left_on="Tipo_Cliente", right_on="Tipo_Id", how="left")
        .join(
            df_cliente,
            left_on="Monedero_Id",
            right_on="Cedula",
            how="left",
            suffix="_Cliente",
        )
        .join(
            df_ecommerce,
            left_on=["Monedero_Id", "Emp_Id"],
            right_on=["profile_idnumber", "Emp_Id"],
            how="left",
            suffix="_Ecommerce",
        )
        .select(
            pl.col("Monedero_Id"),
            pl.col("Monedero_Id").alias("Identidad_Limpia"),
            pl.col("Emp_Id"),
            pl.col("Monedero_Nombre").str.to_titlecase().alias("Nombre_Completo"),
            pl.col("Tipo_Plan"),
            pl.col("Identificacion"),
            pl.col("Identificacion_Formato"),
            pl.when(pl.col("Celular_Valido"))
            .then(pl.col("Celular"))
            .when(pl.col("Celular_Valido_Libros"))
            .then(pl.col("Celular_Libros"))
            .when(pl.col("Celular_Valido_Ecommerce"))
            .then(pl.col("Celular_Ecommerce"))
            .otherwise(pl.col("Celular"))
            .alias("Celular"),
            pl.when(pl.col("Telefono_Valido"))
            .then(pl.col("Telefono"))
            .when(pl.col("Telefono_Valido_Cliente"))
            .then(pl.col("Telefono_Cliente"))
            .when(pl.col("Telefono_Valido_Ecommerce"))
            .then(pl.col("Telefono_Ecommerce"))
            .otherwise(pl.col("Telefono")),
            pl.col("Nacimiento").alias("Fecha_Nacimiento"),
            pl.when(pl.col("Correo_Valido"))
            .then(pl.col("Correo"))
            .when(pl.col("Correo_Valido_Cliente"))
            .then(pl.col("Correo_Cliente"))
            .when(pl.col("Correo_Valido_Ecommerce"))
            .then(pl.col("Correo_Ecommerce"))
            .otherwise(pl.col("Correo")),
            pl.when(
                pl.col("Correo_Valido")
                | pl.col("Correo_Valido_Cliente")
                | pl.col("Correo_Valido_Ecommerce")
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .cast(pl.Boolean)
            .alias("Correo_Valido"),
            pl.col("Activo_Indicador").cast(pl.Boolean),
            pl.col("Acumula_Indicador").cast(pl.Boolean),
            pl.col("Principal_Indicador").cast(pl.Boolean),
            pl.col("Genero"),
            pl.col("Saldo_Puntos"),
            pl.col("Ingreso").alias("Fecha_Ingreso"),
            pl.col("MonederoTarj_Id_Original"),
            pl.col("UltimaCompra").alias("Fecha_UltimaCompra"),
            pl.col("Fecha_Creacion").alias("Fecha_Libro"),
            pl.coalesce([pl.col("Tipo_Nombre"), pl.lit("No Definido")]).alias(
                "Tipo_Cliente"
            ),
            pl.col("Medipack"),
            # pl.col("Hash_MonederoEmp").alias("Hash_IdentidadEmp"),
            pl.lit("Monedero").alias("OrigenRegistro"),
            pl.col("Nombre").alias("Primer_Nombre"),
            pl.col("Segundo_Nombre"),
            pl.col("Apellido").alias("Primer_Apellido"),
            pl.col("Segundo_Apellido"),
            pl.col("Departamento_Id").cast(pl.Int32),
            pl.col("Municipio_Id").cast(pl.Int32),
            pl.col("Ciudad_Id").cast(pl.Int32),
            pl.col("Barrio_Id").cast(pl.Int32),
            pl.col("Correo_Ecommerce"),
            pl.col("Correo_Valido_Ecommerce"),
            pl.when(
                pl.col("Celular_Valido")
                | pl.col("Celular_Valido_Libros")
                | pl.col("Celular_Valido_Ecommerce")
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Celular_Valido"),
            pl.when(
                pl.col("Telefono_Valido")
                | pl.col("Telefono_Valido_Libros")
                | pl.col("Telefono_Valido_Ecommerce")
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Telefono_Valido"),
            pl.col("Pais_Id"),
            pl.col("Pais_ISO2"),
        )
    )

    df_clientes2 = (
        df_cliente.with_columns(
            pl.col("Emp_Id").cast(pl.Int32),
            pl.col("Cliente_Nombre")
            .str.split_exact(" ", 4)
            .struct.rename_fields(
                [
                    "Primer_Nombre",
                    "Segundo_Nombre",
                    "Primer_Apellido",
                    "Segundo_Apellido",
                ]
            )
            .alias("Nombre_Struct"),
        )
        .unnest("Nombre_Struct")
        .join(
            df_clientes1.select(["Identidad_Limpia", "Emp_Id"]),
            left_on=["Cedula", "Emp_Id"],
            right_on=["Identidad_Limpia", "Emp_Id"],
            how="anti",
        )
        .join(
            df_libros_cliente,
            left_on=["Cedula", "Emp_Id"],
            right_on=["Identidad_Limpia", "Emp_Id"],
            how="left",
            suffix="_Libros",
        )
        .join(
            df_ecommerce,
            left_on=["Cedula", "Emp_Id"],
            right_on=["profile_idnumber", "Emp_Id"],
            how="left",
            suffix="_Ecommerce",
        )
        .select(
            pl.lit(None).alias("Monedero_Id"),
            pl.col("Cedula").alias("Identidad_Limpia"),
            pl.col("Emp_Id"),
            pl.col("Cliente_Nombre").alias("Nombre_Completo"),
            pl.lit(None).alias("Tipo_Plan"),
            pl.col("Cedula").alias("Identificacion"),
            pl.lit(None).alias("Identificacion_Formato"),
            pl.when(pl.col("Celular_Valido"))
            .then(pl.col("Celular"))
            .when(pl.col("Celular_Valido_Libros"))
            .then(pl.col("Celular_Libros"))
            .when(pl.col("Celular_Valido_Ecommerce"))
            .then(pl.col("Celular_Ecommerce"))
            .otherwise(pl.col("Celular"))
            .alias("Celular"),
            pl.when(pl.col("Telefono_Valido"))
            .then(pl.col("Telefono"))
            .when(pl.col("Telefono_Valido_Libros"))
            .then(pl.col("Telefono_Libros"))
            .when(pl.col("Telefono_Valido_Ecommerce"))
            .then(pl.col("Telefono_Ecommerce"))
            .otherwise(pl.col("Telefono"))
            .alias("Telefono"),
            pl.col("Fecha_Nacimiento"),
            pl.when(pl.col("Correo_Valido"))
            .then(pl.col("Correo"))
            .when(pl.col("Correo_Valido_Ecommerce"))
            .then(pl.col("Correo_Ecommerce"))
            .otherwise(pl.col("Correo")),
            pl.when(pl.col("Correo_Valido") | pl.col("Correo_Valido_Ecommerce"))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Correo_Valido"),
            pl.when(pl.col("Estado") == "ACTIVO")
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Activo_Indicador")
            .cast(pl.Boolean),
            pl.lit(0).alias("Acumula_Indicador").cast(pl.Boolean),
            pl.lit(None).alias("Principal_Indicador").cast(pl.Boolean),
            pl.col("Genero"),
            pl.lit(None).alias("Saldo_Puntos"),
            pl.lit(None).alias("Fecha_Ingreso"),
            pl.lit(None).alias("MonederoTarj_Id_Original"),
            pl.lit(None).alias("Fecha_UltimaCompra"),
            pl.col("Fecha_Creacion").alias("Fecha_Libro"),
            pl.col("Tipo_Cliente"),
            pl.col("Medipack"),
            # pl.col("Hash_ClienteEmp").alias("Hash_IdentidadEmp"),
            pl.lit("Cliente").alias("OrigenRegistro"),
            pl.col("Primer_Nombre"),
            pl.col("Segundo_Nombre"),
            pl.col("Primer_Apellido"),
            pl.col("Segundo_Apellido"),
            pl.lit(0).alias("Departamento_Id").cast(pl.Int32),
            pl.lit(0).alias("Municipio_Id").cast(pl.Int32),
            pl.lit(0).alias("Ciudad_Id").cast(pl.Int32),
            pl.lit(0).alias("Barrio_Id").cast(pl.Int32),
            pl.col("Correo_Ecommerce").alias("Correo_Ecommerce"),
            pl.col("Correo_Valido_Ecommerce"),
            pl.when(
                pl.col("Celular_Valido")
                | pl.col("Celular_Valido_Libros")
                | pl.col("Celular_Valido_Ecommerce")
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Celular_Valido"),
            pl.when(
                pl.col("Telefono_Valido")
                | pl.col("Telefono_Valido_Libros")
                | pl.col("Telefono_Valido_Ecommerce")
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Telefono_Valido"),
            pl.col("Pais_Id"),
            pl.col("Pais_ISO2"),
        )
    )

    df_clientes3 = (
        df_libros_cliente.with_columns(
            pl.col("Nombre")
            .str.split_exact(" ", 4)
            .struct.rename_fields(
                [
                    "Primer_Nombre",
                    "Segundo_Nombre",
                    "Primer_Apellido",
                    "Segundo_Apellido",
                ]
            )
            .alias("Nombre_Struct"),
        )
        .unnest("Nombre_Struct")
        .join(
            df_clientes2.select(["Identidad_Limpia", "Emp_Id"]),
            left_on=["Identidad_Limpia", "Emp_Id"],
            right_on=["Identidad_Limpia", "Emp_Id"],
            how="anti",
        )
        .join(df_libros_tipo, left_on="Tipo_Cliente", right_on="Tipo_Id", how="left")
        .join(
            df_ecommerce,
            left_on=["Identidad_Limpia", "Emp_Id"],
            right_on=["profile_idnumber", "Emp_Id"],
            how="left",
            suffix="_Ecommerce",
        )
        .select(
            pl.lit(None).alias("Monedero_Id"),
            pl.col("Identidad_Limpia"),
            pl.col("Pais_Id").alias("Emp_Id"),
            pl.col("Nombre").alias("Nombre_Completo"),
            pl.lit(None).alias("Tipo_Plan"),
            pl.col("Identidad").alias("Identificacion"),
            pl.lit(None).alias("Identificacion_Formato"),
            pl.when(pl.col("Celular_Valido"))
            .then(pl.col("Celular"))
            .when(pl.col("Celular_Valido_Ecommerce"))
            .then(pl.col("Celular_Ecommerce"))
            .otherwise(pl.col("Celular"))
            .alias("Celular"),
            pl.when(pl.col("Telefono_Valido"))
            .then(pl.col("Telefono"))
            .when(pl.col("Telefono_Valido_Ecommerce"))
            .then(pl.col("Telefono_Ecommerce"))
            .otherwise(pl.col("Telefono"))
            .alias("Telefono"),
            pl.col("Fecha_Nacimiento"),
            pl.col("Correo").alias("Correo"),
            pl.col("Correo_Valido").alias("Correo_Valido"),
            pl.lit(None).alias("Activo_Indicador").cast(pl.Boolean),
            pl.lit(0).alias("Acumula_Indicador").cast(pl.Boolean),
            pl.lit(None).alias("Principal_Indicador").cast(pl.Boolean),
            pl.col("Genero"),
            pl.lit(None).alias("Saldo_Puntos"),
            pl.lit(None).alias("Fecha_Ingreso"),
            pl.lit(None).alias("MonederoTarj_Id_Original"),
            pl.lit(None).alias("Fecha_UltimaCompra"),
            pl.col("Fecha_Creacion").alias("Fecha_Libro"),
            pl.col("Tipo_Nombre").alias("Tipo_Cliente"),
            pl.col("Medipack"),
            # pl.hash(pl.concat_str(["Identidad_Limpia", "Pais_Id"])).alias("Hash_IdentidadEmp"),
            pl.lit("Libros").alias("OrigenRegistro"),
            pl.col("Primer_Nombre"),
            pl.col("Segundo_Nombre"),
            pl.col("Primer_Apellido"),
            pl.col("Segundo_Apellido"),
            pl.col("Departamento_Id"),
            pl.col("Municipio_Id"),
            pl.lit(0).alias("Ciudad_Id").cast(pl.Int32),
            pl.lit(0).alias("Barrio_Id").cast(pl.Int32),
            pl.col("Correo").alias("Correo_Ecommerce"),
            pl.col("Correo_Valido").alias("Correo_Valido_Ecommerce"),
            pl.when(pl.col("Celular_Valido") | pl.col("Celular_Valido_Ecommerce"))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Celular_Valido"),
            pl.when(pl.col("Telefono_Valido") | pl.col("Telefono_Valido_Ecommerce"))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Telefono_Valido"),
            pl.col("Pais_Id"),
            pl.col("Pais_ISO2"),
        )
    )

    df_clientes4 = (
        df_ecommerce.with_columns(
            pl.col("Emp_Id").cast(pl.Int32),
            pl.col("profile_fullname")
            .str.split_exact(" ", 4)
            .struct.rename_fields(
                [
                    "Primer_Nombre",
                    "Segundo_Nombre",
                    "Primer_Apellido",
                    "Segundo_Apellido",
                ]
            )
            .alias("Nombre_Struct"),
        )
        .unnest("Nombre_Struct")
        .join(
            df_clientes3.select(["Identidad_Limpia", "Emp_Id"]),
            left_on=["profile_idnumber", "Emp_Id"],
            right_on=["Identidad_Limpia", "Emp_Id"],
            how="anti",
        )
        .select(
            pl.lit(None).alias("Monedero_Id"),
            pl.col("profile_idnumber").alias("Identidad_Limpia"),
            pl.col("Emp_Id"),
            pl.col("profile_fullname").alias("Nombre_Completo"),
            pl.lit(None).alias("Tipo_Plan"),
            pl.col("profile_idnumber").alias("Identificacion"),
            pl.lit(None).alias("Identificacion_Formato"),
            pl.col("profile_telephone").alias("Celular"),
            pl.col("profile_telephone").alias("Telefono"),
            pl.col("profile_birthday_date").alias("Fecha_Nacimiento"),
            pl.col("Correo"),
            pl.col("Correo_Valido"),
            pl.lit(None).alias("Activo_Indicador").cast(pl.Boolean),
            pl.lit(0).alias("Acumula_Indicador").cast(pl.Boolean),
            pl.lit(None).alias("Principal_Indicador").cast(pl.Boolean),
            pl.col("Genero"),
            pl.lit(None).alias("Saldo_Puntos"),
            pl.lit(None).alias("Fecha_Ingreso"),
            pl.lit(None).alias("MonederoTarj_Id_Original"),
            pl.lit(None).alias("Fecha_UltimaCompra"),
            pl.lit(None).alias("Fecha_Libro"),
            pl.lit(None).alias("Tipo_Cliente"),
            pl.lit(None).alias("Medipack"),
            # pl.(pl.concat_str(["profile_idnumber", "Emp_Id"])).alias("Hash_IdentidadEmp"),
            pl.lit("Ecommerce").alias("OrigenRegistro"),
            pl.col("Primer_Nombre"),
            pl.col("Segundo_Nombre"),
            pl.col("Primer_Apellido"),
            pl.col("Segundo_Apellido"),
            pl.lit(0).alias("Departamento_Id").cast(pl.Int32),
            pl.lit(0).alias("Municipio_Id").cast(pl.Int32),
            pl.lit(0).alias("Ciudad_Id").cast(pl.Int32),
            pl.lit(0).alias("Barrio_Id").cast(pl.Int32),
            pl.col("Correo").alias("Correo_Ecommerce"),
            pl.col("Correo_Valido").alias("Correo_Valido_Ecommerce"),
            pl.col("Celular_Valido"),
            pl.col("Telefono_Valido"),
            pl.col("Pais_Id"),
            pl.col("Pais_ISO2"),
        )
    )

    # Union the DataFrames
    df_clientes_final = df_clientes1.vstack(df_clientes2).vstack(df_clientes3).vstack(df_clientes4)
    df_clientes_final = (
        df_clientes_final.unique(subset=["Identidad_Limpia", "Emp_Id"])
        .drop_nulls(subset=["Identidad_Limpia", "Emp_Id"])
        .with_columns(
            pl.lit(datetime.now()).alias("Fecha_Actualizado"),
            pl.col("Primer_Nombre").str.to_titlecase(),
            pl.col("Segundo_Nombre").str.to_titlecase(),
            pl.col("Primer_Apellido").str.to_titlecase(),
            pl.col("Segundo_Apellido").str.to_titlecase(),
            pl.col("Nombre_Completo").str.to_titlecase(),
        )
    )
    
    return df_clientes_final


@op(
    ins={BI_Kielsa_Dim_Pais.to_python_identifier(): In(Nothing)},
    out=Out(pl.DataFrame, io_manager_key="polars_parquet_io_manager"),
)
def get_phone_number_valid_df_pattern(
    dwh_farinter_bi: SQLServerResource,
) -> pl.DataFrame:
    """
    Returns True if the given phone number matches the pattern for the specified country.

    Supported countries (with patterns):
    - Honduras (HN): +504XXXXXXXX or 504XXXXXXXX or 8 digits starting with 2, 3, 7, 8, or 9
    - Guatemala (GT): +502XXXXXXXX or 502XXXXXXXX or 8 digits starting with 2, 3, 4, 5, 6, or 7
    - Nicaragua (NI): +505XXXXXXXX or 505XXXXXXXX or 8 digits starting with 2, 3, 5, 7, or 8
    - Costa Rica (CR): +506XXXXXXXX or 506XXXXXXXX or 8 digits starting with 2, 4, 5, 6, 7, or 8
    - El Salvador (SV): +503XXXXXXXX or 503XXXXXXXX or 8 digits starting with 6 or 7
    - Panama (PA): +507XXXXXXXX or 507XXXXXXXX or 8 digits starting with 2 or 6

    Args:
    phone_number (str): The phone number to validate.
    iso2_country_code (str): The ISO2 country code for validation.

    Returns:
    bool: True if the phone number is valid for the specified country, False otherwise.
    """
    # Define the SQL query to select Empresas data from the database
    sql_query = """
        SELECT --TOP (1000) 
            P.[Pais_Id]
            ,P.Pais_ISO2
        FROM [BI_FARINTER].[dbo].[BI_Dim_Pais] P
    """

    # Execute the SQL query and store the result in a Polars DataFrame
    df_paises = pl.read_database(
        sql_query, dwh_farinter_bi.get_arrow_odbc_conn_string()
    )

    patterns = {
        "HN": r"^(?:\+?504[-\s]?[23789]\d{7}|[23789]\d{7})$",
        "GT": r"^(?:\+?502[-\s]?\d{8}|[234567]\d{7})$",
        "NI": r"^(?:\+?505[-\s]?\d{8}|[23578]\d{7})$",
        "CR": r"^(?:\+?506[-\s]?\d{8}|[245678]\d{7})$",
        "SV": r"^(?:\+?503[-\s]?\d{8}|[67]\d{7})$",
        "PA": r"^(?:\+?507[-\s]?\d{8}|[26]\d{7})$",
    }

    # convert patterns to polars and add to df_paises to each country on new columna patterns
    patterns = pl.DataFrame(
        {"Pais_ISO2": list(patterns.keys()), "pattern": list(patterns.values())}
    )

    return df_paises.join(patterns, on="Pais_ISO2", how="inner").select(
        pl.col("Pais_Id"),
        pl.col("Pais_ISO2"),
        pl.col("pattern"),
    )

@op(
    out={"file_path": Out(str)},
)
def create_file_on_smb(
    smb_resource_staging_dagster_dwh: SMBResource, df_clientes: pl.DataFrame
) -> str:  # tuple[str, str]:
    smbr = smb_resource_staging_dagster_dwh
    file_path = smbr.get_full_server_path("\\staging_dagster\\kielsa_clientes.csv")
    # format_file_path = smbr.get_full_server_path("\\staging_dagster\\kielsa_clientes.fmt")

    # Write the CSV file
    with smbr.open_server_file(file_path, mode="w") as f:
        df_clientes.cast({pl.Boolean: pl.Int8}).write_csv(
            f,
            include_bom=False,
            include_header=False,
            separator=",",
            line_terminator="\r\n",  # SQL Server requires CRLF line endings
            quote_char='"',
            quote_style="necessary",
        )

    # Create the format file for SQL Server BULK INSERT
    # format_file_content = f"13.0\n{len(df_clientes.columns)}\n"

    # for index, column in enumerate(df_clientes.columns, start=1):
    #     terminator = "," if index < len(df_clientes.columns) else "\\r\\n"
    #     format_file_content += f"{index}       SQLNCHAR             0       0       \"{terminator}\"       {index}       {column}       \"\"\n"

    # with smbr.open_server_file(format_file_path, mode="w") as f:
    #     f.write(format_file_content)

    return str(file_path)  # , str(format_file_path)


@op(
    ins={"file_path": In(str)},
)
def bulk_load_to_sql_server(
    dwh_farinter_bi: SQLServerResource, file_path: str, df_clientes: pl.DataFrame
) -> None:
    sg = SQLScriptGenerator(
        primary_keys=("Identidad_Limpia", "Emp_Id"),
        db_schema="dbo",
        table_name="BI_Kielsa_Dim_ClienteGeneral",
        temp_table_name="BI_Kielsa_Dim_ClienteGeneral_NEW",
        df=df_clientes,
    )
    # row_terminator = "\r\n"
    format_file_path = ""
    # # Print the first few lines of the CSV file for debugging
    # with open(file_path, "r", encoding="utf-8") as f:
    #     for _ in range(5):
    #         print(f.readline().strip()[:1000])

    sql_script = f"""
        SET XACT_ABORT ON;
        SET NOCOUNT ON;

        BEGIN TRY

            -- Drop the view if it exists
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES t
                WHERE t.TABLE_NAME = '{sg.table_name}' and t.TABLE_SCHEMA = '{sg.db_schema}' 
                and t.TABLE_TYPE = 'VIEW')
                DROP VIEW [{sg.db_schema}].[{sg.table_name}];

            -- Drop the NEW table if it exists
            DROP TABLE IF EXISTS {sg.schema_temp_table_relation};

            -- Drop the old table
            DROP TABLE IF EXISTS [{sg.db_schema}].[{sg.table_name}_OLD];

            -- Create a new table with the same structure as the existing one
            {sg.create_table_sql_script()}
            
            -- Bulk load the data into the new table
            BULK INSERT {sg.schema_temp_table_relation}
            FROM '{file_path}'
            WITH (
                CODEPAGE = '65001', -- UTF-8
                FORMAT = 'CSV',
                --DATAFILETYPE = 'char',
                --FIRSTROW = 2,
                TABLOCK,
                --ROWTERMINATOR = '\\r\\n',
                --FIELDTERMINATOR = ',',
                --FIELDQUOTE = '"',
                --BATCHSIZE = 1048576, -- 1MB
                --ROWS_PER_BATCH = 1048576, -- 1MB
                MAXERRORS = 0 --,
                --ORDER (key_column_1, key_column_2) -- Replace with actual key columns
                --FORMATFILE = '{format_file_path}'
                --ERRORFILE = '{file_path.replace('.csv', '_error.csv') }'
            );

            -- Convert the new table to columnstore
            {sg.columnstore_table_sql_script()}

            -- Add primary key
            {sg.primary_key_table_sql_script()}
            
            -- Swap the tables
            BEGIN TRANSACTION;
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES t
                WHERE t.TABLE_NAME = '{sg.table_name}' and t.TABLE_SCHEMA = '{sg.db_schema}')
                EXEC sp_rename '{sg.db_schema}.{sg.table_name}', '{sg.table_name}_OLD';
            EXEC sp_rename '{sg.schema_temp_table_relation}', '{sg.table_name}';
            COMMIT TRANSACTION;
            --SELECT TOP 0 * INTO [{sg.db_schema}].[{sg.table_name}_OLD]
            --FROM [{sg.db_schema}].[{sg.table_name}] WITH (NOLOCK);

            -- Convert the OLD table to columnstore
            --CREATE CLUSTERED COLUMNSTORE INDEX cci_dl_kielsa_clientegeneral_OLD ON {sg.table_name}_OLD;

            --ALTER TABLE [{sg.db_schema}].[{sg.table_name}] SWITCH TO [{sg.db_schema}].[{sg.table_name}]_OLD 
            --    WITH ( WAIT_AT_LOW_PRIORITY ( MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = BLOCKERS )); 
            --ALTER TABLE {sg.schema_temp_table_relation} SWITCH TO [{sg.db_schema}].[{sg.table_name}];

            -- Drop the old table
            DROP TABLE IF EXISTS [{sg.db_schema}].[{sg.table_name}_OLD];

            -- Drop the NEW table
            DROP TABLE IF EXISTS {sg.schema_temp_table_relation};

        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
            THROW;
        END CATCH;
    """
    # print(sql_script)
    with dwh_farinter_bi.get_sqlalchemy_conn(autocommit=True) as conn:
        # df_clientes.limit(100).write_database(table_name=f"{table_name}_dagster_temp_base", connection=conn, if_table_exists='replace')
        dwh_farinter_bi.execute_and_commit(sql_script, connection=conn)
    # dwh_farinter_dl.execute_and_commit(sql_script, engine="pyodbc") #Allows to execute the query without service account delegation


@graph(
    ins={
        DL_MDBECOMM_Usuarios.to_python_identifier(): GraphIn(None),
        DL_Kielsa_Monedero.to_python_identifier(): GraphIn(None),
        DL_Kielsa_Libros_Cliente.to_python_identifier(): GraphIn(None),
        DL_Kielsa_Libros_Tipo.to_python_identifier(): GraphIn(None),
        DL_Kielsa_Cliente.to_python_identifier(): GraphIn(None),
        BI_Kielsa_Dim_Empresa.to_python_identifier(): GraphIn(None),
        BI_Kielsa_Dim_Pais.to_python_identifier(): GraphIn(None),
    }
)
def BI_Kielsa_Dim_ClienteGeneral_graph(**kwargs):
    df_ecommerce = get_df_ecommerce(kwargs[DL_MDBECOMM_Usuarios.to_python_identifier()])
    df_clientes = get_df_clientes(kwargs[DL_Kielsa_Cliente.to_python_identifier()])
    df_monederos = get_df_monederos(kwargs[DL_Kielsa_Monedero.to_python_identifier()])
    df_libros_cliente = get_df_libros_cliente(
        kwargs[DL_Kielsa_Libros_Cliente.to_python_identifier()]
    )
    df_libros_tipo = get_df_libros_tipo(
        kwargs[DL_Kielsa_Libros_Tipo.to_python_identifier()]
    )
    df_empresas = get_df_empresas(kwargs[BI_Kielsa_Dim_Empresa.to_python_identifier()])
    df_paises_patterns = get_phone_number_valid_df_pattern(
        kwargs[BI_Kielsa_Dim_Pais.to_python_identifier()]
    )

    df_clientes_finales = process_dfs_clientes(
        df_ecommerce=df_ecommerce,
        df_cliente=df_clientes,
        df_monedero=df_monederos,
        df_libros_cliente=df_libros_cliente,
        df_libros_tipo=df_libros_tipo,
        df_empresas=df_empresas,
        df_paises_patterns=df_paises_patterns,
    )
    filepath = create_file_on_smb(df_clientes=df_clientes_finales)

    return bulk_load_to_sql_server(
        file_path=filepath, df_clientes=df_clientes_finales
    )


BI_Kielsa_Dim_ClienteGeneral = AssetsDefinition.from_graph(
    graph_def=BI_Kielsa_Dim_ClienteGeneral_graph,
    # kinds=("sql_server", "polars", "smb"),
    keys_by_output_name={
        "result": AssetKey(["BI_FARINTER", "dbo", "BI_Kielsa_Dim_ClienteGeneral"])
    },
    tags_by_output_name={
        "result": tags_repo.Daily.tag
        | tags_repo.UniquePeriod.tag
        | {f"dagster/kind/{kind}": "" for kind in ("sql_server", "polars", "smb")}
    },
    automation_conditions_by_output_name={"result": automation_daily_delta_2_cron},
    keys_by_input_name={
        DL_MDBECOMM_Usuarios.to_python_identifier(): DL_MDBECOMM_Usuarios,
        DL_Kielsa_Monedero.to_python_identifier(): DL_Kielsa_Monedero,
        DL_Kielsa_Libros_Cliente.to_python_identifier(): DL_Kielsa_Libros_Cliente,
        DL_Kielsa_Libros_Tipo.to_python_identifier(): DL_Kielsa_Libros_Tipo,
        DL_Kielsa_Cliente.to_python_identifier(): DL_Kielsa_Cliente,
        BI_Kielsa_Dim_Empresa.to_python_identifier(): BI_Kielsa_Dim_Empresa,
        BI_Kielsa_Dim_Pais.to_python_identifier(): BI_Kielsa_Dim_Pais,
    },
)


if __name__ == "__main__":

    class TestPhoneNumberValidation(unittest.TestCase):
        def test_honduras_phone_numbers(self):
            # Get the validation pattern for Honduras
            pattern = (
                get_phone_number_valid_df_pattern(dwh_farinter_bi=dwh_farinter_bi)
                .filter(pl.col("Pais_Id") == 1)
                .get_column("pattern")
                .item()
            )

            # Valid phone numbers
            valid_numbers = [
                "+50498765432",  # with +504 and 8 digits
                "50498765432",  # with 504 and 8 digits
                "98765432",  # starting with 9
                "28765432",  # starting with 2
                "38765432",  # starting with 3
                "78765432",  # starting with 7
                "88765432",  # starting with 8
            ]

            valid_df = pl.DataFrame({"phone_number": valid_numbers})
            valid_df = valid_df.with_columns(
                pl.col("phone_number").str.contains(pattern).alias("is_valid")
            )

            pltest.assert_series_equal(
                valid_df["is_valid"],
                pl.Series([True] * len(valid_numbers)).alias("is_valid"),
            )

            # Invalid phone numbers
            invalid_numbers = [
                "+50418765432",  # starting with 1 (invalid)
                "50408765432",  # starting with 0 (invalid)
                "18765432",  # starting with 1 (invalid)
                "08765432",  # starting with 0 (invalid)
                "+50598765432",  # wrong country code +505 (Nicaragua)
                "+504987654321",  # too many digits
                "987654321",  # too many digits without code
            ]

            invalid_df = pl.DataFrame({"phone_number": invalid_numbers})
            invalid_df = invalid_df.with_columns(
                pl.col("phone_number").str.contains(pattern).alias("is_valid")
            )

            pltest.assert_series_equal(
                invalid_df["is_valid"],
                pl.Series([False] * len(invalid_numbers)).alias("is_valid"),
            )

    start_time = datetime.now()
    with instance_for_test() as instance:
        from dagster import ResourceDefinition
        if env_str == "local":
            warnings.warn("Running in local mode, using top 1000 rows")
        @asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1

        mock_dwh_farinter_bi = ResourceDefinition.mock_resource()

        result = materialize(
            assets=[mock_between_asset, BI_Kielsa_Dim_ClienteGeneral],
            instance=instance,
            resources={
                "dwh_farinter_dl": dwh_farinter_dl,
                "dwh_farinter_bi": mock_dwh_farinter_bi,
                "smb_resource_staging_dagster_dwh": smb_resource_staging_dagster_dwh,
                "polars_parquet_io_manager": PolarsParquetIOManager(),
            },
        )
        print(result.output_for_node(BI_Kielsa_Dim_ClienteGeneral.node_def.name))

    end_time = datetime.now()
    print(
        f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}"
    )

    unittest.main()

all_assets = load_assets_from_current_module()

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(
        all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"
    ),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)

all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = (
    build_last_update_freshness_checks(
        assets=filter_assets_by_tags(
            all_assets,
            tags_to_match=tags_repo.Hourly.tag,
            filter_type="any_tag_matches",
        ),
        lower_bound_delta=timedelta(hours=13),
        deadline_cron="0 10-16 * * 1-6",
    )
)

all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
# all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
