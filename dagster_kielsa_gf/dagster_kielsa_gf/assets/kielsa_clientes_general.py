from datetime import datetime
import unittest
import polars as pl
import polars.testing as pltest
from dagster_shared_gf.resources.sql_server_resources import (
    dwh_farinter_dl,
    dwh_farinter_bi,
    SQLServerResource,
)
from dagster import  AssetChecksDefinition, load_asset_checks_from_current_module, load_assets_from_current_module, materialize, asset, op, graph_asset, OpExecutionContext, MaterializeResult, instance_for_test

@op
def get_df_ecommerce(
    dwh_farinter_dl: SQLServerResource
) -> pl.DataFrame:
    # Define the SQL query to select data from the database
    sql_query = """
    SELECT 
        profile_idnumber,
        1 as Emp_Id,
        profile_fullname,
        profile_telephone,
        profile_telephone,
        profile_birthday_date,
        email_principal,
        profile_gender
    FROM (
        SELECT [_id]
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
    ).with_columns(pl.col("Emp_Id").cast(pl.Int32))

    return df_ecommerce


@op
def get_df_monederos(
    dwh_farinter_dl: SQLServerResource
) -> pl.DataFrame:
    # Define the SQL query to select data from the database
    sql_query = """
        SELECT --top 100 
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
    ).with_columns(pl.col("Emp_Id").cast(pl.Int32))

    return df_monedero


@op
def get_df_libros_cliente(
    dwh_farinter_dl: SQLServerResource
) -> pl.DataFrame:
    # Define the SQL query to select data from the database
    sql_query = """
        SELECT --top 100 
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
        pl.col("Tipo_Cliente").cast(pl.Int32), pl.col("Pais_Id").cast(pl.Int32)
    )

    return df_libros_cliente


@op
def get_df_libros_tipo(
    dwh_farinter_dl: SQLServerResource
) -> pl.DataFrame:
    # Define the SQL query to select data from the database
    sql_query = """
        SELECT --top 100
                * 
        FROM [DL_FARINTER].[dbo].[DL_Kielsa_Libros_Tipo]
    """

    # Execute the SQL query and store the result in a Polars DataFrame
    df_libros_tipo = pl.read_database(
        sql_query, dwh_farinter_dl.get_arrow_odbc_conn_string()
    ).with_columns(pl.col("Tipo_Id").cast(pl.Int32))

    return df_libros_tipo


@op
def get_df_clientes(
    dwh_farinter_dl: SQLServerResource
) -> pl.DataFrame:
    # Define the SQL query to select data from the database
    sql_query = """
        SELECT --top 100
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
    ).with_columns(pl.col("Emp_Id").cast(pl.Int32))
    print(df_cliente.columns)

    return df_cliente


@op
def get_df_empresas(
    dwh_farinter_bi: SQLServerResource
) -> pl.DataFrame:
    sql_query = """
        SELECT --TOP (1000) 
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


@op
def process_dfs_clientes(
    context: OpExecutionContext,
    df_monedero: pl.DataFrame,
    df_libros_cliente: pl.DataFrame,
    df_libros_tipo: pl.DataFrame,
    df_ecommerce: pl.DataFrame,
    df_cliente: pl.DataFrame,
    df_empresas: pl.DataFrame,
) -> pl.DataFrame:
    # Process the dataframes
    df_clientes1 = (
        df_monedero.with_columns(pl.col("Emp_Id").cast(pl.Int32))
        .join(
            df_libros_cliente,
            left_on=["Monedero_Id", "Emp_Id"],
            right_on=["Identidad_Limpia", "Pais_Id"],
            how="left",
        )
        .join(df_libros_tipo, left_on="Tipo_Cliente", right_on="Tipo_Id", how="left")
        .join(
            df_ecommerce,
            left_on=["Monedero_Id", "Emp_Id"],
            right_on=["profile_idnumber", "Emp_Id"],
            how="left",
        )
        .select(
            pl.col("Monedero_Id"),
            pl.col("Monedero_Id").alias("Identidad_Limpia"),
            pl.col("Emp_Id"),
            pl.col("Monedero_Nombre").alias("Nombre_Completo"),
            pl.col("Tipo_Plan"),
            pl.col("Identificacion"),
            pl.col("Identificacion_Formato"),
            pl.coalesce(
                [pl.col("Telefono"), pl.col("profile_telephone"), pl.lit("")]
            ).alias("Telefono"),
            pl.col("Celular"),
            pl.col("Nacimiento").alias("Fecha_Nacimiento"),
            pl.coalesce(
                [pl.col("Correo"), pl.col("email_principal"), pl.lit("")]
            ).alias("Correo"),
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
            pl.col("email_principal").alias("Correo_Ecommerce"),
        )
    )

    df_clientes2 = (
        df_cliente.with_columns(pl.col("Emp_Id").cast(pl.Int32))
        .join(
            df_clientes1.select(["Identidad_Limpia", "Emp_Id"]),
            left_on=["Cedula", "Emp_Id"],
            right_on=["Identidad_Limpia", "Emp_Id"],
            how="anti",
        )
        .join(
            df_libros_cliente,
            left_on=["Cedula", "Emp_Id"],
            right_on=["Identidad_Limpia", "Pais_Id"],
            how="left",
        )
        .select(
            pl.lit(None).alias("Monedero_Id"),
            pl.col("Cedula").alias("Identidad_Limpia"),
            pl.col("Emp_Id"),
            pl.col("Cliente_Nombre").alias("Nombre_Completo"),
            pl.lit(None).alias("Tipo_Plan"),
            pl.col("Cedula").alias("Identificacion"),
            pl.lit(None).alias("Identificacion_Formato"),
            pl.col("Telefono"),
            pl.col("Telefono").alias("Celular"),
            pl.col("Fecha_Nacimiento"),
            pl.col("Correo"),
            pl.when(pl.col("Estado") == "ACTIVO")
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("Activo_Indicador")
            .cast(pl.Boolean),
            pl.lit(0).alias("Acumula_Indicador").cast(pl.Boolean),
            pl.lit(None).alias("Principal_Indicador").cast(pl.Boolean),
            pl.lit(None).alias("Genero"),
            pl.lit(None).alias("Saldo_Puntos"),
            pl.lit(None).alias("Fecha_Ingreso"),
            pl.lit(None).alias("MonederoTarj_Id_Original"),
            pl.lit(None).alias("Fecha_UltimaCompra"),
            pl.col("Fecha_Creacion").alias("Fecha_Libro"),
            pl.col("Tipo_Cliente"),
            pl.col("Medipack"),
            # pl.col("Hash_ClienteEmp").alias("Hash_IdentidadEmp"),
            pl.lit("Cliente").alias("OrigenRegistro"),
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
            pl.lit(0).alias("Departamento_Id").cast(pl.Int32),
            pl.lit(0).alias("Municipio_Id").cast(pl.Int32),
            pl.lit(0).alias("Ciudad_Id").cast(pl.Int32),
            pl.lit(0).alias("Barrio_Id").cast(pl.Int32),
            pl.lit(None).alias("Correo_Ecommerce"),
        )
        .unnest("Nombre_Struct")
    )

    df_clientes3 = (
        df_libros_cliente.with_columns(pl.col("Pais_Id").cast(pl.Int32))
        .join(
            df_clientes2.select(["Identidad_Limpia", "Emp_Id"]),
            left_on=["Identidad_Limpia", "Pais_Id"],
            right_on=["Identidad_Limpia", "Emp_Id"],
            how="anti",
        )
        .join(df_libros_tipo, left_on="Tipo_Cliente", right_on="Tipo_Id", how="left")
        .select(
            pl.lit(None).alias("Monedero_Id"),
            pl.col("Identidad_Limpia"),
            pl.col("Pais_Id").alias("Emp_Id"),
            pl.col("Nombre").alias("Nombre_Completo"),
            pl.lit(None).alias("Tipo_Plan"),
            pl.col("Identidad").alias("Identificacion"),
            pl.lit(None).alias("Identificacion_Formato"),
            pl.col("Telefono"),
            pl.col("Telefono").alias("Celular"),
            pl.col("Fecha_Nacimiento"),
            pl.lit(None).alias("Correo"),
            pl.lit(None).alias("Activo_Indicador").cast(pl.Boolean),
            pl.lit(0).alias("Acumula_Indicador").cast(pl.Boolean),
            pl.lit(None).alias("Principal_Indicador").cast(pl.Boolean),
            pl.lit(None).alias("Genero"),
            pl.lit(None).alias("Saldo_Puntos"),
            pl.lit(None).alias("Fecha_Ingreso"),
            pl.lit(None).alias("MonederoTarj_Id_Original"),
            pl.lit(None).alias("Fecha_UltimaCompra"),
            pl.col("Fecha_Creacion").alias("Fecha_Libro"),
            pl.col("Tipo_Nombre").alias("Tipo_Cliente"),
            pl.col("Medipack"),
            # pl.hash(pl.concat_str(["Identidad_Limpia", "Pais_Id"])).alias("Hash_IdentidadEmp"),
            pl.lit("Libros").alias("OrigenRegistro"),
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
            pl.col("Departamento_Id"),
            pl.col("Municipio_Id"),
            pl.lit(0).alias("Ciudad_Id").cast(pl.Int32),
            pl.lit(0).alias("Barrio_Id").cast(pl.Int32),
            pl.lit(None).alias("Correo_Ecommerce"),
        )
        .unnest("Nombre_Struct")
    )

    df_clientes4 = (
        df_ecommerce.with_columns(pl.col("Emp_Id").cast(pl.Int32))
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
            pl.col("profile_telephone").alias("Telefono"),
            pl.col("profile_telephone").alias("Celular"),
            pl.col("profile_birthday_date").alias("Fecha_Nacimiento"),
            pl.col("email_principal").alias("Correo"),
            pl.lit(None).alias("Activo_Indicador").cast(pl.Boolean),
            pl.lit(0).alias("Acumula_Indicador").cast(pl.Boolean),
            pl.lit(None).alias("Principal_Indicador").cast(pl.Boolean),
            pl.when(pl.col("profile_gender") == "mujer")
            .then(pl.lit("M"))
            .when(pl.col("profile_gender") == "hombre")
            .then(pl.lit("H"))
            .otherwise(pl.lit("N"))
            .alias("Genero"),
            pl.lit(None).alias("Saldo_Puntos"),
            pl.lit(None).alias("Fecha_Ingreso"),
            pl.lit(None).alias("MonederoTarj_Id_Original"),
            pl.lit(None).alias("Fecha_UltimaCompra"),
            pl.lit(None).alias("Fecha_Libro"),
            pl.lit(None).alias("Tipo_Cliente"),
            pl.lit(None).alias("Medipack"),
            # pl.(pl.concat_str(["profile_idnumber", "Emp_Id"])).alias("Hash_IdentidadEmp"),
            pl.lit("Ecommerce").alias("OrigenRegistro"),
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
            pl.lit(0).alias("Departamento_Id").cast(pl.Int32),
            pl.lit(0).alias("Municipio_Id").cast(pl.Int32),
            pl.lit(0).alias("Ciudad_Id").cast(pl.Int32),
            pl.lit(0).alias("Barrio_Id").cast(pl.Int32),
            pl.col("email_principal").alias("Correo_Ecommerce"),
        )
        .unnest("Nombre_Struct")
    )

    # Union the DataFrames
    return (
        df_clientes1.vstack(df_clientes2)
        .vstack(df_clientes3)
        .vstack(df_clientes4)
        .join(df_empresas, left_on="Emp_Id", right_on="Emp_Id", how="inner")
    )

    # Adding final transformations 'Fecha_Actualizado' and a column to indicate if the phone number is valid



@op
def get_phone_number_valid_df_pattern(dwh_farinter_bi: SQLServerResource) -> pl.DataFrame:
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
    df_paises = pl.read_database(sql_query, dwh_farinter_bi.get_arrow_odbc_conn_string())

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

@op #(out={"df_clientes": Out(pl.DataFrame), "filas": Out(int)})
def correciones_clientes(df_clientes: pl.DataFrame, df_paises_patterns: pl.DataFrame) -> pl.DataFrame: #tuple[pl.DataFrame, int]:
    df_pv_pattern = df_paises_patterns

    df_clientes = (
        df_clientes
        .join(df_pv_pattern, on="Pais_Id", how="left")
        .with_columns(
            pl.lit(datetime.now()).alias("Fecha_Actualizado"),
            pl.col("Primer_Nombre").str.to_titlecase(),
            pl.col("Segundo_Nombre").str.to_titlecase(),
            pl.col("Primer_Apellido").str.to_titlecase(),
            pl.col("Segundo_Apellido").str.to_titlecase(),
            pl.col("Nombre_Completo").str.to_titlecase(),
            pl.col("Telefono")
            .str.contains(pl.col("pattern"))
            .alias("Telefono_Valido")
            .cast(pl.Boolean),
            pl.col("Celular")
            .str.contains(pl.col("pattern"))
            .alias("Celular_Valido")
            .cast(pl.Boolean),
        )
        .drop(["pattern"])
    )

    return df_clientes
    #return  df_clientes, len(df_clientes)

@op
def escribir_clientes(context: OpExecutionContext, df_clientes: pl.DataFrame, dwh_farinter_dl: SQLServerResource) -> int:
    # Write the result to a table
    with pl.Config() as cfg:
        cfg.set_tbl_cols(20)
        context.log.debug(df_clientes.head(15))

    with dwh_farinter_dl.get_sqlalchemy_conn() as conn:
        df_clientes.write_database(table_name="DL_Kielsa_ClienteGeneral", connection=conn, if_table_exists='replace')

    return len(df_clientes)


@graph_asset
def BI_Kielsa_Dim_ClienteGeneral() -> MaterializeResult:
    df_ecommerce = get_df_ecommerce()
    df_clientes = get_df_clientes()
    df_monederos = get_df_monederos()
    df_libros_cliente = get_df_libros_cliente()
    df_libros_tipo = get_df_libros_tipo()
    df_empresas = get_df_empresas()
    df_paises_patterns = get_phone_number_valid_df_pattern()

    df_clientes_unidos = process_dfs_clientes(
        df_ecommerce=df_ecommerce,
        df_cliente=df_clientes,
        df_monedero=df_monederos,
        df_libros_cliente=df_libros_cliente,
        df_libros_tipo=df_libros_tipo,
        df_empresas=df_empresas,
    )
    df_clientes_corregidos = correciones_clientes(
        df_clientes=df_clientes_unidos, df_paises_patterns=df_paises_patterns
    )
    filas = escribir_clientes(df_clientes_corregidos)

    return {"result":  filas}


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
        @asset(name="between_asset")
        def mock_between_asset() -> int:
            return 1
        result = materialize(assets=[mock_between_asset, BI_Kielsa_Dim_ClienteGeneral], instance=instance, resources={"dwh_farinter_dl": dwh_farinter_dl, "dwh_farinter_bi": dwh_farinter_bi})
        print(result.output_for_node("BI_Kielsa_Dim_ClienteGeneral"))

    end_time = datetime.now()
    print(f"Tiempo de ejecución: {end_time - start_time}, desde {start_time}, hasta {end_time}")

    unittest.main()

all_assets = load_assets_from_current_module()

# all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
#     assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
#     lower_bound_delta=timedelta(hours=26),
#     deadline_cron="0 9 * * 1-6",
# )
# print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
# all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
#     assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
#     lower_bound_delta=timedelta(hours=13),
#     deadline_cron="0 10-16 * * 1-6",
# )

all_asset_checks: list[AssetChecksDefinition] = load_asset_checks_from_current_module()
#all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks
