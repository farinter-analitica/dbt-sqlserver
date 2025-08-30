import dlt
from dagster_shared_gf.dlt_shared.sql_database import sql_database
from dagster_shared_gf.resources import sql_server_resources


def main():
    # Set environment variables
    # print(p_driver)
    # credentials = ConnectionStringCredentials()

    # Set the necessary attributes
    # no funciona, tal vez por caracteres especiales en la contraseña
    # credentials.drivername = "mssql+pymssql"
    # credentials.database = "DL_FARINTER"
    # credentials.username = p_user
    # credentials.password = p_password.get_value()
    # credentials.host = "172.16.2.227"
    # credentials.port = 1433
    # credentials.query = {"driver": p_driver}

    # Convert credentials to connection string
    # connection_string = credentials.to_native_representation()

    connection_url_source = (
        sql_server_resources.dwh_farinter_dl.get_sqlalchemy_pyodbc_conn_string()
    )
    connection_url_dest = (
        sql_server_resources.dwh_farinter_dl.get_sqlalchemy_pyodbc_conn_string()
    )

    # print(connection_url_source)
    # print(connection_url_dest)
    ##exit()

    # Parse a connection string and update credentials
    # native_value = "postgresql://my_user:my_password@localhost:5432/my_database"
    # credentials.parse_native_representation(native_value)

    # Get a URL representation of the connection
    # url_representation = credentials.to_url()
    # print(url_representation)

    # connection_string = (
    #     f"{p_user}"
    #     f":{p_password.get_value()}"
    #     f"@{p_server}"
    #     f"/{"DL_FARINTER"}"
    #     f"?&driver={p_driver}"
    #     f"&TrustServerCertificate={'yes'}"
    # )
    # #driver://username:password@host:port/database.
    # #print( f"mssql+pymssql://{connection_string}")
    # credentials = ConnectionStringCredentials(
    #     f"""mssql+pymssql://{p_user}:{p_password.get_value()}@{p_server}/{"DL_FARINTER"}?&driver={p_driver}&TrustServerCertificate={'yes'}"""
    #     #{connection_string}"
    # )

    # print(connection_url_dest)

    # pipeline = dlt.pipeline(
    #   pipeline_name='chess',
    #   destination=dlt.destinations.mssql("mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15"),
    #   dataset_name='chess_data')
    mssql_destination = dlt.destinations.mssql(credentials=connection_url_dest)
    # print(mssql_destination.config_params)
    # exit()
    sql_alchemy_source = sql_database(
        connection_url_source,
        table_names=["DL_SAP_T001"],
        # backend="pyarrow",
        # table_adapter_callback=_double_as_decimal_adapter
    ).with_resources("DL_SAP_T001")

    # print(sql_alchemy_source.discover_schema().to_dict())

    pipeline = dlt.pipeline(
        pipeline_name="sap",
        destination=mssql_destination,
        dataset_name="sap_data",
    )

    info = pipeline.run(sql_alchemy_source)
    print(info)


if __name__ == "__main__":
    main()
