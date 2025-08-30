import dlt
from dagster_shared_gf.dlt_shared.sql_database import sql_table
from sqlalchemy.engine import URL, create_engine
from dagster_shared_gf.resources import sql_server_resources
from dagster import EnvVar


def main():
    # Set environment variables
    p_server = sql_server_resources.p_server
    p_user = sql_server_resources.p_user
    p_password: EnvVar = sql_server_resources.p_password
    p_driver = sql_server_resources.p_driver

    if p_driver is None:
        raise ValueError("p_driver is not set. Please set the SQL Server driver.")

    connection_url_source = URL.create(
        "mssql+pymssql",
        username=p_user,
        password=p_password.get_value(),
        host=p_server,
        # , port=1433
        database="DL_FARINTER",
        query={"driver": p_driver, "TrustServerCertificate": "yes"},
    )
    connection_url_dest = URL.create(
        "mssql",
        username=p_user,
        password=p_password.get_value(),
        host=p_server,
        # , port=1433
        database="DL_FARINTER",
        query={"driver": p_driver, "TrustServerCertificate": "yes"},
    )

    source_engine = create_engine(connection_url_source)

    mssql_destination = dlt.destinations.mssql(
        credentials=connection_url_dest.render_as_string(hide_password=False)
    )

    sql_alchemy_table = sql_table(
        credentials=source_engine, table="DL_SAP_T001", schema="dbo"
    )

    pipeline = dlt.pipeline(
        pipeline_name="sap", destination=mssql_destination, dataset_name="sap_data"
    )

    info = pipeline.run(sql_alchemy_table)
    print(info)


if __name__ == "__main__":
    main()
