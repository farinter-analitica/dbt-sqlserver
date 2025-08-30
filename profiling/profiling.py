import pandas as pd  # type: ignore
from ydata_profiling import ProfileReport  # type: ignore
from dagster_shared_gf.resources.sql_server_resources import dwh_farinter_dl
from sqlalchemy import create_engine, text, URL
# dwh_farinter_dl = SQLServerResource(
#     server= dwh_farinter.server,
#     databases= dwh_farinter.databases,
#     user=dwh_farinter.user,
#     password=dwh_farinter.password,
#     trust_server_certificate=dwh_farinter.trust_server_certificate,
#     default_database="DL_FARINTER"
#     )

# Database connection details
server = dwh_farinter_dl.server
database = dwh_farinter_dl.default_database
username = dwh_farinter_dl.username
password = dwh_farinter_dl.get_password()
trust_certificate = dwh_farinter_dl.trust_server_certificate
driver = "ODBC Driver 18 for SQL Server"

# Create a connection URL
connection_url = URL.create(
    "mssql+pymssql",
    username=username,
    password=password,
    host=server,
    database=database,
    query={"driver": driver, "TrustServerCertificate": trust_certificate},
)

# Create a SQLAlchemy engine
engine = create_engine(connection_url)

# SQL query to fetch data from the linked server
table_name = "MARA"
schema = "prd"
database = "PRD"
linked_server = "SAPPRD"
query = f"""
SELECT * FROM {linked_server + "." + database if linked_server else database}.{schema}.{table_name}
"""

# Execute the query and fetch the data into a pandas DataFrame
with engine.connect() as connection:
    df = pd.read_sql_query(text(query), connection)

# Generate a profiling report
profile = ProfileReport(df, title="Data Profiling Report", explorative=True)

# Save the report to an HTML file
profile.to_file(f"data_profiling_report_{table_name}.html")

# If you want to display the report in a Jupyter notebook, you can use:
# profile.to_notebook_iframe()
