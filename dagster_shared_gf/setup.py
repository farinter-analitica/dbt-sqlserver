from setuptools import find_packages, setup

setup(
    name="dagster_shared_gf",
    packages=find_packages(exclude=["dagster_shared_gf_tests"]),
    install_requires=[
        "dagster>=1.7.12,<2.0",
        "dbt-core>=1.8.3,<2.0",
        "dagster-dbt>=0.23",
        "dbt-postgres>=1.8,<2.0",
        "dbt-sqlserver @ git+https://github.com/axellpadilla/dbt-sqlserver.git@dbt_18#egg=dbt-sqlserver",
        #"dbt-sqlserver>=1.8.0,<2.0",
        "dagster-webserver>=1.7,<2.0",
        "dagster-postgres",
        "dagster-cloud",
        "trycast",
        "pandas",
        "pyodbc",
        "smbprotocol",
    ],
    extras_require={"dev": [ "pytest"]},
)
#Install module with python
