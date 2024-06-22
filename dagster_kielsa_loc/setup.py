from setuptools import find_packages, setup

setup(
    name="dagster_kielsa",
    packages=find_packages(exclude=["dagster_kielsa_tests"]),
    install_requires=[
        "dagster==1.7.9",
        "dbt-core==1.7.16",
        "dagster-dbt==0.23.9",
        "dagster-cloud",
        "dagster-postgres==1.7.4",
        "pandas",
        "dbt-sqlserver==1.7.4", 
        "pyodbc"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
