from setuptools import find_packages, setup

setup(
    name="dagster_farinter",
    packages=find_packages(exclude=["dagster_farinter_tests"]),
    install_requires=[
        "dagster_shared_gf"
        "dagster==1.7.9",
        "dbt-core==1.7.16",
        "dagster-dbt==0.23.9",
        "dagster-cloud",
        "dagster-postgres==1.7.4",
        "pandas",
        "dbt-sqlserver==1.7.4"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "pyodbc"]},
)
