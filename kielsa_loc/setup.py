from setuptools import find_packages, setup

setup(
    name="kielsa",
    packages=find_packages(exclude=["kielsa_tests"]),
    install_requires=[
        "dagster",
        "dbt-core",
        "dagster-dbt",
        "dagster-cloud",
        "dagster-postgres",
        "pandas",
        "dbt-sqlserver"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "pyodbc"]},
)
