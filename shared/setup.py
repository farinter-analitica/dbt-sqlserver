from setuptools import find_packages, setup

setup(
    name="shared",
    packages=find_packages(exclude=["shared_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-postgres"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest","pyodbc", "dagster-dbt"]},
)
#Install module with python