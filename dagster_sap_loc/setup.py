from setuptools import find_packages, setup

setup(
    name="dagster_sap",
    packages=find_packages(exclude=["dagster_sap_tests"]),
    install_requires=[
        "dagster_shared_gf"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "pyodbc"]},
)
