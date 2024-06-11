from setuptools import find_packages, setup

setup(
    name="Main_Dagster",
    packages=find_packages(exclude=["Main_Dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-postgres"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
