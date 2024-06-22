from setuptools import find_packages, setup

setup(
    name="dagster_farinter",
    packages=find_packages(exclude=["dagster_farinter_tests"]),
    install_requires=[
        "dagster_shared_gf"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "pyodbc"]},
)
