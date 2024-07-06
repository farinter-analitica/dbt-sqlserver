from setuptools import find_packages, setup

setup(
    name="dagster_sap_gf",
    packages=find_packages(exclude=["dagster_sap_gf_tests"]),
    install_requires=[
        "dagster_shared_gf"
    ],
    extras_require={"dev": [ "pytest"]},
)
