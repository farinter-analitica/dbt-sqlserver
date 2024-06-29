from setuptools import find_packages, setup

setup(
    name="dagster_kielsa",
    packages=find_packages(exclude=["dagster_kielsa_tests"]),
    install_requires=[
        "dagster_shared_gf"
    ],
    extras_require={"dev": [ "pytest"]},
)
