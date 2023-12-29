from setuptools import find_packages, setup

setup(
    name="dagster_htmdec",
    packages=find_packages(exclude=["dagster_htmdec_tests"]),
    install_requires=[
        "dagster",
        "dagster-docker",
        "girder-client",
        "matplotlib",
        "numpy",
        "pandas",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
