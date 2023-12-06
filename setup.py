from setuptools import find_packages, setup

setup(
    name="dagster_htmdec",
    packages=find_packages(exclude=["dagster_htmdec_tests"]),
    install_requires=[
        "dagster",
        "girder-client",
        "pandas",
        "scikit-learn",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
