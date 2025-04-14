# project_root/setup.py
from setuptools import setup, find_packages

setup(
    # external user看到的package名字
    name="metar_etl",
    version="0.1.0",
    description="ETL pipeline for METAR data",
    author="Anqi",
    # 内部打包的module name
    packages=["metar_etl"],
    # tell setuptools：location of source code of metar_etl package
    package_dir={"metar_etl": "airflow/scripts"},
    install_requires=[
        "pandas",
        "requests",
        "pyspark",
        "google-cloud-storage",
        "python-dotenv",
        # other dependencies
    ],
    include_package_data=True,
)
