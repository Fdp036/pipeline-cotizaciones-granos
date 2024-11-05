from setuptools import setup

setup(
    name="taba-pipeline-compras",
    version="0.1",
    install_requires=[
        "pandas==2.2.1",
        "psycopg2-binary~=2.9.9",
        "boto3~=1.34.58",
        "botocore~=1.34.58",
        "numpy~=1.26.4",
        "yfinance~=0.2.41",
        "requests~=2.32.3",
        "openpyxl~=3.1.2"
    ]
)
