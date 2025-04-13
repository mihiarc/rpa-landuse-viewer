from setuptools import setup, find_packages

setup(
    name="rpa_landuse_app",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "plotly",
        "folium",
        "pytest",
        "mysql-connector-python",
        "redis",
        "pyarrow",
        "tqdm",
        "python-dotenv",
        "fastapi",
        "uvicorn"
    ],
    python_requires=">=3.8",
) 