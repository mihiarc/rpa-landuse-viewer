from setuptools import setup, find_packages

setup(
    name="rpa-landuse-viewer",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "duckdb",
        "pandas",
        "tqdm",
        "python-dotenv",
    ],
    python_requires=">=3.8",
    description="RPA Land Use Projections Viewer",
    author="RPA Team",
) 