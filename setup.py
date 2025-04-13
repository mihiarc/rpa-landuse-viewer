from setuptools import setup, find_packages

setup(
    name="rpa_landuse_app",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "pytest",
        "pyarrow",
        "tqdm"
    ],
    python_requires=">=3.8",
) 