from setuptools import setup, find_packages

setup(
    name="rpa_landuse_app",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.5.0,<2.0.0",
        "pytest>=6.2.5",
        "pyarrow",
        "tqdm",
        "numpy>=1.24.0,<1.26.0"
    ],
    python_requires=">=3.11,<3.12",
) 