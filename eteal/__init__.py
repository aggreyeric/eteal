"""
eteal – a lightweight ETL library.

Modules
-------
eteal.extract   – read data from SQL databases and files
eteal.transform – apply transformations to DataFrames
eteal.load      – write data to SQL databases and MinIO/S3
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("eteal")
except PackageNotFoundError:  # package not installed (e.g. during development)
    __version__ = "0.0.0.dev"

__all__ = ["__version__"]
