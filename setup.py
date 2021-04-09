#!/usr/bin/env python

import sys
from setuptools import setup, find_packages

setup(
    name="dask-snowflake",
    version="0.0.1",
    description="Dask + Snowflake intergration",
    license="MIT",
    packages=find_packages(),
    long_description=open("README.md").read(),
    python_requires=">=3.7",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    include_package_data=True,
    zip_safe=False,
)
