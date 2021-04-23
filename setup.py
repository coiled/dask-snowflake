#!/usr/bin/env python

from setuptools import find_packages, setup

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
