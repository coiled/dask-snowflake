#!/usr/bin/env python

from setuptools import setup

setup(
    name="dask-snowflake",
    version="0.0.2",
    description="Dask + Snowflake intergration",
    license="BSD",
    maintainer="James Bourbeau",
    maintainer_email="james@coiled.io",
    packages=["dask_snowflake"],
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    include_package_data=True,
    zip_safe=False,
)
