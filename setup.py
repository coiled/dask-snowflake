#!/usr/bin/env python

from setuptools import setup

setup(
    name="dask-snowflake",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
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
