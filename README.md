# Dask-Snowflake

[![Tests](https://github.com/coiled/dask-snowflake/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/dask-snowflake/actions/workflows/tests.yml)
[![Linting](https://github.com/coiled/dask-snowflake/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/coiled/dask-snowflake/actions/workflows/pre-commit.yml)

This connector is in an early experimental/testing phase.

[Reach out to us](https://coiled.io/contact-us/) if you are interested in trying
it out!

## Installation

`dask-snowflake` can be installed with `pip`:

```
pip install dask-snowflake
```

or with `conda`:

```
conda install -c conda-forge dask-snowflake
```

## Usage

`dask-snowflake` provides `read_snowflake` and `to_snowflake` methods
for parallel IO from Snowflake with Dask.

```python
>>> from dask_snowflake import read_snowflake
>>> example_query = '''
...    SELECT *
...    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;
... '''
>>> ddf = read_snowflake(
...     query=example_query,
...     connection_kwargs={
...         "user": "...",
...         "password": "...",
...         "account": "...",
...     },
... )
```

```python
>>> from dask_snowflake import to_snowflake
>>> to_snowflake(
...     ddf,
...     name="my_table",
...     connection_kwargs={
...         "user": "...",
...         "password": "...",
...         "account": "...",
...     },
... )
```

See their docstrings for further API information.

## Running Tests Locally

If you have a Snowflake accout and access to a database, you can create an environment file in the source tree to run the tests.
This file will be ignored by git, reducing the risk of accidentally commiting it.

### Example `.env` file

```env
SNOWFLAKE_USER="<test user name>"
SNOWFLAKE_PASSWORD="<test_user_password>"
SNOWFLAKE_ACCOUNT="<account>.<region>.aws"
SNOWFLAKE_WAREHOUSE="<test warehouse>"
SNOWFLAKE_ROLE="<test role>"
SNOWFLAKE_DATABASE="<test database>"
SNOWFLAKE_SCHEMA="<test schema>"
```

After creating `snowflake.env`, install the `pytest-dotenv` module

```shell
conda install pytest-dotenv
# or
pip install pytest-dotenv
```

If you run the tests and get an MemoryError mentioning "write+execute memory for ffi.callback()", you probably have stale build of `cffi` from conda-forge.
Remove it and install the version from pip:

```shell
conda remove cffi --force
pip install cffi
```

After that you should be able to execute `pytest` and the tests should pass.

## License

[BSD-3](LICENSE)
