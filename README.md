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

## License

[BSD-3](LICENSE)