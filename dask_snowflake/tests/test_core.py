import os
import uuid

import pandas as pd
import pytest
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

import dask.dataframe as dd
from distributed import Client

from dask_snowflake import read_snowflake, to_snowflake

client = Client(n_workers=2, threads_per_worker=10)


@pytest.fixture
def table(connection_kwargs):
    name = f"test_table_{uuid.uuid4().hex}".upper()

    yield name

    engine = create_engine(URL(**connection_kwargs))
    engine.execute(f"DROP TABLE IF EXISTS {name}")


@pytest.fixture(scope="module")
def connection_kwargs():
    return dict(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        database="testdb",
        schema="public",
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )


def test_write_read_roundtrip(table, connection_kwargs):

    # TODO: Find out if snowflake supports lower-case column names
    df = pd.DataFrame({"A": range(10), "B": range(10, 20)})
    ddf = dd.from_pandas(df, npartitions=2)

    to_snowflake(ddf, name=table, connection_kwargs=connection_kwargs)

    query = f"SELECT * FROM {table}"
    df_out = read_snowflake(query, connection_kwargs=connection_kwargs)
    # FIXME: Why does read_snowflake return lower-case columns names?
    df_out.columns = df_out.columns.str.upper()
    # FIXME: We need to sort the DataFrame because paritions are written
    # in a non-sequential order.
    dd.utils.assert_eq(
        df, df_out.sort_values(by="A").reset_index(drop=True), check_dtype=False
    )
