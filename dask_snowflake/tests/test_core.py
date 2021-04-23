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
def table(credentials):
    name = f"test_table_{uuid.uuid4().hex}".upper()

    yield name

    engine = create_engine(URL(**credentials))
    engine.execute(f"DROP TABLE {name}")


@pytest.fixture(scope="module")
def credentials():
    return dict(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        database="testdb",
        schema="public",
        warehouse="COMPUTE_WH",
    )


def test_write_read_roundtrip(table, credentials):

    # TODO: Find out if snowflake supports lower-case column names
    df = pd.DataFrame({"A": range(100), "B": range(100, 200)})
    ddf = dd.from_pandas(df, npartitions=50)

    to_snowflake(ddf, name=table, **credentials)
    df_out = read_snowflake(name=table, **credentials)
    # FIXME: Why does read_snowflake return lower-case columns names?
    df_out.columns = df_out.columns.str.upper()
    # FIXME: We need to sort the DataFrame because paritions are written
    # in a non-sequential order.
    pd.testing.assert_frame_equal(df, df_out.sort_values(by="A").reset_index(drop=True))
