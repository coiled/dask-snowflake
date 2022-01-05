import os
import uuid

import pandas as pd
import pytest
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

import dask
import dask.dataframe as dd
from distributed import Client, Lock, worker_client

from dask_snowflake import read_snowflake, to_snowflake


@pytest.fixture
def client():
    with Client(n_workers=2, threads_per_worker=10) as client:
        yield client


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
        role=os.environ["SNOWFLAKE_ROLE"],
    )


# TODO: Find out if snowflake supports lower-case column names
df = pd.DataFrame({"A": range(10), "B": range(10, 20)})
ddf = dd.from_pandas(df, npartitions=2)


def test_write_read_roundtrip(table, connection_kwargs, client):
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


def test_application_id_default(table, connection_kwargs, monkeypatch):
    # Patch Snowflake's normal connection mechanism with checks that
    # the expected application ID is set
    count = 0

    def mock_connect(**kwargs):
        nonlocal count
        count += 1
        assert kwargs["application"] == "dask"
        return snowflake.connector.Connect(**kwargs)

    monkeypatch.setattr(snowflake.connector, "connect", mock_connect)

    to_snowflake(ddf, name=table, connection_kwargs=connection_kwargs)
    # One extra connection is made to ensure the DB table exists
    count_after_write = ddf.npartitions + 1
    assert count == count_after_write

    ddf_out = read_snowflake(
        f"SELECT * FROM {table}", connection_kwargs=connection_kwargs
    )
    assert count == count_after_write + ddf_out.npartitions


def test_application_id_config(table, connection_kwargs, monkeypatch):
    with dask.config.set({"snowflake.partner": "foo"}):
        # Patch Snowflake's normal connection mechanism with checks that
        # the expected application ID is set
        count = 0

        def mock_connect(**kwargs):
            nonlocal count
            count += 1
            assert kwargs["application"] == "foo"
            return snowflake.connector.Connect(**kwargs)

        monkeypatch.setattr(snowflake.connector, "connect", mock_connect)

        to_snowflake(ddf, name=table, connection_kwargs=connection_kwargs)
        # One extra connection is made to ensure the DB table exists
        count_after_write = ddf.npartitions + 1
        assert count == count_after_write

        ddf_out = read_snowflake(
            f"SELECT * FROM {table}", connection_kwargs=connection_kwargs
        )
        assert count == count_after_write + ddf_out.npartitions


def test_application_id_config_on_cluster(table, connection_kwargs, client):
    # Ensure client and workers have different `snowflake.partner` values set.
    # Later we'll check that the config value on the workers is the one that's actually used.
    with dask.config.set({"snowflake.partner": "foo"}):
        client.run(lambda: dask.config.set({"snowflake.partner": "bar"}))
        assert dask.config.get("snowflake.partner") == "foo"
        assert all(
            client.run(lambda: dask.config.get("snowflake.partner") == "bar").values()
        )

        # Patch Snowflake's normal connection mechanism with checks that
        # the expected application ID is set
        def patch_snowflake_connect():
            def mock_connect(**kwargs):
                with worker_client() as client:
                    # A lock is needed to safely increment the connect counter below
                    with Lock("snowflake-connect"):
                        assert kwargs["application"] == "bar"
                        count = client.get_metadata("connect-count", 0)
                        client.set_metadata("connect-count", count + 1)
                    return snowflake.connector.Connect(**kwargs)

            snowflake.connector.connect = mock_connect

        client.run(patch_snowflake_connect)

        to_snowflake(ddf, name=table, connection_kwargs=connection_kwargs)
        # One extra connection is made to ensure the DB table exists
        count_after_write = ddf.npartitions + 1

        ddf_out = read_snowflake(
            f"SELECT * FROM {table}", connection_kwargs=connection_kwargs
        )
        assert (
            client.get_metadata("connect-count")
            == count_after_write + ddf_out.npartitions
        )


def test_application_id_explicit(table, connection_kwargs, monkeypatch):
    # Include explicit application ID in input `connection_kwargs`
    connection_kwargs["application"] = "foo"

    # Patch Snowflake's normal connection mechanism with checks that
    # the expected application ID is set
    count = 0

    def mock_connect(**kwargs):
        nonlocal count
        count += 1
        assert kwargs["application"] == "foo"
        return snowflake.connector.Connect(**kwargs)

    monkeypatch.setattr(snowflake.connector, "connect", mock_connect)

    to_snowflake(ddf, name=table, connection_kwargs=connection_kwargs)
    # One extra connection is made to ensure the DB table exists
    count_after_write = ddf.npartitions + 1
    assert count == count_after_write

    ddf_out = read_snowflake(
        f"SELECT * FROM {table}", connection_kwargs=connection_kwargs
    )
    assert count == count_after_write + ddf_out.npartitions
