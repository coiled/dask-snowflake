import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer, write_pandas

import dask
import dask.dataframe as dd
from dask.delayed import delayed
from dask.utils import SerializableLock


@delayed
def write_snowflake(
    df: pd.DataFrame,
    *,
    name: str,
    user: str,
    password: str,
    account: str,
    database: str,
    schema: str,
    warehouse: str,
):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        schema=schema,
        warehouse=warehouse,
    )
    # NOTE: Use a process-wide lock to avoid a `boto` multithreading issue
    # https://github.com/snowflakedb/snowflake-connector-python/issues/156
    with SerializableLock(token="write_snowflake"):
        write_pandas(
            conn=conn,
            df=df,
            # NOTE: since ensure_db_exists uses uppercase for the table name
            table_name=name.upper(),
            parallel=1,
        )


def ensure_db_exists(
    df: dd.DataFrame,
    *,
    name: str,
    user: str,
    password: str,
    account: str,
    database: str,
    schema: str,
    warehouse: str,
):
    # NOTE: we have a separate `ensure_db_exists` function in order to use
    # pandas' `to_sql` which will create a table if the requested one doesn't
    # already exist. However, we don't always want to Snowflake's `pd_writer`
    # approach because it doesn't allow us disable parallel file uploading.
    # For these cases we use a separate `write_snowflake` function.
    engine = create_engine(
        URL(
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema,
            warehouse=warehouse,
            numpy=True,
        )
    )
    # NOTE: pd_writer will automatically uppercase the table name
    df._meta.to_sql(
        name=name, con=engine, index=False, if_exists="append", method=pd_writer
    )


def to_snowflake(
    df,
    *,
    name: str,
    user: str,
    password: str,
    account: str,
    database: str,
    schema: str,
    warehouse: str,
):
    storage_options = {
        "name": name,
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
    }
    # Write the DataFrame meta to ensure table exists before
    # trying to write all partitions in parallel. Otherwise
    # we run into race conditions around creating a new table.
    ensure_db_exists(df, **storage_options)
    dask.compute(
        [write_snowflake(partition, **storage_options) for partition in df.to_delayed()]
    )


def read_snowflake(
    *,
    name: str,
    user: str,
    password: str,
    account: str,
    database: str,
    schema: str,
    warehouse: str,
):
    engine = create_engine(
        URL(
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema,
            warehouse=warehouse,
            numpy=True,
        )
    )
    return pd.read_sql(f"SELECT * FROM {name}", engine)
