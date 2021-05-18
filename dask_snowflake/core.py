from typing import Dict, Optional

import pandas as pd
import pyarrow as pa
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import pd_writer, write_pandas
from snowflake.connector.result_batch import ArrowResultBatch
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

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


def _fetch_snowflake_batch(chunk: ArrowResultBatch, arrow_options: Dict):
    return pa.concat_tables(chunk.create_iter(iter_unit="table")).to_pandas(
        **arrow_options
    )


def read_snowflake(
    query: str, conn: SnowflakeConnection, arrow_options: Optional[Dict] = None
) -> dd.DataFrame:
    """
    Generate a dask.DataFrame based of the result of a snowflakeDB query.


    Parameters
    ----------
    query:
        The snowflake DB query to execute
    conn:
        An already established connnection to the database
    arrow_options:
        Optional arguments provided to the arrow.Table.to_pandas method

    Examples
    --------

    example_query = '''
        SELECT *
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;
    '''
    import snowflake.connector
    from dask_snowflake.core import read_snowflake

    with snowflake.connector.connect(
        user="XXX",
        password="XXX",
        account="XXX",
    ) as conn:
        ddf = read_snowflake(
            query=example_query,
            conn=conn,
        )
        ddf

    """
    with conn.cursor() as cur:
        cur.check_can_use_pandas()
        cur.check_can_use_arrow_resultset()
        cur.execute(query)
        batches = cur.get_result_batches()

    if arrow_options is None:
        arrow_options = {}

    # There are sometimes null batches
    filtered_batches = [b for b in batches if b.uncompressed_size]
    if not filtered_batches:
        # TODO: How to gracefully handle empty results?
        raise NotImplementedError("Empty result")

    meta = None
    for b in filtered_batches:
        if not isinstance(b, ArrowResultBatch):
            # This should never since the above check_can_use* calls should
            # raise before if arrow is not properly setup
            raise RuntimeError(f"Received unknown result batch type {type(b)}")
        meta = _fetch_snowflake_batch(b, arrow_options=arrow_options)
        break

    fetch_delayed = dask.delayed(_fetch_snowflake_batch)
    return dd.from_delayed(
        [fetch_delayed(c, arrow_options=arrow_options) for c in filtered_batches],
        meta=meta,
    )
