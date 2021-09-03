from functools import partial
from typing import Dict, Optional

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer, write_pandas
from snowflake.connector.result_batch import ArrowResultBatch
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

import dask
import dask.dataframe as dd
from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.delayed import delayed
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from dask.utils import SerializableLock


@delayed
def write_snowflake(
    df: dd.DataFrame,
    name: str,
    connection_kwargs: Dict,
):
    # TODO: Remove the `use_new_put_get` logic below once the known PUT issue with
    # `snowflake-connector-python` is resolved
    if "use_new_put_get" not in connection_kwargs:
        connection_kwargs["use_new_put_get"] = False

    with snowflake.connector.connect(**connection_kwargs) as conn:
        # NOTE: Use a process-wide lock to avoid a `boto` multithreading issue
        # https://github.com/snowflakedb/snowflake-connector-python/issues/156
        with SerializableLock(token="write_snowflake"):
            write_pandas(
                conn=conn,
                df=df,
                schema=connection_kwargs.get("schema", None),
                # NOTE: since ensure_db_exists uses uppercase for the table name
                table_name=name.upper(),
                parallel=1,
                quote_identifiers=False,
            )


def ensure_db_exists(
    df: pd.DataFrame,
    name: str,
    connection_kwargs,
):
    # NOTE: we have a separate `ensure_db_exists` function in order to use
    # pandas' `to_sql` which will create a table if the requested one doesn't
    # already exist. However, we don't always want to Snowflake's `pd_writer`
    # approach because it doesn't allow us disable parallel file uploading.
    # For these cases we use a separate `write_snowflake` function.
    engine = create_engine(URL(**connection_kwargs))
    # # NOTE: pd_writer will automatically uppercase the table name
    df._meta.to_sql(
        name=name,
        schema=connection_kwargs.get("schema", None),
        con=engine,
        index=False,
        if_exists="append",
        method=pd_writer,
    )


def to_snowflake(
    df: dd.DataFrame,
    name: str,
    connection_kwargs: Dict,
):
    """Write a Dask DataFrame to a Snowflake table.

    Parameters
    ----------
    df:
        Dask DataFrame to save.
    name:
        Name of the table to save to.
    connection_kwargs:
        Connection arguments used when connecting to Snowflake with
        ``snowflake.connector.connect``.

    Examples
    --------

    >>> from dask_snowflake import to_snowflake
    >>> df = ...  # Create a Dask DataFrame
    >>> to_snowflake(
    ...     df,
    ...     name="my_table",
    ...     connection_kwargs={
    ...         "user": "...",
    ...         "password": "...",
    ...         "account": "...",
    ...     },
    ... )

    """
    # Write the DataFrame meta to ensure table exists before
    # trying to write all partitions in parallel. Otherwise
    # we run into race conditions around creating a new table.
    ensure_db_exists(df, name, connection_kwargs)
    dask.compute(
        [
            write_snowflake(partition, name, connection_kwargs)
            for partition in df.to_delayed()
        ]
    )


def _fetch_snowflake_batch(chunk: ArrowResultBatch, arrow_options: Dict):
    return chunk.to_pandas(**arrow_options)


def read_snowflake(
    query: str, connection_kwargs: Dict, arrow_options: Optional[Dict] = None
) -> dd.DataFrame:
    """Load a Dask DataFrame based of the result of a Snowflake query.

    Parameters
    ----------
    query:
        The Snowflake query to execute.
    connection_kwargs:
        Connection arguments used when connecting to Snowflake with
        ``snowflake.connector.connect``.
    arrow_options:
        Optional arguments forwarded to ``arrow.Table.to_pandas`` when
        converting data to a pandas DataFrame.

    Examples
    --------

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

    """
    if "application" not in connection_kwargs:
        # TODO: Set partner connect ID / application to dask once public
        connection_kwargs["application"] = dask.config.get(
            "snowflake.partner", "Coiled_Cloud"
        )

    label = "read-snowflake-"
    output_name = label + tokenize(
        query,
        connection_kwargs,
        arrow_options,
    )

    with snowflake.connector.connect(**connection_kwargs) as conn:
        with conn.cursor() as cur:
            cur.check_can_use_pandas()
            cur.check_can_use_arrow_resultset()
            cur.execute(query)
            batches = cur.get_result_batches()

    if arrow_options is None:
        arrow_options = {}

    meta = None
    for b in batches:
        if not isinstance(b, ArrowResultBatch):
            # This should never since the above check_can_use* calls should
            # raise before if arrow is not properly setup
            raise RuntimeError(f"Received unknown result batch type {type(b)}")
        meta = b.to_pandas()
        break

    if not batches:
        # empty dataframe - just use meta
        graph = {(output_name, 0): meta}
        divisions = (None, None)
    else:
        # Create Blockwise layer
        layer = DataFrameIOLayer(
            output_name,
            meta.columns,
            batches,
            # TODO: Implement wrapper to only convert columns requested
            partial(_fetch_snowflake_batch, arrow_options=arrow_options),
            label=label,
        )
        divisions = tuple([None] * (len(batches) + 1))
        graph = HighLevelGraph({output_name: layer}, {output_name: set()})
    return new_dd_object(graph, output_name, meta, divisions)
