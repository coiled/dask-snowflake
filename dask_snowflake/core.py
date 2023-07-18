from __future__ import annotations

from functools import partial
from typing import Sequence

import pandas as pd
import pyarrow as pa
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
from dask.utils import parse_bytes


@delayed
def write_snowflake(
    df: pd.DataFrame,
    name: str,
    connection_kwargs: dict,
):
    connection_kwargs = {
        **{"application": dask.config.get("snowflake.partner", "dask")},
        **connection_kwargs,
    }
    with snowflake.connector.connect(**connection_kwargs) as conn:
        write_pandas(
            conn=conn,
            df=df,
            schema=connection_kwargs.get("schema", None),
            # NOTE: since ensure_db_exists uses uppercase for the table name
            table_name=name.upper(),
            quote_identifiers=False,
        )


@delayed
def ensure_db_exists(
    df: pd.DataFrame,
    name: str,
    connection_kwargs,
):
    connection_kwargs = {
        **{"application": dask.config.get("snowflake.partner", "dask")},
        **connection_kwargs,
    }
    # NOTE: we have a separate `ensure_db_exists` function in order to use
    # pandas' `to_sql` which will create a table if the requested one doesn't
    # already exist. However, we don't always want to use Snowflake's `pd_writer`
    # approach because it doesn't allow us disable parallel file uploading.
    # For these cases we use a separate `write_snowflake` function.
    engine = create_engine(URL(**connection_kwargs))
    # # NOTE: pd_writer will automatically uppercase the table name
    df.to_sql(
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
    connection_kwargs: dict,
    compute: bool = True,
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
    compute:
        Whether or not to compute immediately. If ``True``, write DataFrame
        partitions to Snowflake immediately. If ``False``, return a list of
        delayed objects that can be computed later. Defaults to ``True``.

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
    # Also, some clusters will overwrite the `snowflake.partner` configuration value.
    # We run `ensure_db_exists` on the cluster to ensure we capture the
    # right partner application ID.
    ensure_db_exists(df._meta, name, connection_kwargs).compute()
    parts = [
        write_snowflake(partition, name, connection_kwargs)
        for partition in df.to_delayed()
    ]
    if compute:
        dask.compute(parts)
    else:
        return parts


def _fetch_batches(chunks: list[ArrowResultBatch], arrow_options: dict):
    return pa.concat_tables([chunk.to_arrow() for chunk in chunks]).to_pandas(
        **arrow_options
    )


@delayed
def _fetch_query_batches(query, connection_kwargs, execute_params):
    connection_kwargs = {
        **{"application": dask.config.get("snowflake.partner", "dask")},
        **connection_kwargs,
    }
    with snowflake.connector.connect(**connection_kwargs) as conn:
        with conn.cursor() as cur:
            cur.check_can_use_pandas()
            cur.check_can_use_arrow_resultset()
            cur.execute(query, execute_params)
            batches = cur.get_result_batches()

    return [b for b in batches if b.rowcount > 0]


def _partition_batches(
    batches: list[ArrowResultBatch],
    meta: pd.DataFrame,
    npartitions: None | int = None,
    partition_size: None | str | int = None,
) -> list[list[ArrowResultBatch]]:
    """
    Given a list of batches and a sample, partition the batches into dask dataframe
    partitions.

    Batch sizing is seemingly not under our control, and is typically much smaller
    than the optimal partition size:
    https://docs.snowflake.com/en/user-guide/python-connector-distributed-fetch.html
    So instead batch the batches into partitions of approximately the right size.
    """
    if (npartitions is None) is (partition_size is None):
        raise ValueError(
            "Must provide exactly one of `npartitions` or `partition_size`"
        )

    if npartitions is not None:
        assert npartitions >= 1
        target = sum([b.rowcount for b in batches]) // npartitions
    elif partition_size is not None:
        partition_bytes = (
            parse_bytes(partition_size)
            if isinstance(partition_size, str)
            else partition_size
        )
        approx_row_size = meta.memory_usage().sum() / len(meta)
        target = max(partition_bytes / approx_row_size, 1)
    else:
        assert False  # unreachable

    batches_partitioned: list[list[ArrowResultBatch]] = []
    curr: list[ArrowResultBatch] = []
    partition_len = 0
    for batch in batches:
        if len(curr) > 0 and batch.rowcount + partition_len > target:
            batches_partitioned.append(curr)
            curr = [batch]
            partition_len = batch.rowcount
        else:
            curr.append(batch)
            partition_len += batch.rowcount
    if curr:
        batches_partitioned.append(curr)

    return batches_partitioned


def read_snowflake(
    query: str,
    *,
    connection_kwargs: dict,
    arrow_options: dict | None = None,
    execute_params: Sequence | dict | None = None,
    partition_size: str | int | None = None,
    npartitions: int | None = None,
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
    execute_params:
        Optional query parameters to pass to Snowflake's ``Cursor.execute(...)``
        method.
    partition_size: int or str
        Approximate size of each partition in the target Dask DataFrame. Either
        an integer number of bytes, or a string description like "100 MiB".
        Reasonable values are often around few hundred MiB per partition. You
        must provide either this or ``npartitions``, with ``npartitions`` taking
        precedence. Partitioning is approximate, and your actual partition sizes may
        vary.
    npartitions: int
        An integer number of partitions for the target Dask DataFrame. You
        must provide either this or ``partition_size``, with ``npartitions`` taking
        precedence. Partitioning is approximate, and your actual number of partitions
        may vary.

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
    if arrow_options is None:
        arrow_options = {}

    # Provide a reasonable default, as the raw batches tend to be too small.
    if partition_size is None and npartitions is None:
        partition_size = "100MiB"

    label = "read-snowflake-"
    output_name = label + tokenize(
        query,
        connection_kwargs,
        arrow_options,
    )

    # Disable `log_imported_packages_in_telemetry` as a temporary workaround for
    # https://github.com/snowflakedb/snowflake-connector-python/issues/1648.
    # Also xref https://github.com/coiled/dask-snowflake/issues/51.
    if connection_kwargs.get("log_imported_packages_in_telemetry"):
        raise ValueError(
            "Using `log_imported_packages_in_telemetry=True` when creating a "
            "Snowflake connection is not currently supported."
        )
    else:
        connection_kwargs["log_imported_packages_in_telemetry"] = False

    # Some clusters will overwrite the `snowflake.partner` configuration value.
    # We fetch snowflake batches on the cluster to ensure we capture the
    # right partner application ID.
    batches = _fetch_query_batches(query, connection_kwargs, execute_params).compute()
    if not batches:
        # Empty results set -> return an empty DataFrame
        meta = dd.utils.make_meta({})
        graph = {(output_name, 0): meta}
        divisions = (None, None)
        return new_dd_object(graph, output_name, meta, divisions)

    batch_types = set(type(b) for b in batches)
    if len(batch_types) > 1 or next(iter(batch_types)) is not ArrowResultBatch:
        # See https://github.com/coiled/dask-snowflake/issues/21
        raise RuntimeError(
            f"Currently only `ArrowResultBatch` are supported, but received batch types {batch_types}"
        )

    # Read the first non-empty batch to determine meta, which is useful for a
    # better size estimate when partitioning. We could also allow empty meta
    # here, which should involve less data transfer to the client, at the
    # cost of worse size estimates. Batches seem less than 1MiB in practice,
    # so this is likely okay right now, but could be revisited.
    meta = batches[0].to_pandas(**arrow_options)

    batches_partitioned = _partition_batches(
        batches, meta, npartitions=npartitions, partition_size=partition_size
    )

    # Create Blockwise layer
    layer = DataFrameIOLayer(
        output_name,
        meta.columns,
        batches_partitioned,
        # TODO: Implement wrapper to only convert columns requested
        partial(_fetch_batches, arrow_options=arrow_options),
        label=label,
    )
    divisions = tuple([None] * (len(batches_partitioned) + 1))
    graph = HighLevelGraph({output_name: layer}, {output_name: set()})
    return new_dd_object(graph, output_name, meta, divisions)
