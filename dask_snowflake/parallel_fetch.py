from typing import Dict, Optional

import pyarrow as pa
from snowflake.connector.result_batch import ArrowResultBatch

from snowflake.connector import SnowflakeConnection
import dask
import dask.dataframe as dd


def _fetch_snowflake_batch(chunk: ArrowResultBatch, arrow_options: Dict):
    return pa.concat_tables(chunk.create_iter(iter_unit="table")).to_pandas(
        **arrow_options
    )


def from_snowflake(
    query: str, conn: SnowflakeConnection, arrow_options: Optional[Dict] = None
) -> dd.DataFrame:
    """
    Generate a dask.DataFrame based off the result of a snowflakeDB query.

    Parameters
    ==========
    query:
        The snowflake DB query to execute
    conn:
        An already established connnection to the database
    arrow_options:
        Optional arguments provided to the arrow.Table.to_pandas method
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
        raise ValueError("Empty result")

    meta = None
    for b in filtered_batches:
        meta = _fetch_snowflake_batch(b, arrow_options=arrow_options)
        break

    fetch_delayed = dask.delayed(_fetch_snowflake_batch)
    return dd.from_delayed(
        [fetch_delayed(c, arrow_options=arrow_options) for c in filtered_batches],
        meta=meta,
    )


if __name__ == "__main__":
    # Example usage
    example_query_one_table = """
    SELECT *
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;
    """
    import snowflake.connector

    conn = snowflake.connector.connect(
        user="XXX",
        password="XXX",
        account="XXX",
    )

    # This will block until snowflake calculated the query. Afterwards we can do
    # our dask things and every individual task takes subseconds from what I've
    # seen

    # According to the docs, this dataframe will be valid for about 6h before
    # the access will be invalidated
    ddf = from_snowflake(example_query_one_table, conn)
    ddf.npartitions
