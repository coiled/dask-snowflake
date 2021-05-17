import math
import pyarrow as pa
import dask
import dask.dataframe as dd
from toolz.itertoolz import partition_all


def _flatten(list_of_lists):
    # TODO there is something in dask / toolz for this
    if not isinstance(list_of_lists, list):
        return list_of_lists
    result = []
    for sublist in list_of_lists:
        for el in sublist:
            result.append(el)
    return result


def _fetch_snowflake_batches(chunks):
    tables_iters = []
    for c in chunks:
        # Returns a list of one table. Probably compat but just in case keep
        # this interface
        tables_iters.append(c.create_iter(iter_unit="table"))
    all_tables = _flatten(tables_iters)
    # TODO: Do we need to promote here? I would assume the schema of the result
    # batch to be fixed so No but is this assumption valid?
    return pa.concat_tables(all_tables).to_pandas()


def from_snowflake(
    query,
    conn,
    chunk_size=10 * 1024 ** 2,
    size_measure="bytes",
):
    with conn.cursor() as cur:
        cur.check_can_use_pandas()
        cur.check_can_use_arrow_resultset()
        cur.execute(query)
        batches = cur.get_result_batches()

    if size_measure != "bytes":
        raise NotImplementedError
    fetch_delayed = dask.delayed(_fetch_snowflake_batches)
    meta = None
    for b in batches:
        if b.uncompressed_size:
            meta = _fetch_snowflake_batches([b])
            break

    fetch_delayed = dask.delayed(_fetch_snowflake_batches)
    # We need to perform a few tests if this chunk size actually is usefull. My
    # first impressions seemed like the batches are very small
    if chunk_size:
        total_size_result = sum(
            (c.uncompressed_size for c in batches if c.uncompressed_size)
        )
        num_chunks = math.ceil(total_size_result / chunk_size)
        print(f"Num chunks: {num_chunks} / {len(batches)}")
        if num_chunks < len(batches):
            batches_for_tasks = list(partition_all(num_chunks, batches))
        else:
            batches_for_tasks = [(el,) for el in batches if el.uncompressed_size]
        print(f"Num partitions: {len(batches_for_tasks)}")

        print("Constructing dataframe")
        return dd.from_delayed(
            [fetch_delayed(to_fetch) for to_fetch in batches_for_tasks], meta=meta
        )
    else:
        return dd.from_delayed(
            [fetch_delayed([to_fetch]) for to_fetch in batches], meta=meta
        )



if __name__ == "__main__":
    # Example usage
    example_query_one_table = """
    SELECT *
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;
    """
    import snowflake.connector
    conn = snowflake.connector.connect(
        user='XXX',
        password='XXX',
        account="XXX",
    )

    # This will block until snowflake calculated the query. Afterwards we can do
    # our dask things and every individual task takes subseconds from what I've
    # seen

    # According to the docs, this dataframe will be valid for about 6h before
    # the access will be invalidated
    ddf = from_snowflake(example_query_one_table, conn, chunk_size=10)
    ddf.npartitions
