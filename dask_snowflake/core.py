import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer

import dask
from dask.delayed import delayed


@delayed
def write_partition(
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
    df.to_sql(name=name, con=engine, index=False, if_exists="append", method=pd_writer)


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
    engine_kwargs = {
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
    write_partition(df._meta, **engine_kwargs).compute()
    dask.compute(
        [write_partition(partition, **engine_kwargs) for partition in df.to_delayed()]
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
