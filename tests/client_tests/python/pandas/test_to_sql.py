import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

import pandas as pd
from pandas._testing import assert_frame_equal

INPUT_FRAME = pd.DataFrame.from_records([{"id": 1, "name": "foo", "value": 42.42}])
SQL_SELECT_STATEMENT = "SELECT * FROM doc.foo;"
SQL_REFRESH_STATEMENT = "REFRESH TABLE doc.foo;"


def test_crate_to_sql(cratedb_http_host, cratedb_http_port):
    # Connect to database.
    engine = sa.create_engine(
        url=f"crate://{cratedb_http_host}:{cratedb_http_port}",
        echo=True,
    )
    con = engine.connect()

    # Insert data using pandas.
    df = INPUT_FRAME
    retval = df.to_sql(name="foo", con=con, if_exists="replace", index=False)
    assert retval == -1

    # Synchronize table content.
    con.execute(sa.text(SQL_REFRESH_STATEMENT))

    # Read back and verify data using pandas.
    df = pd.read_sql(sql=sa.text(SQL_SELECT_STATEMENT), con=con)
    assert_frame_equal(df, INPUT_FRAME)


@pytest.mark.skip(reason="Needs COLLATE and pg_table_is_visible")
def test_psycopg_to_sql(cratedb_psql_host, cratedb_psql_port):
    # Connect to database.
    engine = sa.create_engine(
        url=f"postgresql+psycopg_relaxed://crate@{cratedb_psql_host}:{cratedb_psql_port}",
        isolation_level="AUTOCOMMIT",
        use_native_hstore=False,
        echo=True,
    )
    conn = engine.connect()

    # Insert data using pandas.
    df = INPUT_FRAME
    retval = df.to_sql(name="foo", con=conn, if_exists="replace", index=False)
    assert retval == -1

    # Synchronize table content.
    conn.execute(sa.text(SQL_REFRESH_STATEMENT))

    # Read back and verify data using pandas.
    df = pd.read_sql(sql=sa.text(SQL_SELECT_STATEMENT), con=conn)
    assert_frame_equal(df, INPUT_FRAME)


@pytest.mark.skip(reason="Needs COLLATE and pg_table_is_visible")
@pytest.mark.asyncio
async def test_psycopg_async_to_sql(cratedb_psql_host, cratedb_psql_port):
    # Connect to database.
    engine = create_async_engine(
        url=f"postgresql+psycopg_relaxed://crate@{cratedb_psql_host}:{cratedb_psql_port}",
        isolation_level="AUTOCOMMIT",
        use_native_hstore=False,
        echo=True,
    )

    # Insert data using pandas.
    async with engine.begin() as conn:
        df = INPUT_FRAME
        retval = await conn.run_sync(to_sql_sync, df=df, name="foo", if_exists="replace", index=False)
        assert retval == -1

    # TODO: Read back dataframe and compare with original.


@pytest.mark.skip(reason="Needs COLLATE and pg_table_is_visible")
@pytest.mark.asyncio
async def test_asyncpg_to_sql(cratedb_psql_host, cratedb_psql_port):
    # Connect to database.
    engine = create_async_engine(
        url=f"postgresql+asyncpg_relaxed://crate@{cratedb_psql_host}:{cratedb_psql_port}",
        isolation_level="AUTOCOMMIT",
        echo=True,
    )

    # Insert data using pandas.
    async with engine.begin() as conn:
        df = INPUT_FRAME
        retval = await conn.run_sync(to_sql_sync, df=df, name="foo", if_exists="replace", index=False)
        assert retval == -1

    # TODO: Read back dataframe and compare with original.


def to_sql_sync(conn, df, name, **kwargs):
    """
    Making df.to_sql connection the first argument to make it compatible
    with conn.run_sync(), see https://stackoverflow.com/a/70861276.
    """
    return df.to_sql(name=name, con=conn, **kwargs)
