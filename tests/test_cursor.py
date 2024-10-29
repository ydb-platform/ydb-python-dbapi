import pytest

import ydb

import ydb_dbapi
import ydb_dbapi.cursors


@pytest.fixture
async def cursor(driver: ydb.aio.Driver):
    session_pool = ydb.aio.QuerySessionPool(driver)
    session = await session_pool.acquire()
    cursor = ydb_dbapi.cursors.Cursor(session_pool, session)

    yield cursor

    await session_pool.release(session)


@pytest.mark.asyncio
async def test_cursor_ddl(cursor):
    op = ydb_dbapi.cursors.YdbQuery(
        yql_text="""
        CREATE TABLE table (
        id Int64 NOT NULL,
        i64Val Int64,
        PRIMARY KEY(id)
        )
        """,
        is_ddl=True,
    )

    await cursor.execute(op)
