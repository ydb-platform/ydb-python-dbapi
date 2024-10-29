from collections.abc import AsyncGenerator
from collections.abc import Generator

import pytest
import ydb
import ydb_dbapi

INSERT_YQL = """
DELETE FROM table;
INSERT INTO table (id, val) VALUES
(1, 1),
(2, 2),
(3, 3),
(4, 4)
"""


@pytest.fixture
async def session(
    session_pool: ydb.aio.QuerySessionPool,
) -> AsyncGenerator[ydb.aio.QuerySession]:
    await session_pool.execute_with_retries(INSERT_YQL)

    session = await session_pool.acquire()
    yield session
    await session_pool.release(session)


@pytest.fixture
def session_sync(
    session_pool_sync: ydb.QuerySessionPool,
) -> Generator[ydb.QuerySession]:
    session_pool_sync.execute_with_retries(INSERT_YQL)

    session = session_pool_sync.acquire()
    yield session
    session_pool_sync.release(session)


class TestAsyncCursor:
    @pytest.mark.asyncio
    async def test_cursor_fetch_one(
        self, session: ydb.aio.QuerySession
    ) -> None:
        async with ydb_dbapi.AsyncCursor(session=session) as cursor:
            yql_text = """
            SELECT id, val FROM table
            """
            await cursor.execute(query=yql_text)

            for i in range(4):
                res = await cursor.fetchone()
                assert res is not None
                assert res[0] == i + 1

            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_fetch_many(
        self, session: ydb.aio.QuerySession
    ) -> None:
        async with ydb_dbapi.AsyncCursor(session=session) as cursor:
            yql_text = """
            SELECT id, val FROM table
            """
            await cursor.execute(query=yql_text)

            res = await cursor.fetchmany()
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 1

            res = await cursor.fetchmany(size=2)
            assert res is not None
            assert len(res) == 2
            assert res[0][0] == 2
            assert res[1][0] == 3

            res = await cursor.fetchmany(size=2)
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 4

            assert await cursor.fetchmany(size=2) is None

    @pytest.mark.asyncio
    async def test_cursor_fetch_all(
        self, session: ydb.aio.QuerySession
    ) -> None:
        async with ydb_dbapi.AsyncCursor(session=session) as cursor:
            yql_text = """
            SELECT id, val FROM table
            """
            await cursor.execute(query=yql_text)

            assert cursor.rowcount == 4

            res = await cursor.fetchall()
            assert res is not None
            assert len(res) == 4
            for i in range(4):
                assert res[i][0] == i + 1

            assert await cursor.fetchall() is None

    @pytest.mark.asyncio
    async def test_cursor_next_set(
        self, session: ydb.aio.QuerySession
    ) -> None:
        async with ydb_dbapi.AsyncCursor(session=session) as cursor:
            yql_text = """SELECT 1 as val; SELECT 2 as val;"""
            await cursor.execute(query=yql_text)

            res = await cursor.fetchall()
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 1

            nextset = await cursor.nextset()
            assert nextset

            res = await cursor.fetchall()
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 2

            nextset = await cursor.nextset()
            assert nextset

            assert await cursor.fetchall() is None

            nextset = await cursor.nextset()
            assert not nextset


# The same test class as above but for Cursor


class TestCursor:
    def test_cursor_fetch_one(self, session_sync: ydb.QuerySession) -> None:
        with ydb_dbapi.Cursor(session=session_sync) as cursor:
            yql_text = """
            SELECT id, val FROM table
            """
            cursor.execute(query=yql_text)

            for i in range(4):
                res = cursor.fetchone()
                assert res is not None
                assert res[0] == i + 1

            assert cursor.fetchone() is None

    def test_cursor_fetch_many(self, session_sync: ydb.QuerySession) -> None:
        with ydb_dbapi.Cursor(session=session_sync) as cursor:
            yql_text = """
            SELECT id, val FROM table
            """
            cursor.execute(query=yql_text)

            res = cursor.fetchmany()
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 1

            res = cursor.fetchmany(size=2)
            assert res is not None
            assert len(res) == 2
            assert res[0][0] == 2
            assert res[1][0] == 3

            res = cursor.fetchmany(size=2)
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 4

            assert cursor.fetchmany(size=2) is None

    def test_cursor_fetch_all(self, session_sync: ydb.QuerySession) -> None:
        with ydb_dbapi.Cursor(session=session_sync) as cursor:
            yql_text = """
            SELECT id, val FROM table
            """
            cursor.execute(query=yql_text)

            assert cursor.rowcount == 4

            res = cursor.fetchall()
            assert res is not None
            assert len(res) == 4
            for i in range(4):
                assert res[i][0] == i + 1

            assert cursor.fetchall() is None

    def test_cursor_next_set(self, session_sync: ydb.QuerySession) -> None:
        with ydb_dbapi.Cursor(session=session_sync) as cursor:
            yql_text = """SELECT 1 as val; SELECT 2 as val;"""
            cursor.execute(query=yql_text)

            res = cursor.fetchall()
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 1

            nextset = cursor.nextset()
            assert nextset

            res = cursor.fetchall()
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 2

            nextset = cursor.nextset()
            assert nextset

            assert cursor.fetchall() is None

            nextset = cursor.nextset()
            assert not nextset
