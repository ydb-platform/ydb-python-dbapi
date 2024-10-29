from collections.abc import AsyncGenerator
from collections.abc import Generator

import pytest
import ydb
import ydb_dbapi


@pytest.fixture
async def session(
    session_pool: ydb.aio.QuerySessionPool,
) -> AsyncGenerator[ydb.aio.QuerySession]:
    for name in ["table", "table1", "table2"]:
        await session_pool.execute_with_retries(
            f"""
            DELETE FROM {name};
            INSERT INTO {name} (id, val) VALUES
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 3)
            """
        )

    session = await session_pool.acquire()
    yield session
    await session_pool.release(session)


@pytest.fixture
def session_sync(
    session_pool_sync: ydb.QuerySessionPool,
) -> Generator[ydb.QuerySession]:
    for name in ["table", "table1", "table2"]:
        session_pool_sync.execute_with_retries(
            f"""
            DELETE FROM {name};
            INSERT INTO {name} (id, val) VALUES
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 3)
            """
        )

    session = session_pool_sync.acquire()
    yield session
    session_pool_sync.release(session)


RESULT_SET_LENGTH = 4
RESULT_SET_COUNT = 3


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

            for i in range(RESULT_SET_LENGTH):
                res = await cursor.fetchone()
                assert res is not None
                assert res[0] == i

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
            assert res[0][0] == 0

            res = await cursor.fetchmany(size=2)
            assert res is not None
            assert len(res) == 2
            assert res[0][0] == 1
            assert res[1][0] == 2

            res = await cursor.fetchmany(size=2)
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 3

            assert await cursor.fetchmany(size=2) == []

    @pytest.mark.asyncio
    async def test_cursor_fetch_all(
        self, session: ydb.aio.QuerySession
    ) -> None:
        async with ydb_dbapi.AsyncCursor(session=session) as cursor:
            yql_text = """
            SELECT id, val FROM table
            """
            await cursor.execute(query=yql_text)

            assert cursor.rowcount == RESULT_SET_LENGTH

            res = await cursor.fetchall()
            assert res is not None
            assert len(res) == RESULT_SET_LENGTH
            for i in range(RESULT_SET_LENGTH):
                assert res[i][0] == i

            assert await cursor.fetchall() == []

    @pytest.mark.asyncio
    async def test_cursor_fetch_one_multiple_result_sets(
        self, session: ydb.aio.QuerySession
    ) -> None:
        async with ydb_dbapi.AsyncCursor(session=session) as cursor:
            yql_text = """
            SELECT id, val FROM table;
            SELECT id, val FROM table1;
            SELECT id, val FROM table2;
            """
            await cursor.execute(query=yql_text)

            assert cursor.rowcount == 12

            for i in range(RESULT_SET_LENGTH * RESULT_SET_COUNT):
                res = await cursor.fetchone()
                assert res is not None
                assert res[0] == i % RESULT_SET_LENGTH

            assert await cursor.fetchone() is None
            assert not await cursor.nextset()

    @pytest.mark.asyncio
    async def test_cursor_fetch_many_multiple_result_sets(
        self, session: ydb.aio.QuerySession
    ) -> None:
        async with ydb_dbapi.AsyncCursor(session=session) as cursor:
            yql_text = """
            SELECT id, val FROM table;
            SELECT id, val FROM table1;
            SELECT id, val FROM table2;
            """
            await cursor.execute(query=yql_text)

            assert cursor.rowcount == 12

            halfsize = (RESULT_SET_LENGTH * RESULT_SET_COUNT) // 2
            for _ in range(2):
                res = await cursor.fetchmany(size=halfsize)
                assert res is not None
                assert len(res) == halfsize

            assert await cursor.fetchmany(2) == []
            assert not await cursor.nextset()

    @pytest.mark.asyncio
    async def test_cursor_fetch_all_multiple_result_sets(
        self, session: ydb.aio.QuerySession
    ) -> None:
        async with ydb_dbapi.AsyncCursor(session=session) as cursor:
            yql_text = """
            SELECT id, val FROM table;
            SELECT id, val FROM table1;
            SELECT id, val FROM table2;
            """
            await cursor.execute(query=yql_text)

            assert cursor.rowcount == 12

            res = await cursor.fetchall()

            assert len(res) == RESULT_SET_COUNT * RESULT_SET_LENGTH

            assert await cursor.fetchall() == []
            assert not await cursor.nextset()


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
                assert res[0] == i

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
            assert res[0][0] == 0

            res = cursor.fetchmany(size=2)
            assert res is not None
            assert len(res) == 2
            assert res[0][0] == 1
            assert res[1][0] == 2

            res = cursor.fetchmany(size=2)
            assert res is not None
            assert len(res) == 1
            assert res[0][0] == 3

            assert cursor.fetchmany(size=2) == []

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
                assert res[i][0] == i

            assert cursor.fetchall() == []

    def test_cursor_fetch_one_multiple_result_sets(
        self, session_sync: ydb.QuerySession
    ) -> None:
        with ydb_dbapi.Cursor(session=session_sync) as cursor:
            yql_text = """
            SELECT id, val FROM table;
            SELECT id, val FROM table1;
            SELECT id, val FROM table2;
            """
            cursor.execute(query=yql_text)

            assert cursor.rowcount == 12

            for i in range(RESULT_SET_LENGTH * RESULT_SET_COUNT):
                res = cursor.fetchone()
                assert res is not None
                assert res[0] == i % RESULT_SET_LENGTH

            assert cursor.fetchone() is None
            assert not cursor.nextset()

    def test_cursor_fetch_many_multiple_result_sets(
        self, session_sync: ydb.QuerySession
    ) -> None:
        with ydb_dbapi.Cursor(session=session_sync) as cursor:
            yql_text = """
            SELECT id, val FROM table;
            SELECT id, val FROM table1;
            SELECT id, val FROM table2;
            """
            cursor.execute(query=yql_text)

            assert cursor.rowcount == 12

            halfsize = (RESULT_SET_LENGTH * RESULT_SET_COUNT) // 2
            for _ in range(2):
                res = cursor.fetchmany(size=halfsize)
                assert res is not None
                assert len(res) == halfsize

            assert cursor.fetchmany(2) == []
            assert not cursor.nextset()

    def test_cursor_fetch_all_multiple_result_sets(
        self, session_sync: ydb.QuerySession
    ) -> None:
        with ydb_dbapi.Cursor(session=session_sync) as cursor:
            yql_text = """
            SELECT id, val FROM table;
            SELECT id, val FROM table1;
            SELECT id, val FROM table2;
            """
            cursor.execute(query=yql_text)

            assert cursor.rowcount == 12

            res = cursor.fetchall()

            assert len(res) == RESULT_SET_COUNT * RESULT_SET_LENGTH

            assert cursor.fetchall() == []
            assert not cursor.nextset()
