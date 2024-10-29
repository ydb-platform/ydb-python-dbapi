import pytest
import ydb_dbapi
from ydb import QuerySession as QuerySessionSync
from ydb.aio import QuerySession


class TestAsyncCursor:
    @pytest.mark.asyncio
    async def test_cursor_ddl(self, session: QuerySession) -> None:
        cursor = ydb_dbapi.AsyncCursor(session=session)

        yql = """
            CREATE TABLE table (
            id Int64 NOT NULL,
            val Int64,
            PRIMARY KEY(id)
            )
        """

        with pytest.raises(ydb_dbapi.Error):
            await cursor.execute(query=yql)

        yql = """
        DROP TABLE table
        """

        await cursor.execute(query=yql)

        assert cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_dml(self, session: QuerySession) -> None:
        cursor = ydb_dbapi.AsyncCursor(session=session)
        yql_text = """
        INSERT INTO table (id, val) VALUES
        (1, 1),
        (2, 2),
        (3, 3)
        """

        await cursor.execute(query=yql_text)
        assert cursor.fetchone() is None

        cursor = ydb_dbapi.AsyncCursor(session=session)

        yql_text = """
        SELECT COUNT(*) FROM table as sum
        """

        await cursor.execute(query=yql_text)

        res = cursor.fetchone()
        assert res is not None
        assert len(res) == 1
        assert res[0] == 3

    @pytest.mark.asyncio
    async def test_cursor_fetch_one(self, session: QuerySession) -> None:
        cursor = ydb_dbapi.AsyncCursor(session=session)
        yql_text = """
        INSERT INTO table (id, val) VALUES
        (1, 1),
        (2, 2)
        """

        await cursor.execute(query=yql_text)
        assert cursor.fetchone() is None

        cursor = ydb_dbapi.AsyncCursor(session=session)

        yql_text = """
        SELECT id, val FROM table
        """

        await cursor.execute(query=yql_text)

        res = cursor.fetchone()
        assert res is not None
        assert res[0] == 1

        res = cursor.fetchone()
        assert res is not None
        assert res[0] == 2

        assert cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_fetch_many(self, session: QuerySession) -> None:
        cursor = ydb_dbapi.AsyncCursor(session=session)
        yql_text = """
        INSERT INTO table (id, val) VALUES
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4)
        """

        await cursor.execute(query=yql_text)
        assert cursor.fetchone() is None

        cursor = ydb_dbapi.AsyncCursor(session=session)

        yql_text = """
        SELECT id, val FROM table
        """

        await cursor.execute(query=yql_text)

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

    @pytest.mark.asyncio
    async def test_cursor_fetch_all(self, session: QuerySession) -> None:
        cursor = ydb_dbapi.AsyncCursor(session=session)
        yql_text = """
        INSERT INTO table (id, val) VALUES
        (1, 1),
        (2, 2),
        (3, 3)
        """

        await cursor.execute(query=yql_text)
        assert cursor.fetchone() is None

        cursor = ydb_dbapi.AsyncCursor(session=session)

        yql_text = """
        SELECT id, val FROM table
        """

        await cursor.execute(query=yql_text)

        assert cursor.rowcount == 3

        res = cursor.fetchall()
        assert res is not None
        assert len(res) == 3
        assert res[0][0] == 1
        assert res[1][0] == 2
        assert res[2][0] == 3

        assert cursor.fetchall() is None

    @pytest.mark.asyncio
    async def test_cursor_next_set(self, session: QuerySession) -> None:
        cursor = ydb_dbapi.AsyncCursor(session=session)
        yql_text = """SELECT 1 as val; SELECT 2 as val;"""

        await cursor.execute(query=yql_text)

        res = cursor.fetchall()
        assert res is not None
        assert len(res) == 1
        assert res[0][0] == 1

        nextset = await cursor.nextset()
        assert nextset

        res = cursor.fetchall()
        assert res is not None
        assert len(res) == 1
        assert res[0][0] == 2

        nextset = await cursor.nextset()
        assert nextset

        assert cursor.fetchall() is None

        nextset = await cursor.nextset()
        assert not nextset


# The same test class as above but for Cursor


class TestCursor:
    def test_cursor_ddl(self, session_sync: QuerySessionSync) -> None:
        cursor = ydb_dbapi.Cursor(session=session_sync)

        yql = """
            CREATE TABLE table (
            id Int64 NOT NULL,
            val Int64,
            PRIMARY KEY(id)
            )
        """

        with pytest.raises(ydb_dbapi.Error):
            cursor.execute(query=yql)

        yql = """
        DROP TABLE table
        """

        cursor.execute(query=yql)

        assert cursor.fetchone() is None

    def test_cursor_dml(self, session_sync: QuerySessionSync) -> None:
        cursor = ydb_dbapi.Cursor(session=session_sync)
        yql_text = """
        INSERT INTO table (id, val) VALUES
        (1, 1),
        (2, 2),
        (3, 3)
        """

        cursor.execute(query=yql_text)
        assert cursor.fetchone() is None

        cursor = ydb_dbapi.Cursor(session=session_sync)

        yql_text = """
        SELECT COUNT(*) FROM table as sum
        """

        cursor.execute(query=yql_text)

        res = cursor.fetchone()
        assert res is not None
        assert len(res) == 1
        assert res[0] == 3

    def test_cursor_fetch_one(self, session_sync: QuerySessionSync) -> None:
        cursor = ydb_dbapi.Cursor(session=session_sync)
        yql_text = """
        INSERT INTO table (id, val) VALUES
        (1, 1),
        (2, 2)
        """

        cursor.execute(query=yql_text)
        assert cursor.fetchone() is None

        cursor = ydb_dbapi.Cursor(session=session_sync)

        yql_text = """
        SELECT id, val FROM table
        """

        cursor.execute(query=yql_text)

        res = cursor.fetchone()
        assert res is not None
        assert res[0] == 1

        res = cursor.fetchone()
        assert res is not None
        assert res[0] == 2

        assert cursor.fetchone() is None

    def test_cursor_fetch_many(self, session_sync: QuerySessionSync) -> None:
        cursor = ydb_dbapi.Cursor(session=session_sync)
        yql_text = """
        INSERT INTO table (id, val) VALUES
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4)
        """

        cursor.execute(query=yql_text)
        assert cursor.fetchone() is None

        cursor = ydb_dbapi.Cursor(session=session_sync)

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

    def test_cursor_fetch_all(self, session_sync: QuerySessionSync) -> None:
        cursor = ydb_dbapi.Cursor(session=session_sync)
        yql_text = """
        INSERT INTO table (id, val) VALUES
        (1, 1),
        (2, 2),
        (3, 3)
        """

        cursor.execute(query=yql_text)
        assert cursor.fetchone() is None

        cursor = ydb_dbapi.Cursor(session=session_sync)

        yql_text = """
        SELECT id, val FROM table
        """

        cursor.execute(query=yql_text)

        assert cursor.rowcount == 3

        res = cursor.fetchall()
        assert res is not None
        assert len(res) == 3
        assert res[0][0] == 1
        assert res[1][0] == 2
        assert res[2][0] == 3

        assert cursor.fetchall() is None

    def test_cursor_next_set(self, session_sync: QuerySessionSync) -> None:
        cursor = ydb_dbapi.Cursor(session=session_sync)
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
