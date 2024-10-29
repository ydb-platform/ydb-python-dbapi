from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Generator
from contextlib import suppress
from inspect import iscoroutine

import pytest
import pytest_asyncio
import ydb
import ydb_dbapi as dbapi
from sqlalchemy.util import await_only
from sqlalchemy.util import greenlet_spawn


def maybe_await(obj: callable) -> any:
    if not iscoroutine(obj):
        return obj
    return await_only(obj)


class BaseDBApiTestSuit:
    def _test_isolation_level_read_only(
        self,
        connection: dbapi.Connection,
        isolation_level: str,
        read_only: bool,
    ) -> None:
        connection.set_isolation_level("AUTOCOMMIT")
        cursor = connection.cursor()
        with suppress(dbapi.DatabaseError):
            maybe_await(cursor.execute("DROP TABLE foo"))

        cursor = connection.cursor()
        maybe_await(cursor.execute(
            "CREATE TABLE foo(id Int64 NOT NULL, PRIMARY KEY (id))"
        ))

        connection.set_isolation_level(isolation_level)
        cursor = connection.cursor()

        query = "UPSERT INTO foo(id) VALUES (1)"
        if read_only:
            with pytest.raises(dbapi.DatabaseError):
                maybe_await(cursor.execute(query))

        else:
            maybe_await(cursor.execute(query))

        maybe_await(connection.rollback())

        connection.set_isolation_level("AUTOCOMMIT")

        cursor = connection.cursor()

        maybe_await(cursor.execute("DROP TABLE foo"))

    def _test_connection(self, connection: dbapi.Connection) -> None:
        maybe_await(connection.commit())
        maybe_await(connection.rollback())

        cur = connection.cursor()
        with suppress(dbapi.DatabaseError):
            maybe_await(cur.execute("DROP TABLE foo"))

        assert not maybe_await(connection.check_exists("/local/foo"))
        with pytest.raises(dbapi.ProgrammingError):
            maybe_await(connection.describe("/local/foo"))

        maybe_await(cur.execute(
            "CREATE TABLE foo(id Int64 NOT NULL, PRIMARY KEY (id))"
        ))

        assert maybe_await(connection.check_exists("/local/foo"))

        col = (maybe_await(connection.describe("/local/foo"))).columns[0]
        assert col.name == "id"
        assert col.type == ydb.PrimitiveType.Int64

        maybe_await(cur.execute("DROP TABLE foo"))
        maybe_await(cur.close())

    def _test_cursor_raw_query(self, connection: dbapi.Connection) -> None:
        cur = connection.cursor()
        assert cur

        with suppress(dbapi.DatabaseError):
            maybe_await(cur.execute("DROP TABLE test"))

        maybe_await(cur.execute(
            "CREATE TABLE test(id Int64 NOT NULL, text Utf8, PRIMARY KEY (id))"
        ))

        maybe_await(cur.execute(
            """
            DECLARE $data AS List<Struct<id:Int64, text: Utf8>>;

            INSERT INTO test SELECT id, text FROM AS_TABLE($data);
            """,
            {
                "$data": ydb.TypedValue(
                    [
                        {"id": 17, "text": "seventeen"},
                        {"id": 21, "text": "twenty one"},
                    ],
                    ydb.ListType(
                        ydb.StructType()
                        .add_member("id", ydb.PrimitiveType.Int64)
                        .add_member("text", ydb.PrimitiveType.Utf8)
                    ),
                )
            },
        ))

        maybe_await(cur.execute("DROP TABLE test"))

        maybe_await(cur.close())

    def _test_errors(
        self,
        connection: dbapi.Connection,
        connect_method: callable = dbapi.connect
    ) -> None:
        with pytest.raises(dbapi.InterfaceError):
            maybe_await(connect_method(
                "localhost:2136",  # type: ignore
                database="/local666",  # type: ignore
            ))

        cur = connection.cursor()

        with suppress(dbapi.DatabaseError):
            maybe_await(cur.execute("DROP TABLE test"))

        with pytest.raises(dbapi.DataError):
            maybe_await(cur.execute("SELECT 18446744073709551616"))

        with pytest.raises(dbapi.DataError):
            maybe_await(cur.execute("SELECT * FROM 拉屎"))

        with pytest.raises(dbapi.DataError):
            maybe_await(cur.execute("SELECT floor(5 / 2)"))

        with pytest.raises(dbapi.ProgrammingError):
            maybe_await(cur.execute("SELECT * FROM test"))

        maybe_await(cur.execute(
            "CREATE TABLE test(id Int64, PRIMARY KEY (id))"
        ))

        maybe_await(cur.execute("INSERT INTO test(id) VALUES(1)"))

        with pytest.raises(dbapi.IntegrityError):
            maybe_await(cur.execute("INSERT INTO test(id) VALUES(1)"))

        maybe_await(cur.execute("DROP TABLE test"))
        maybe_await(cur.close())


class TestConnection(BaseDBApiTestSuit):
    @pytest.fixture
    def connection(
        self, connection_kwargs: dict
    ) -> Generator[dbapi.Connection]:
        conn = dbapi.connect(**connection_kwargs)  # ignore: typing
        try:
            yield conn
        finally:
            conn.close()

    @pytest.mark.parametrize(
        ("isolation_level", "read_only"),
        [
            (dbapi.IsolationLevel.SERIALIZABLE, False),
            (dbapi.IsolationLevel.AUTOCOMMIT, False),
            (dbapi.IsolationLevel.ONLINE_READONLY, True),
            (dbapi.IsolationLevel.ONLINE_READONLY_INCONSISTENT, True),
            (dbapi.IsolationLevel.STALE_READONLY, True),
            (dbapi.IsolationLevel.SNAPSHOT_READONLY, True),
        ],
    )
    def test_isolation_level_read_only(
        self,
        isolation_level: str,
        read_only: bool,
        connection: dbapi.Connection,
    ) -> None:
        self._test_isolation_level_read_only(
            connection, isolation_level, read_only
        )

    def test_connection(self, connection: dbapi.Connection) -> None:
        self._test_connection(connection)

    def test_cursor_raw_query(self, connection: dbapi.Connection) -> None:
        self._test_cursor_raw_query(connection)

    def test_errors(self, connection: dbapi.Connection) -> None:
        self._test_errors(connection)


class TestAsyncConnection(BaseDBApiTestSuit):
    @pytest_asyncio.fixture
    async def connection(
        self, connection_kwargs: dict
    ) -> AsyncGenerator[dbapi.AsyncConnection]:
        def connect() -> dbapi.AsyncConnection:
            return maybe_await(dbapi.async_connect(**connection_kwargs))

        conn = await greenlet_spawn(connect)
        try:
            yield conn
        finally:
            await greenlet_spawn(conn.close)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("isolation_level", "read_only"),
        [
            (dbapi.IsolationLevel.SERIALIZABLE, False),
            (dbapi.IsolationLevel.AUTOCOMMIT, False),
            (dbapi.IsolationLevel.ONLINE_READONLY, True),
            (dbapi.IsolationLevel.ONLINE_READONLY_INCONSISTENT, True),
            (dbapi.IsolationLevel.STALE_READONLY, True),
            (dbapi.IsolationLevel.SNAPSHOT_READONLY, True),
        ],
    )
    async def test_isolation_level_read_only(
        self,
        isolation_level: str,
        read_only: bool,
        connection: dbapi.AsyncConnection,
    ) -> None:
        await greenlet_spawn(
            self._test_isolation_level_read_only,
            connection, isolation_level, read_only
        )

    @pytest.mark.asyncio
    async def test_connection(self, connection: dbapi.AsyncConnection) -> None:
        await greenlet_spawn(self._test_connection, connection)

    @pytest.mark.asyncio
    async def test_cursor_raw_query(
        self, connection: dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(self._test_cursor_raw_query, connection)

    @pytest.mark.asyncio
    async def test_errors(self, connection: dbapi.AsyncConnection) -> None:
        await greenlet_spawn(self._test_errors, connection)
