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
        cursor = connection.cursor()
        with suppress(dbapi.DatabaseError):
            maybe_await(cursor.execute_scheme("DROP TABLE foo"))
        maybe_await(
            cursor.execute_scheme(
                "CREATE TABLE foo(id Int64 NOT NULL, PRIMARY KEY (id))"
            )
        )

        connection.set_isolation_level(isolation_level)
        cursor = connection.cursor()
        maybe_await(connection.begin())

        query = "UPSERT INTO foo(id) VALUES (1)"
        if read_only:
            with pytest.raises(dbapi.DatabaseError):
                maybe_await(cursor.execute(query))
        else:
            maybe_await(cursor.execute(query))

        maybe_await(connection.rollback())

        maybe_await(cursor.execute_scheme("DROP TABLE foo"))

    def _test_commit_rollback_after_begin(
        self,
        connection: dbapi.Connection,
        isolation_level: str,
    ) -> None:
        connection.set_isolation_level(isolation_level)

        for _ in range(10):
            maybe_await(connection.begin())
            maybe_await(connection.commit())

        for _ in range(10):
            maybe_await(connection.begin())
            maybe_await(connection.rollback())


    def _test_connection(self, connection: dbapi.Connection) -> None:
        maybe_await(connection.commit())
        maybe_await(connection.rollback())

        cur = connection.cursor()
        with suppress(dbapi.DatabaseError):
            maybe_await(cur.execute_scheme("DROP TABLE foo"))

        assert not maybe_await(connection.check_exists("/local/foo"))
        with pytest.raises(dbapi.ProgrammingError):
            maybe_await(connection.describe("/local/foo"))

        maybe_await(
            cur.execute_scheme(
                "CREATE TABLE foo(id Int64 NOT NULL, PRIMARY KEY (id))"
            )
        )

        assert maybe_await(connection.check_exists("/local/foo"))

        col = (maybe_await(connection.describe("/local/foo"))).columns[0]
        assert col.name == "id"
        assert col.type == ydb.PrimitiveType.Int64

        maybe_await(cur.execute_scheme("DROP TABLE foo"))
        maybe_await(cur.close())

    def _test_cursor_raw_query(self, connection: dbapi.Connection) -> None:
        cur = connection.cursor()
        assert cur

        with suppress(dbapi.DatabaseError):
            maybe_await(cur.execute_scheme("DROP TABLE test"))

        maybe_await(cur.execute_scheme(
            "CREATE TABLE test(id Int64 NOT NULL, text Utf8, PRIMARY KEY (id))"
        ))

        maybe_await(
            cur.execute(
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
            )
        )

        maybe_await(cur.execute_scheme("DROP TABLE test"))

        maybe_await(cur.close())

    def _test_errors(
        self,
        connection: dbapi.Connection,
        connect_method: callable = dbapi.connect,
    ) -> None:
        with pytest.raises(dbapi.InterfaceError):
            maybe_await(
                connect_method(
                    "localhost:2136",  # type: ignore
                    database="/local666",  # type: ignore
                )
            )

        cur = connection.cursor()

        with suppress(dbapi.DatabaseError):
            maybe_await(cur.execute_scheme("DROP TABLE test"))

        with pytest.raises(dbapi.DataError):
            maybe_await(cur.execute("SELECT 18446744073709551616"))

        with pytest.raises(dbapi.DataError):
            maybe_await(cur.execute("SELECT * FROM 拉屎"))

        with pytest.raises(dbapi.DataError):
            maybe_await(cur.execute("SELECT floor(5 / 2)"))

        with pytest.raises(dbapi.ProgrammingError):
            maybe_await(cur.execute("SELECT * FROM test"))

        maybe_await(
            cur.execute_scheme("CREATE TABLE test(id Int64, PRIMARY KEY (id))")
        )

        maybe_await(cur.execute("INSERT INTO test(id) VALUES(1)"))

        with pytest.raises(dbapi.IntegrityError):
            maybe_await(cur.execute("INSERT INTO test(id) VALUES(1)"))

        maybe_await(cur.execute_scheme("DROP TABLE test"))
        maybe_await(cur.close())

    def _test_bulk_upsert(self, connection: dbapi.Connection) -> None:
        cursor = connection.cursor()
        with suppress(dbapi.DatabaseError):
            maybe_await(cursor.execute_scheme("DROP TABLE pet"))

        maybe_await(
            cursor.execute_scheme(
                """
            CREATE TABLE pet (
            pet_id INT,
            name TEXT NOT NULL,
            pet_type TEXT NOT NULL,
            birth_date TEXT NOT NULL,
            owner TEXT NOT NULL,
            PRIMARY KEY (pet_id)
            );
            """
            )
        )

        column_types = (
            ydb.BulkUpsertColumns()
            .add_column("pet_id", ydb.OptionalType(ydb.PrimitiveType.Int32))
            .add_column("name", ydb.PrimitiveType.Utf8)
            .add_column("pet_type", ydb.PrimitiveType.Utf8)
            .add_column("birth_date", ydb.PrimitiveType.Utf8)
            .add_column("owner", ydb.PrimitiveType.Utf8)
        )

        rows = [
            {
                "pet_id": 3,
                "name": "Lester",
                "pet_type": "Hamster",
                "birth_date": "2020-06-23",
                "owner": "Lily",
            },
            {
                "pet_id": 4,
                "name": "Quincy",
                "pet_type": "Parrot",
                "birth_date": "2013-08-11",
                "owner": "Anne",
            },
        ]

        maybe_await(connection.bulk_upsert("pet", rows, column_types))

        maybe_await(cursor.execute("SELECT * FROM pet"))
        assert cursor.rowcount == 2

        maybe_await(cursor.execute_scheme("DROP TABLE pet"))

    def _test_error_with_interactive_tx(
        self,
        connection: dbapi.Connection,
    ) -> None:
        cur = connection.cursor()
        maybe_await(
            cur.execute_scheme(
                """
            DROP TABLE IF EXISTS test;
            CREATE TABLE test (
            id Int64 NOT NULL,
            val Int64,
            PRIMARY KEY(id)
            )
            """
            )
        )

        connection.set_isolation_level(dbapi.IsolationLevel.SERIALIZABLE)
        maybe_await(connection.begin())

        cur = connection.cursor()
        maybe_await(cur.execute("INSERT INTO test(id, val) VALUES (1,1)"))
        with pytest.raises(dbapi.Error):
            maybe_await(cur.execute("INSERT INTO test(id, val) VALUES (1,1)"))

        maybe_await(
            cur.execute_scheme(
                """
                DROP TABLE IF EXISTS test;
                """
            )
        )

        maybe_await(cur.close())
        maybe_await(connection.rollback())

    def _test_get_view_names(
        self,
        connection: dbapi.Connection,
    ) -> None:
        cur = connection.cursor()

        maybe_await(
            cur.execute_scheme(
                """
                DROP VIEW if exists test_view;
                """
            )
        )

        res = maybe_await(connection.get_view_names())

        assert len(res) == 0

        maybe_await(
            cur.execute_scheme(
                """
                CREATE VIEW test_view WITH (security_invoker = TRUE) as (
                    select 1 as res
                );
                """
            )
        )

        res = maybe_await(connection.get_view_names())

        assert len(res) == 1
        assert res[0] == "test_view"

        maybe_await(
            cur.execute_scheme(
                """
                DROP VIEW test_view;
                """
            )
        )

        maybe_await(cur.close())

    def _test_get_table_names(
        self,
        connection: dbapi.Connection,
    ) -> None:
        cur = connection.cursor()

        row_table_name = "test_table_names_row"
        column_table_name = "test_table_names_column"

        maybe_await(
            cur.execute_scheme(
                f"""
                DROP TABLE if exists {row_table_name};
                DROP TABLE if exists {column_table_name};
                """
            )
        )

        res = maybe_await(connection.get_table_names())

        assert len(res) == 0

        maybe_await(
            cur.execute_scheme(
                f"""
                CREATE TABLE {row_table_name} (
                    id Utf8 NOT NULL,
                    PRIMARY KEY(id)
                );
                CREATE TABLE {column_table_name} (
                    id Utf8 NOT NULL,
                    PRIMARY KEY(id)
                ) WITH (STORE = COLUMN);
                """
            )
        )

        res = maybe_await(connection.get_table_names())

        assert len(res) == 2
        assert row_table_name in res
        assert column_table_name in res

        maybe_await(
            cur.execute_scheme(
                f"""
                DROP TABLE {row_table_name};
                DROP TABLE {column_table_name};
                """
            )
        )

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

    @pytest.mark.parametrize(
        ("isolation_level"),
        [
            (dbapi.IsolationLevel.SERIALIZABLE),
            (dbapi.IsolationLevel.AUTOCOMMIT),
            (dbapi.IsolationLevel.ONLINE_READONLY),
            (dbapi.IsolationLevel.ONLINE_READONLY_INCONSISTENT),
            (dbapi.IsolationLevel.STALE_READONLY),
            (dbapi.IsolationLevel.SNAPSHOT_READONLY),
        ],
    )
    def test_commit_rollback_after_begin(
        self,
        isolation_level: str,
        connection: dbapi.Connection,
    ) -> None:
        self._test_commit_rollback_after_begin(
            connection, isolation_level
        )

    def test_connection(self, connection: dbapi.Connection) -> None:
        self._test_connection(connection)

    def test_cursor_raw_query(self, connection: dbapi.Connection) -> None:
        self._test_cursor_raw_query(connection)

    def test_errors(self, connection: dbapi.Connection) -> None:
        self._test_errors(connection)

    def test_bulk_upsert(self, connection: dbapi.Connection) -> None:
        self._test_bulk_upsert(connection)

    def test_errors_with_interactive_tx(
        self, connection: dbapi.Connection
    ) -> None:
        self._test_error_with_interactive_tx(connection)

    def test_get_view_names(
        self, connection: dbapi.Connection
    ) -> None:
        self._test_get_view_names(connection)

    def test_get_table_names(
        self, connection: dbapi.Connection
    ) -> None:
        self._test_get_table_names(connection)


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

            def close() -> None:
                maybe_await(conn.close())

            await greenlet_spawn(close)

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
            connection,
            isolation_level,
            read_only,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("isolation_level"),
        [
            (dbapi.IsolationLevel.SERIALIZABLE),
            (dbapi.IsolationLevel.AUTOCOMMIT),
            (dbapi.IsolationLevel.ONLINE_READONLY),
            (dbapi.IsolationLevel.ONLINE_READONLY_INCONSISTENT),
            (dbapi.IsolationLevel.STALE_READONLY),
            (dbapi.IsolationLevel.SNAPSHOT_READONLY),
        ],
    )
    async def test_commit_rollback_after_begin(
        self,
        isolation_level: str,
        connection: dbapi.AsyncConnection,
    ) -> None:
        await greenlet_spawn(
            self._test_commit_rollback_after_begin,
            connection,
            isolation_level
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

    @pytest.mark.asyncio
    async def test_bulk_upsert(
        self, connection: dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(self._test_bulk_upsert, connection)

    @pytest.mark.asyncio
    async def test_errors_with_interactive_tx(
        self, connection: dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(self._test_error_with_interactive_tx, connection)

    @pytest.mark.asyncio
    async def test_get_view_names(
        self, connection: dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(self._test_get_view_names, connection)

    @pytest.mark.asyncio
    async def test_get_table_names(
        self, connection: dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(self._test_get_table_names, connection)
