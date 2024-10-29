from contextlib import suppress

import pytest
import pytest_asyncio
import ydb
import ydb_dbapi as dbapi


class BaseDBApiTestSuit:
    async def _test_isolation_level_read_only(
        self,
        connection: dbapi.Connection,
        isolation_level: str,
        read_only: bool,
    ) -> None:
        async with connection.cursor() as cursor:
            with suppress(dbapi.DatabaseError):
                await cursor.execute("DROP TABLE foo")

        async with connection.cursor() as cursor:
            await cursor.execute(
                "CREATE TABLE foo(id Int64 NOT NULL, PRIMARY KEY (id))"
            )

        connection.set_isolation_level(isolation_level)

        async with connection.cursor() as cursor:
            query = "UPSERT INTO foo(id) VALUES (1)"
            if read_only:
                with pytest.raises(dbapi.DatabaseError):
                    await cursor.execute(query)
                    await cursor.finish_query()

            else:
                await cursor.execute(query)

        await connection.rollback()

        async with connection.cursor() as cursor:
            cursor.execute("DROP TABLE foo")

    async def _test_connection(self, connection: dbapi.Connection) -> None:
        await connection.commit()
        await connection.rollback()

        cur = connection.cursor()
        with suppress(dbapi.DatabaseError):
            await cur.execute("DROP TABLE foo")
            await cur.finish_query()

        assert not await connection.check_exists("/local/foo")
        with pytest.raises(dbapi.ProgrammingError):
            await connection.describe("/local/foo")

        await cur.execute(
            "CREATE TABLE foo(id Int64 NOT NULL, PRIMARY KEY (id))"
        )
        await cur.finish_query()

        assert await connection.check_exists("/local/foo")

        col = (await connection.describe("/local/foo")).columns[0]
        assert col.name == "id"
        assert col.type == ydb.PrimitiveType.Int64

        await cur.execute("DROP TABLE foo")
        await cur.close()

    async def _test_cursor_raw_query(self, connection: dbapi.Connection) -> None:
        cur = connection.cursor()
        assert cur

        with suppress(dbapi.DatabaseError):
            await cur.execute("DROP TABLE test")
            await cur.finish_query()

        await cur.execute(
            "CREATE TABLE test(id Int64 NOT NULL, text Utf8, PRIMARY KEY (id))"
        )
        await cur.finish_query()

        await cur.execute(
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
        await cur.finish_query()

        await cur.execute("DROP TABLE test")

        await cur.close()

    async def _test_errors(self, connection: dbapi.Connection) -> None:
        with pytest.raises(dbapi.InterfaceError):
            await dbapi.connect("localhost:2136", database="/local666")

        cur = connection.cursor()

        with suppress(dbapi.DatabaseError):
            await cur.execute("DROP TABLE test")
            await cur.finish_query()

        with pytest.raises(dbapi.DataError):
            await cur.execute("SELECT 18446744073709551616")

        with pytest.raises(dbapi.DataError):
            await cur.execute("SELECT * FROM 拉屎")

        with pytest.raises(dbapi.DataError):
            await cur.execute("SELECT floor(5 / 2)")

        with pytest.raises(dbapi.ProgrammingError):
            await cur.execute("SELECT * FROM test")

        await cur.execute("CREATE TABLE test(id Int64, PRIMARY KEY (id))")
        await cur.finish_query()

        await cur.execute("INSERT INTO test(id) VALUES(1)")
        await cur.finish_query()

        with pytest.raises(dbapi.IntegrityError):
            await cur.execute("INSERT INTO test(id) VALUES(1)")

        await cur.execute("DROP TABLE test")
        await cur.close()


class TestAsyncConnection(BaseDBApiTestSuit):
    @pytest_asyncio.fixture
    async def connection(self, connection_kwargs):
        conn = await dbapi.connect(**connection_kwargs)
        try:
            yield conn
        finally:
            await conn.close()

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
        connection: dbapi.Connection,
    ):
        await self._test_isolation_level_read_only(
            connection, isolation_level, read_only
        )

    @pytest.mark.asyncio
    async def test_connection(self, connection: dbapi.Connection):
        await self._test_connection(connection)

    @pytest.mark.asyncio
    async def test_cursor_raw_query(self, connection: dbapi.Connection):
        await self._test_cursor_raw_query(connection)

    @pytest.mark.asyncio
    async def test_errors(self, connection: dbapi.Connection):
        await self._test_errors(connection)
