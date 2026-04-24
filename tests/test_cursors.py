from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Generator
from inspect import iscoroutine

import pytest
import pytest_asyncio
import ydb
import ydb_dbapi
from sqlalchemy.util import await_only
from sqlalchemy.util import greenlet_spawn
from ydb_dbapi import AsyncCursor
from ydb_dbapi import Cursor
from ydb_dbapi.utils import CursorStatus


def maybe_await(obj: callable) -> any:
    if not iscoroutine(obj):
        return obj
    return await_only(obj)


RESULT_SET_LENGTH = 4
RESULT_SET_COUNT = 3


class FakeSyncConnection:
    def _clear_current_cursor(self, cursor: Cursor | None = None) -> None: ...

    def _invalidate_session(self) -> None: ...


class FakeAsyncConnection:
    def _clear_current_cursor(
        self, cursor: AsyncCursor | None = None
    ) -> None: ...

    async def _invalidate_session(self) -> None: ...


class BaseCursorTestSuit:
    def _test_cursor_fetch_one(self, cursor: Cursor | AsyncCursor) -> None:
        yql_text = """
        SELECT id, val FROM table
        """
        maybe_await(cursor.execute(query=yql_text))

        for i in range(4):
            res = cursor.fetchone()
            assert res is not None
            assert res[0] == i

        assert cursor.fetchone() is None

    def _test_cursor_fetch_many(self, cursor: Cursor | AsyncCursor) -> None:
        yql_text = """
        SELECT id, val FROM table
        """
        maybe_await(cursor.execute(query=yql_text))

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

    def _test_cursor_fetch_all(self, cursor: Cursor | AsyncCursor) -> None:
        yql_text = """
        SELECT id, val FROM table
        """
        maybe_await(cursor.execute(query=yql_text))

        assert cursor.rowcount == 4

        res = cursor.fetchall()
        assert res is not None
        assert len(res) == 4
        for i in range(4):
            assert res[i][0] == i

        assert cursor.fetchall() == []

    def _test_cursor_fetch_one_multiple_result_sets(
        self, cursor: Cursor | AsyncCursor
    ) -> None:
        yql_text = """
        SELECT id, val FROM table;
        SELECT id, val FROM table1;
        SELECT id, val FROM table2;
        """
        maybe_await(cursor.execute(query=yql_text))

        assert cursor.rowcount == 12

        for i in range(RESULT_SET_LENGTH * RESULT_SET_COUNT):
            res = cursor.fetchone()
            assert res is not None
            assert res[0] == i % RESULT_SET_LENGTH

        assert cursor.fetchone() is None
        assert not maybe_await(cursor.nextset())

    def _test_cursor_fetch_many_multiple_result_sets(
        self, cursor: Cursor | AsyncCursor
    ) -> None:
        yql_text = """
        SELECT id, val FROM table;
        SELECT id, val FROM table1;
        SELECT id, val FROM table2;
        """
        maybe_await(cursor.execute(query=yql_text))

        assert cursor.rowcount == 12

        halfsize = (RESULT_SET_LENGTH * RESULT_SET_COUNT) // 2
        for _ in range(2):
            res = cursor.fetchmany(size=halfsize)
            assert res is not None
            assert len(res) == halfsize

        assert cursor.fetchmany(2) == []
        assert not maybe_await(cursor.nextset())

    def _test_cursor_fetch_all_multiple_result_sets(
        self, cursor: Cursor | AsyncCursor
    ) -> None:
        yql_text = """
        SELECT id, val FROM table;
        SELECT id, val FROM table1;
        SELECT id, val FROM table2;
        """
        maybe_await(cursor.execute(query=yql_text))

        assert cursor.rowcount == 12

        res = cursor.fetchall()

        assert len(res) == RESULT_SET_COUNT * RESULT_SET_LENGTH

        assert cursor.fetchall() == []
        assert not maybe_await(cursor.nextset())

    def _test_cursor_state_after_error(
        self, cursor: Cursor | AsyncCursor
    ) -> None:
        query = "INSERT INTO table (id, val) VALUES (0,0)"
        with pytest.raises(ydb_dbapi.Error):
            maybe_await(cursor.execute(query=query))

        assert cursor._state == CursorStatus.finished


class BaseStreamCursorIntegrationTestSuit:
    def _make_multi_result_query(self, count: int) -> str:
        return ";\n".join(
            f'SELECT {index} AS id, CAST("{index}" AS Utf8) AS value'
            for index in range(count)
        )

    def _test_stream_cursor_blocks_shared_transaction_session(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        connection.set_isolation_level(ydb_dbapi.IsolationLevel.SERIALIZABLE)
        maybe_await(connection.begin())

        stream_cursor = connection.cursor(stream_results=True)
        other_cursor = connection.cursor()

        try:
            maybe_await(stream_cursor.execute("SELECT 1 AS id"))

            with pytest.raises(ydb_dbapi.ProgrammingError):
                maybe_await(other_cursor.execute("SELECT 2 AS id"))

            with pytest.raises(ydb_dbapi.ProgrammingError):
                maybe_await(connection.commit())

            rows = maybe_await(stream_cursor.fetchall())
            assert rows == [(1,)]

            maybe_await(connection.commit())
        finally:
            maybe_await(stream_cursor.close())
            maybe_await(other_cursor.close())
            if connection._tx_context or connection._session:
                maybe_await(connection.rollback())

    def _test_stream_cursor_fetches_real_data(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        stream_cursor = connection.cursor(stream_results=True)

        try:
            maybe_await(
                stream_cursor.execute(
                    """
                    SELECT 0 AS id, CAST("zero" AS Utf8) AS value
                    UNION ALL
                    SELECT 1 AS id, CAST("one" AS Utf8) AS value
                    UNION ALL
                    SELECT 2 AS id, CAST("two" AS Utf8) AS value;

                    SELECT 10 AS id, CAST("ten" AS Utf8) AS value
                    UNION ALL
                    SELECT 11 AS id, CAST("eleven" AS Utf8) AS value;
                    """
                )
            )

            assert stream_cursor.rowcount == -1

            row = maybe_await(stream_cursor.fetchone())
            assert row == (0, "zero")
            assert stream_cursor.rowcount == -1

            rows = maybe_await(stream_cursor.fetchmany(size=2))
            assert rows == [(1, "one"), (2, "two")]
            assert stream_cursor.rowcount == -1

            rows = maybe_await(stream_cursor.fetchall())
            assert rows == [(10, "ten"), (11, "eleven")]
            assert stream_cursor.rowcount == 5
            assert maybe_await(stream_cursor.fetchall()) == []
        finally:
            maybe_await(stream_cursor.close())

    def _test_stream_cursor_empty_result_set(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        stream_cursor = connection.cursor(stream_results=True)

        try:
            maybe_await(
                stream_cursor.execute(
                    """
                    SELECT id
                    FROM (
                        SELECT CAST(1 AS Int64) AS id
                    )
                    WHERE FALSE;
                    """
                )
            )

            assert stream_cursor.description is not None
            assert stream_cursor.description[0][0] == "id"
            assert stream_cursor.rowcount == -1
            assert maybe_await(stream_cursor.fetchone()) is None
            assert stream_cursor.rowcount == 0
            assert maybe_await(stream_cursor.fetchmany(size=2)) == []
            assert maybe_await(stream_cursor.fetchall()) == []
        finally:
            maybe_await(stream_cursor.close())

    def _test_stream_cursor_close_releases_session_for_next_query(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        stream_cursor = connection.cursor(stream_results=True)
        other_cursor = connection.cursor()

        try:
            maybe_await(
                stream_cursor.execute(
                    """
                    SELECT number AS id
                    FROM AS_TABLE([<|number:1|>, <|number:2|>, <|number:3|>]);
                    """
                )
            )

            assert maybe_await(stream_cursor.fetchone()) == (1,)
            maybe_await(stream_cursor.close())

            maybe_await(other_cursor.execute("SELECT 99 AS id"))
            assert other_cursor.fetchall() == [(99,)]
        finally:
            maybe_await(stream_cursor.close())
            maybe_await(other_cursor.close())

    def _test_stream_cursor_execute_while_running_fails(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        stream_cursor = connection.cursor(stream_results=True)

        try:
            maybe_await(
                stream_cursor.execute(
                    """
                    SELECT number AS id
                    FROM AS_TABLE([<|number:1|>, <|number:2|>]);
                    """
                )
            )

            with pytest.raises(ydb_dbapi.ProgrammingError):
                maybe_await(stream_cursor.execute("SELECT 10 AS id"))

            assert maybe_await(stream_cursor.fetchall()) == [(1,), (2,)]
            maybe_await(stream_cursor.execute("SELECT 10 AS id"))
            assert maybe_await(stream_cursor.fetchall()) == [(10,)]
        finally:
            maybe_await(stream_cursor.close())

    def _test_stream_cursor_close_unblocks_shared_transaction_session(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        connection.set_isolation_level(ydb_dbapi.IsolationLevel.SERIALIZABLE)
        maybe_await(connection.begin())

        stream_cursor = connection.cursor(stream_results=True)
        other_cursor = connection.cursor()

        try:
            maybe_await(
                stream_cursor.execute(
                    """
                    SELECT number AS id
                    FROM AS_TABLE([<|number:1|>, <|number:2|>, <|number:3|>]);
                    """
                )
            )
            assert maybe_await(stream_cursor.fetchone()) == (1,)

            maybe_await(stream_cursor.close())

            maybe_await(other_cursor.execute("SELECT 77 AS id"))
            assert other_cursor.fetchall() == [(77,)]
            maybe_await(connection.commit())
        finally:
            maybe_await(stream_cursor.close())
            maybe_await(other_cursor.close())
            if connection._tx_context or connection._session:
                maybe_await(connection.rollback())

    def _test_stream_cursor_many_result_sets_fetchone_state(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        result_set_count = 8
        stream_cursor = connection.cursor(stream_results=True)

        try:
            maybe_await(
                stream_cursor.execute(
                    self._make_multi_result_query(result_set_count)
                )
            )

            assert stream_cursor._state == CursorStatus.running
            assert stream_cursor.rowcount == -1
            assert stream_cursor.description is not None
            assert stream_cursor.description[0][0] == "id"
            assert stream_cursor.description[1][0] == "value"

            for index in range(result_set_count):
                assert maybe_await(stream_cursor.fetchone()) == (
                    index,
                    str(index),
                )
                assert stream_cursor._state == CursorStatus.running
                if index < result_set_count - 1:
                    assert stream_cursor.rowcount == -1

            assert stream_cursor.rowcount == -1
            assert maybe_await(stream_cursor.fetchone()) is None
            assert stream_cursor.rowcount == result_set_count
            assert stream_cursor._state == CursorStatus.finished
        finally:
            maybe_await(stream_cursor.close())

    def _test_stream_cursor_many_result_sets_fetchmany(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        result_set_count = 9
        stream_cursor = connection.cursor(stream_results=True)

        try:
            maybe_await(
                stream_cursor.execute(
                    self._make_multi_result_query(result_set_count)
                )
            )

            rows = maybe_await(stream_cursor.fetchmany(size=4))
            assert rows == [
                (0, "0"),
                (1, "1"),
                (2, "2"),
                (3, "3"),
            ]
            assert stream_cursor.rowcount == -1
            assert stream_cursor._state == CursorStatus.running

            rows = maybe_await(stream_cursor.fetchmany(size=4))
            assert rows == [
                (4, "4"),
                (5, "5"),
                (6, "6"),
                (7, "7"),
            ]
            assert stream_cursor.rowcount == -1

            rows = maybe_await(stream_cursor.fetchmany(size=4))
            assert rows == [(8, "8")]
            assert stream_cursor.rowcount == result_set_count
            assert stream_cursor._state == CursorStatus.finished

            assert maybe_await(stream_cursor.fetchmany(size=4)) == []
            assert stream_cursor.rowcount == result_set_count
            assert stream_cursor._state == CursorStatus.finished
        finally:
            maybe_await(stream_cursor.close())

    def _test_stream_cursor_many_result_sets_fetchall(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        result_set_count = 6
        stream_cursor = connection.cursor(stream_results=True)

        try:
            maybe_await(
                stream_cursor.execute(
                    self._make_multi_result_query(result_set_count)
                )
            )

            rows = maybe_await(stream_cursor.fetchall())
            assert rows == [
                (index, str(index)) for index in range(result_set_count)
            ]
            assert stream_cursor.rowcount == result_set_count
            assert stream_cursor._state == CursorStatus.finished
            assert maybe_await(stream_cursor.fetchall()) == []
        finally:
            maybe_await(stream_cursor.close())

    def _test_other_cursor_reusable_after_blocked_execute(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        connection.set_isolation_level(ydb_dbapi.IsolationLevel.SERIALIZABLE)
        maybe_await(connection.begin())

        stream_cursor = connection.cursor(stream_results=True)
        other_cursor = connection.cursor()

        try:
            maybe_await(stream_cursor.execute("SELECT 1 AS id"))

            with pytest.raises(ydb_dbapi.ProgrammingError):
                maybe_await(other_cursor.execute("SELECT 2 AS id"))

            assert other_cursor._state == CursorStatus.ready

            maybe_await(stream_cursor.fetchall())
            maybe_await(stream_cursor.close())

            maybe_await(other_cursor.execute("SELECT 3 AS id"))
            assert other_cursor.fetchall() == [(3,)]
        finally:
            maybe_await(stream_cursor.close())
            maybe_await(other_cursor.close())
            if connection._tx_context or connection._session:
                maybe_await(connection.rollback())

    def _test_stream_cursor_reusable_after_blocked_execute(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        connection.set_isolation_level(ydb_dbapi.IsolationLevel.SERIALIZABLE)
        maybe_await(connection.begin())

        first_stream = connection.cursor(stream_results=True)
        second_stream = connection.cursor(stream_results=True)

        try:
            maybe_await(first_stream.execute("SELECT 1 AS id"))

            with pytest.raises(ydb_dbapi.ProgrammingError):
                maybe_await(second_stream.execute("SELECT 2 AS id"))

            assert second_stream._state == CursorStatus.ready

            maybe_await(first_stream.fetchall())
            maybe_await(first_stream.close())

            maybe_await(second_stream.execute("SELECT 3 AS id"))
            assert maybe_await(second_stream.fetchall()) == [(3,)]
        finally:
            maybe_await(first_stream.close())
            maybe_await(second_stream.close())
            if connection._tx_context or connection._session:
                maybe_await(connection.rollback())

    def _test_connection_close_with_running_stream_cursor(
        self,
        connection: ydb_dbapi.Connection | ydb_dbapi.AsyncConnection,
    ) -> None:
        connection.set_isolation_level(ydb_dbapi.IsolationLevel.SERIALIZABLE)
        maybe_await(connection.begin())

        stream_cursor = connection.cursor(stream_results=True)
        maybe_await(stream_cursor.execute("SELECT 1 AS id"))

        assert stream_cursor._state == CursorStatus.running

        maybe_await(connection.close())


class TestCursor(BaseCursorTestSuit):
    @pytest.fixture
    def sync_cursor(
        self, session_pool_sync: ydb.QuerySessionPool
    ) -> Generator[Cursor]:
        cursor = Cursor(
            FakeSyncConnection(),
            session_pool_sync,
            ydb.QuerySerializableReadWrite(),
            request_settings=ydb.BaseRequestSettings(),
            retry_settings=ydb.RetrySettings(),
        )
        yield cursor
        cursor.close()

    def test_cursor_fetch_one(self, sync_cursor: Cursor) -> None:
        self._test_cursor_fetch_one(sync_cursor)

    def test_cursor_fetch_many(self, sync_cursor: Cursor) -> None:
        self._test_cursor_fetch_many(sync_cursor)

    def test_cursor_fetch_all(self, sync_cursor: Cursor) -> None:
        self._test_cursor_fetch_all(sync_cursor)

    def test_cursor_fetch_one_multiple_result_sets(
        self, sync_cursor: Cursor
    ) -> None:
        self._test_cursor_fetch_one_multiple_result_sets(sync_cursor)

    def test_cursor_fetch_many_multiple_result_sets(
        self, sync_cursor: Cursor
    ) -> None:
        self._test_cursor_fetch_many_multiple_result_sets(sync_cursor)

    def test_cursor_fetch_all_multiple_result_sets(
        self, sync_cursor: Cursor
    ) -> None:
        self._test_cursor_fetch_all_multiple_result_sets(sync_cursor)

    def test_cursor_state_after_error(self, sync_cursor: Cursor) -> None:
        self._test_cursor_state_after_error(sync_cursor)


class TestAsyncCursor(BaseCursorTestSuit):
    @pytest.fixture
    async def async_cursor(
        self, session_pool: ydb.aio.QuerySessionPool
    ) -> AsyncGenerator[Cursor]:
        cursor = AsyncCursor(
            FakeAsyncConnection(),
            session_pool,
            ydb.QuerySerializableReadWrite(),
            request_settings=ydb.BaseRequestSettings(),
            retry_settings=ydb.RetrySettings(),
        )
        yield cursor
        cursor.close()

    @pytest.mark.asyncio
    async def test_cursor_fetch_one(self, async_cursor: AsyncCursor) -> None:
        await greenlet_spawn(self._test_cursor_fetch_one, async_cursor)

    @pytest.mark.asyncio
    async def test_cursor_fetch_many(self, async_cursor: AsyncCursor) -> None:
        await greenlet_spawn(self._test_cursor_fetch_many, async_cursor)

    @pytest.mark.asyncio
    async def test_cursor_fetch_all(self, async_cursor: AsyncCursor) -> None:
        await greenlet_spawn(self._test_cursor_fetch_all, async_cursor)

    @pytest.mark.asyncio
    async def test_cursor_fetch_one_multiple_result_sets(
        self, async_cursor: AsyncCursor
    ) -> None:
        await greenlet_spawn(
            self._test_cursor_fetch_one_multiple_result_sets, async_cursor
        )

    @pytest.mark.asyncio
    async def test_cursor_fetch_many_multiple_result_sets(
        self, async_cursor: AsyncCursor
    ) -> None:
        await greenlet_spawn(
            self._test_cursor_fetch_many_multiple_result_sets, async_cursor
        )

    @pytest.mark.asyncio
    async def test_cursor_fetch_all_multiple_result_sets(
        self, async_cursor: AsyncCursor
    ) -> None:
        await greenlet_spawn(
            self._test_cursor_fetch_all_multiple_result_sets, async_cursor
        )

    @pytest.mark.asyncio
    async def test_cursor_state_after_error(
        self, async_cursor: AsyncCursor
    ) -> None:
        await greenlet_spawn(self._test_cursor_state_after_error, async_cursor)


class TestStreamCursorIntegration(BaseStreamCursorIntegrationTestSuit):
    @pytest.fixture
    def connection(
        self, connection_kwargs: dict
    ) -> Generator[ydb_dbapi.Connection]:
        conn = ydb_dbapi.connect(**connection_kwargs)  # ignore: typing
        try:
            yield conn
        finally:
            conn.close()

    def test_stream_cursor_blocks_shared_transaction_session(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_blocks_shared_transaction_session(connection)

    def test_stream_cursor_fetches_real_data(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_fetches_real_data(connection)

    def test_stream_cursor_empty_result_set(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_empty_result_set(connection)

    def test_stream_cursor_close_releases_session_for_next_query(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_close_releases_session_for_next_query(
            connection
        )

    def test_stream_cursor_execute_while_running_fails(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_execute_while_running_fails(connection)

    def test_stream_cursor_close_unblocks_shared_transaction_session(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_close_unblocks_shared_transaction_session(
            connection
        )

    def test_stream_cursor_many_result_sets_fetchone_state(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_many_result_sets_fetchone_state(connection)

    def test_stream_cursor_many_result_sets_fetchmany(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_many_result_sets_fetchmany(connection)

    def test_stream_cursor_many_result_sets_fetchall(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_many_result_sets_fetchall(connection)

    def test_other_cursor_reusable_after_blocked_execute(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_other_cursor_reusable_after_blocked_execute(connection)

    def test_stream_cursor_reusable_after_blocked_execute(
        self, connection: ydb_dbapi.Connection
    ) -> None:
        self._test_stream_cursor_reusable_after_blocked_execute(connection)

    def test_connection_close_with_running_stream_cursor(
        self, connection_kwargs: dict
    ) -> None:
        conn = ydb_dbapi.connect(**connection_kwargs)
        self._test_connection_close_with_running_stream_cursor(conn)


class TestAsyncStreamCursorIntegration(BaseStreamCursorIntegrationTestSuit):
    @pytest_asyncio.fixture
    async def connection(
        self, connection_kwargs: dict
    ) -> AsyncGenerator[ydb_dbapi.AsyncConnection]:
        def connect() -> ydb_dbapi.AsyncConnection:
            return maybe_await(ydb_dbapi.async_connect(**connection_kwargs))

        conn = await greenlet_spawn(connect)
        try:
            yield conn
        finally:

            def close() -> None:
                maybe_await(conn.close())

            await greenlet_spawn(close)

    @pytest.mark.asyncio
    async def test_stream_cursor_blocks_shared_transaction_session(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_blocks_shared_transaction_session,
            connection,
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_fetches_real_data(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_fetches_real_data,
            connection,
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_empty_result_set(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_empty_result_set,
            connection,
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_close_releases_session_for_next_query(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_close_releases_session_for_next_query,
            connection,
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_execute_while_running_fails(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_execute_while_running_fails,
            connection,
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_close_unblocks_shared_transaction_session(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_close_unblocks_shared_transaction_session,
            connection,
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_many_result_sets_fetchone_state(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_many_result_sets_fetchone_state,
            connection,
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_many_result_sets_fetchmany(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_many_result_sets_fetchmany,
            connection,
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_many_result_sets_fetchall(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_many_result_sets_fetchall,
            connection,
        )

    @pytest.mark.asyncio
    async def test_other_cursor_reusable_after_blocked_execute(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_other_cursor_reusable_after_blocked_execute,
            connection,
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_reusable_after_blocked_execute(
        self, connection: ydb_dbapi.AsyncConnection
    ) -> None:
        await greenlet_spawn(
            self._test_stream_cursor_reusable_after_blocked_execute,
            connection,
        )

    @pytest.mark.asyncio
    async def test_connection_close_with_running_stream_cursor(
        self, connection_kwargs: dict
    ) -> None:
        def connect() -> ydb_dbapi.AsyncConnection:
            return maybe_await(ydb_dbapi.async_connect(**connection_kwargs))

        conn = await greenlet_spawn(connect)
        await greenlet_spawn(
            self._test_connection_close_with_running_stream_cursor,
            conn,
        )
