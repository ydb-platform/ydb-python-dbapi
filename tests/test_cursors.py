from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Generator
from inspect import iscoroutine

import pytest
import ydb
import ydb_dbapi
from sqlalchemy.util import await_only
from sqlalchemy.util import greenlet_spawn
from typing_extensions import Self
from ydb_dbapi import AsyncCursor
from ydb_dbapi import AsyncStreamCursor
from ydb_dbapi import Cursor
from ydb_dbapi import StreamCursor
from ydb_dbapi.utils import CursorStatus


def maybe_await(obj: callable) -> any:
    if not iscoroutine(obj):
        return obj
    return await_only(obj)


RESULT_SET_LENGTH = 4
RESULT_SET_COUNT = 3


class FakeSyncConnection:
    _tx_context = None

    def _invalidate_session(self) -> None: ...


class FakeAsyncConnection:
    _tx_context = None

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

    def _test_execute_replaces_unread_result(
        self, cursor: Cursor | AsyncCursor
    ) -> None:
        maybe_await(cursor.execute("SELECT id FROM table ORDER BY id"))
        assert cursor.rowcount == RESULT_SET_LENGTH

        maybe_await(cursor.execute("SELECT 99 AS id"))

        assert cursor.rowcount == 1
        assert cursor.fetchall() == [(99,)]

    def _test_execute_resets_consumed_result_count(
        self, cursor: Cursor | AsyncCursor
    ) -> None:
        maybe_await(cursor.execute("SELECT id FROM table ORDER BY id"))
        assert len(cursor.fetchall()) == RESULT_SET_LENGTH

        maybe_await(cursor.execute("SELECT 99 AS id"))

        assert cursor.rowcount == 1
        assert cursor.fetchall() == [(99,)]

    def _test_execute_resets_description_for_statement_without_result(
        self, cursor: Cursor | AsyncCursor
    ) -> None:
        maybe_await(cursor.execute("SELECT id FROM table"))
        assert cursor.description is not None

        maybe_await(
            cursor.execute("UPSERT INTO table (id, val) VALUES (100, 100)")
        )

        assert cursor.description is None
        assert cursor.rowcount == -1

    def _test_cursor_state_after_error(
        self, cursor: Cursor | AsyncCursor
    ) -> None:
        query = "INSERT INTO table (id, val) VALUES (0,0)"
        with pytest.raises(ydb_dbapi.Error):
            maybe_await(cursor.execute(query=query))

        assert cursor._state == CursorStatus.finished


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

    def test_execute_replaces_unread_result(self, sync_cursor: Cursor) -> None:
        self._test_execute_replaces_unread_result(sync_cursor)

    def test_execute_resets_consumed_result_count(
        self, sync_cursor: Cursor
    ) -> None:
        self._test_execute_resets_consumed_result_count(sync_cursor)

    def test_execute_resets_description_for_statement_without_result(
        self, sync_cursor: Cursor
    ) -> None:
        self._test_execute_resets_description_for_statement_without_result(
            sync_cursor
        )

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
    async def test_execute_replaces_unread_result(
        self, async_cursor: AsyncCursor
    ) -> None:
        await greenlet_spawn(
            self._test_execute_replaces_unread_result, async_cursor
        )

    @pytest.mark.asyncio
    async def test_execute_resets_consumed_result_count(
        self, async_cursor: AsyncCursor
    ) -> None:
        await greenlet_spawn(
            self._test_execute_resets_consumed_result_count, async_cursor
        )

    @pytest.mark.asyncio
    async def test_execute_resets_description_for_statement_without_result(
        self, async_cursor: AsyncCursor
    ) -> None:
        await greenlet_spawn(
            self._test_execute_resets_description_for_statement_without_result,
            async_cursor,
        )

    @pytest.mark.asyncio
    async def test_cursor_state_after_error(
        self, async_cursor: AsyncCursor
    ) -> None:
        await greenlet_spawn(self._test_cursor_state_after_error, async_cursor)


class FakeResultSet:
    def __init__(self, rows: list[tuple], columns: list[object]) -> None:
        self.rows = rows
        self.columns = columns


def make_stream_result_sets(count: int = 1) -> list[FakeResultSet]:
    return [
        FakeResultSet(
            rows=[(row_id, row_id) for row_id in range(RESULT_SET_LENGTH)],
            columns=[],
        )
        for _ in range(count)
    ]


class FakeSyncResponseContextIterator:
    def __init__(self, result_sets: list[FakeResultSet]) -> None:
        self._result_sets = iter(result_sets)
        self.cancelled = False

    def __iter__(self) -> FakeSyncResponseContextIterator:
        return self

    def __next__(self) -> FakeResultSet:
        return next(self._result_sets)

    def cancel(self) -> None:
        self.cancelled = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        for _ in self:
            pass


class FakeAsyncResponseContextIterator:
    def __init__(self, result_sets: list[FakeResultSet]) -> None:
        self._result_sets = iter(result_sets)
        self.cancelled = False

    def __aiter__(self) -> FakeAsyncResponseContextIterator:
        return self

    async def __anext__(self) -> FakeResultSet:
        try:
            return next(self._result_sets)
        except StopIteration as e:
            raise StopAsyncIteration from e

    def cancel(self) -> None:
        self.cancelled = True

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: object, exc: object, tb: object
    ) -> None:
        async for _ in self:
            pass


class FakeFailingSyncResponseContextIterator(FakeSyncResponseContextIterator):
    def __next__(self) -> FakeResultSet:
        raise ydb.issues.GenericError("boom")


class FakeFailingAsyncResponseContextIterator(
    FakeAsyncResponseContextIterator
):
    async def __anext__(self) -> FakeResultSet:
        raise ydb.issues.GenericError("boom")


class FakeSyncStreamSession:
    def __init__(self, result_sets: list[FakeResultSet]) -> None:
        self._result_sets = result_sets
        self.deleted = False

    def transaction(
        self, tx_mode: ydb.BaseQueryTxMode
    ) -> FakeSyncStreamSession:
        return self

    def execute(self, **kwargs: object) -> FakeSyncResponseContextIterator:
        return FakeSyncResponseContextIterator(self._result_sets)

    def delete(self) -> None:
        self.deleted = True


class FakeAsyncStreamSession:
    def __init__(self, result_sets: list[FakeResultSet]) -> None:
        self._result_sets = result_sets
        self.deleted = False

    def transaction(
        self, tx_mode: ydb.BaseQueryTxMode
    ) -> FakeAsyncStreamSession:
        return self

    async def execute(
        self, **kwargs: object
    ) -> FakeAsyncResponseContextIterator:
        return FakeAsyncResponseContextIterator(self._result_sets)

    async def delete(self) -> None:
        self.deleted = True


class FakeFailingSyncStreamSession(FakeSyncStreamSession):
    def execute(
        self, **kwargs: object
    ) -> FakeFailingSyncResponseContextIterator:
        return FakeFailingSyncResponseContextIterator(self._result_sets)


class FakeFailingAsyncStreamSession(FakeAsyncStreamSession):
    async def execute(
        self, **kwargs: object
    ) -> FakeFailingAsyncResponseContextIterator:
        return FakeFailingAsyncResponseContextIterator(self._result_sets)


class FakeSyncStreamSessionPool:
    def __init__(self, result_sets: list[FakeResultSet]) -> None:
        self.session = FakeSyncStreamSession(result_sets)
        self.released_sessions: list[FakeSyncStreamSession] = []

    def acquire(self, timeout: float | None = None) -> FakeSyncStreamSession:
        return self.session

    def release(self, session: FakeSyncStreamSession) -> None:
        self.released_sessions.append(session)


class FakeAsyncStreamSessionPool:
    def __init__(self, result_sets: list[FakeResultSet]) -> None:
        self.session = FakeAsyncStreamSession(result_sets)
        self.released_sessions: list[FakeAsyncStreamSession] = []

    async def acquire(
        self, timeout: float | None = None
    ) -> FakeAsyncStreamSession:
        return self.session

    async def release(self, session: FakeAsyncStreamSession) -> None:
        self.released_sessions.append(session)


class FakeSyncStreamConnection:
    _tx_context = None

    def __init__(self) -> None:
        self.current_cursor: StreamCursor | None = None

    def _invalidate_session(self) -> None: ...

    def _set_current_cursor(self, cursor: StreamCursor) -> None:
        self.current_cursor = cursor

    def _clear_current_cursor(self, cursor: Cursor | None = None) -> None:
        if cursor is None or self.current_cursor is cursor:
            self.current_cursor = None


class FakeAsyncStreamConnection:
    _tx_context = None

    def __init__(self) -> None:
        self.current_cursor: AsyncStreamCursor | None = None

    async def _invalidate_session(self) -> None: ...

    def _set_current_cursor(self, cursor: AsyncStreamCursor) -> None:
        self.current_cursor = cursor

    def _clear_current_cursor(self, cursor: AsyncCursor | None = None) -> None:
        if cursor is None or self.current_cursor is cursor:
            self.current_cursor = None


class TestStreamCursor:
    def _make_cursor(self, result_sets: list) -> StreamCursor:
        return StreamCursor(
            FakeSyncStreamConnection(),
            FakeSyncStreamSessionPool(result_sets),
            ydb.QuerySerializableReadWrite(),
            request_settings=ydb.BaseRequestSettings(),
            retry_settings=ydb.RetrySettings(),
        )

    def test_stream_cursor_fetch_one(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        try:
            cursor.execute("SELECT id, val FROM table")

            assert cursor.rowcount == -1

            for i in range(RESULT_SET_LENGTH):
                row = cursor.fetchone()
                assert row is not None
                assert row[0] == i

            assert cursor.fetchone() is None
            assert cursor.rowcount == RESULT_SET_LENGTH
        finally:
            cursor.close()

    def test_stream_cursor_fetch_many(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets(RESULT_SET_COUNT))
        try:
            cursor.execute(
                """
                SELECT id, val FROM table;
                SELECT id, val FROM table1;
                SELECT id, val FROM table2;
                """
            )

            assert cursor.rowcount == -1

            rows = cursor.fetchmany(size=5)
            assert len(rows) == 5
            assert cursor.rowcount == -1

            rows = cursor.fetchmany(size=7)
            assert len(rows) == 7
            assert cursor.fetchmany(size=1) == []
            assert cursor.rowcount == RESULT_SET_LENGTH * RESULT_SET_COUNT
        finally:
            cursor.close()

    def test_stream_cursor_fetch_all(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        try:
            cursor.execute("SELECT id, val FROM table")

            rows = cursor.fetchall()
            assert len(rows) == RESULT_SET_LENGTH
            assert cursor.rowcount == RESULT_SET_LENGTH
            assert cursor.fetchall() == []
        finally:
            cursor.close()

    def test_stream_cursor_releases_session_after_drain(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        pool = cursor._session_pool
        try:
            cursor.execute("SELECT id, val FROM table")
            cursor.fetchall()

            assert pool.released_sessions == [pool.session]
            assert pool.session.deleted is False
        finally:
            cursor.close()

    def test_stream_cursor_discards_session_on_close(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        pool = cursor._session_pool
        cursor.execute("SELECT id, val FROM table")
        cursor.close()

        assert pool.session.deleted is True
        assert pool.released_sessions == [pool.session]

    def test_stream_cursor_cancels_stream_on_close(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        cursor.execute("SELECT id, val FROM table")
        stream = cursor._stream
        cursor.close()

        assert stream is not None
        assert stream.cancelled is True

    def test_stream_cursor_invalidate_owner_session_on_error(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        pool = cursor._session_pool
        pool.session = FakeFailingSyncStreamSession(make_stream_result_sets())

        with pytest.raises(ydb_dbapi.Error):
            cursor.execute("SELECT id, val FROM table")

        assert pool.session.deleted is True
        assert pool.released_sessions == [pool.session]
        assert cursor._state == CursorStatus.finished


class TestAsyncStreamCursor:
    def _make_cursor(self, result_sets: list) -> AsyncStreamCursor:
        return AsyncStreamCursor(
            FakeAsyncStreamConnection(),
            FakeAsyncStreamSessionPool(result_sets),
            ydb.QuerySerializableReadWrite(),
            request_settings=ydb.BaseRequestSettings(),
            retry_settings=ydb.RetrySettings(),
        )

    @pytest.mark.asyncio
    async def test_stream_cursor_fetch_one(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        try:
            await cursor.execute("SELECT id, val FROM table")

            assert cursor.rowcount == -1

            for i in range(RESULT_SET_LENGTH):
                row = await cursor.fetchone()
                assert row is not None
                assert row[0] == i

            assert await cursor.fetchone() is None
            assert cursor.rowcount == RESULT_SET_LENGTH
        finally:
            await cursor.close()

    @pytest.mark.asyncio
    async def test_stream_cursor_fetch_many(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets(RESULT_SET_COUNT))
        try:
            await cursor.execute(
                """
                SELECT id, val FROM table;
                SELECT id, val FROM table1;
                SELECT id, val FROM table2;
                """
            )

            assert cursor.rowcount == -1

            rows = await cursor.fetchmany(size=5)
            assert len(rows) == 5
            assert cursor.rowcount == -1

            rows = await cursor.fetchmany(size=7)
            assert len(rows) == 7
            assert await cursor.fetchmany(size=1) == []
            assert cursor.rowcount == RESULT_SET_LENGTH * RESULT_SET_COUNT
        finally:
            await cursor.close()

    @pytest.mark.asyncio
    async def test_stream_cursor_fetch_all(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        try:
            await cursor.execute("SELECT id, val FROM table")

            rows = await cursor.fetchall()
            assert len(rows) == RESULT_SET_LENGTH
            assert cursor.rowcount == RESULT_SET_LENGTH
            assert await cursor.fetchall() == []
        finally:
            await cursor.close()

    @pytest.mark.asyncio
    async def test_stream_cursor_discards_session_on_close(self) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        pool = cursor._session_pool
        await cursor.execute("SELECT id, val FROM table")
        await cursor.close()

        assert pool.session.deleted is True
        assert pool.released_sessions == [pool.session]

    @pytest.mark.asyncio
    async def test_stream_cursor_invalidate_owner_session_on_error(
        self,
    ) -> None:
        cursor = self._make_cursor(make_stream_result_sets())
        pool = cursor._session_pool
        pool.session = FakeFailingAsyncStreamSession(make_stream_result_sets())

        with pytest.raises(ydb_dbapi.Error):
            await cursor.execute("SELECT id, val FROM table")

        assert pool.session.deleted is True
        assert pool.released_sessions == [pool.session]
        assert cursor._state == CursorStatus.finished
