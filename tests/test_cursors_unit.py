from __future__ import annotations

from collections.abc import AsyncGenerator
from inspect import iscoroutine

import pytest
import ydb
from sqlalchemy.util import await_only
from typing_extensions import Self
from ydb_dbapi import AsyncCursor
from ydb_dbapi import AsyncStreamCursor
from ydb_dbapi import Cursor
from ydb_dbapi import StreamCursor


def maybe_await(obj: callable) -> any:
    if not iscoroutine(obj):
        return obj
    return await_only(obj)


RESULT_SET_LENGTH = 4
RESULT_SET_COUNT = 3


class FakeResultSet:
    def __init__(self, rows: list[tuple], columns: list[object]) -> None:
        self.rows = rows
        self.columns = columns


class FakeSyncResponseContextIterator:
    def __init__(self, result_sets: list[FakeResultSet]) -> None:
        self._result_sets = iter(result_sets)
        self.cancelled = False

    def __iter__(self) -> Self:
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

    def __aiter__(self) -> Self:
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


class FakeSyncStreamSession:
    def __init__(self, result_sets: list[FakeResultSet]) -> None:
        self._result_sets = result_sets
        self.deleted = False

    def transaction(self, tx_mode: ydb.BaseQueryTxMode) -> Self:
        return self

    def execute(self, **kwargs: object) -> FakeSyncResponseContextIterator:
        return FakeSyncResponseContextIterator(self._result_sets)

    def delete(self) -> None:
        self.deleted = True


class FakeAsyncStreamSession:
    def __init__(self, result_sets: list[FakeResultSet]) -> None:
        self._result_sets = result_sets
        self.deleted = False

    def transaction(self, tx_mode: ydb.BaseQueryTxMode) -> Self:
        return self

    async def execute(
        self, **kwargs: object
    ) -> FakeAsyncResponseContextIterator:
        return FakeAsyncResponseContextIterator(self._result_sets)

    async def delete(self) -> None:
        self.deleted = True


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


def make_result_sets(count: int = 1) -> list[FakeResultSet]:
    return [
        FakeResultSet(
            rows=[(row_id, row_id) for row_id in range(RESULT_SET_LENGTH)],
            columns=[],
        )
        for _ in range(count)
    ]


class FakeSyncConnection:
    def _clear_current_cursor(self, cursor: Cursor | None = None) -> None: ...

    def _invalidate_session(self) -> None: ...

    def _set_current_cursor(self, cursor: StreamCursor) -> None: ...


class FakeAsyncConnection:
    def _clear_current_cursor(
        self, cursor: AsyncCursor | None = None
    ) -> None: ...

    async def _invalidate_session(self) -> None: ...

    def _set_current_cursor(self, cursor: AsyncStreamCursor) -> None: ...


class BaseStreamCursorTestSuit:
    def _test_stream_cursor_fetch_one(self, cursor: StreamCursor) -> None:
        maybe_await(cursor.execute("SELECT id, val FROM table"))

        assert cursor.rowcount == -1

        for i in range(4):
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == i

        assert cursor.fetchone() is None
        assert cursor.rowcount == 4

    def _test_stream_cursor_fetch_many(self, cursor: StreamCursor) -> None:
        maybe_await(
            cursor.execute(
                """
                SELECT id, val FROM table;
                SELECT id, val FROM table1;
                SELECT id, val FROM table2;
                """
            )
        )

        assert cursor.rowcount == -1

        rows = cursor.fetchmany(size=5)
        assert len(rows) == 5
        assert cursor.rowcount == -1

        rows = cursor.fetchmany(size=7)
        assert len(rows) == 7
        assert cursor.fetchmany(size=1) == []
        assert cursor.rowcount == 12

    def _test_stream_cursor_fetch_all(self, cursor: StreamCursor) -> None:
        maybe_await(cursor.execute("SELECT id, val FROM table"))

        rows = cursor.fetchall()
        assert len(rows) == 4
        assert cursor.rowcount == 4
        assert cursor.fetchall() == []


class TestStreamCursor(BaseStreamCursorTestSuit):
    @pytest.fixture
    def sync_cursor(self) -> StreamCursor:
        cursor = StreamCursor(
            FakeSyncConnection(),
            FakeSyncStreamSessionPool(make_result_sets()),
            ydb.QuerySerializableReadWrite(),
            request_settings=ydb.BaseRequestSettings(),
            retry_settings=ydb.RetrySettings(),
        )
        try:
            yield cursor
        finally:
            cursor.close()

    def test_cursor_fetch_one(self, sync_cursor: StreamCursor) -> None:
        self._test_stream_cursor_fetch_one(sync_cursor)

    def test_cursor_fetch_many(self, sync_cursor: StreamCursor) -> None:
        sync_cursor._session_pool = FakeSyncStreamSessionPool(
            make_result_sets(RESULT_SET_COUNT)
        )
        self._test_stream_cursor_fetch_many(sync_cursor)

    def test_cursor_fetch_all(self, sync_cursor: StreamCursor) -> None:
        self._test_stream_cursor_fetch_all(sync_cursor)


class TestAsyncStreamCursor:
    @pytest.fixture
    async def async_cursor(self) -> AsyncGenerator[AsyncStreamCursor]:
        cursor = AsyncStreamCursor(
            FakeAsyncConnection(),
            FakeAsyncStreamSessionPool(make_result_sets()),
            ydb.QuerySerializableReadWrite(),
            request_settings=ydb.BaseRequestSettings(),
            retry_settings=ydb.RetrySettings(),
        )
        yield cursor
        await cursor.close()

    @pytest.mark.asyncio
    async def test_cursor_fetch_one(
        self, async_cursor: AsyncStreamCursor
    ) -> None:
        await async_cursor.execute("SELECT id, val FROM table")

        assert async_cursor.rowcount == -1

        for i in range(4):
            row = await async_cursor.fetchone()
            assert row is not None
            assert row[0] == i

        assert await async_cursor.fetchone() is None
        assert async_cursor.rowcount == 4

    @pytest.mark.asyncio
    async def test_cursor_fetch_many(
        self, async_cursor: AsyncStreamCursor
    ) -> None:
        async_cursor._session_pool = FakeAsyncStreamSessionPool(
            make_result_sets(RESULT_SET_COUNT)
        )
        await async_cursor.execute(
            """
            SELECT id, val FROM table;
            SELECT id, val FROM table1;
            SELECT id, val FROM table2;
            """
        )

        assert async_cursor.rowcount == -1

        rows = await async_cursor.fetchmany(size=5)
        assert len(rows) == 5
        assert async_cursor.rowcount == -1

        rows = await async_cursor.fetchmany(size=7)
        assert len(rows) == 7
        assert await async_cursor.fetchmany(size=1) == []
        assert async_cursor.rowcount == 12

    @pytest.mark.asyncio
    async def test_cursor_fetch_all(
        self, async_cursor: AsyncStreamCursor
    ) -> None:
        await async_cursor.execute("SELECT id, val FROM table")

        rows = await async_cursor.fetchall()
        assert len(rows) == 4
        assert async_cursor.rowcount == 4
        assert await async_cursor.fetchall() == []
