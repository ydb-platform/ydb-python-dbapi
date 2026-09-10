from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Generator
from inspect import iscoroutine

import pytest
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
