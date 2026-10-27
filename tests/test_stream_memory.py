from __future__ import annotations

import tracemalloc
from collections.abc import AsyncGenerator
from collections.abc import Generator
from inspect import iscoroutine
from typing import Union

import pytest
import pytest_asyncio
import ydb_dbapi as dbapi
from sqlalchemy.util import await_only
from sqlalchemy.util import greenlet_spawn

ROW_COUNT = 100_000
BATCH_SIZE = 1_000
PAYLOAD_SIZE = 256
TABLE_NAME = "stream_memory_table"

ConnectionType = Union[dbapi.Connection, dbapi.AsyncConnection]


def maybe_await(obj: callable) -> any:
    if not iscoroutine(obj):
        return obj
    return await_only(obj)


def _create_table(connection: ConnectionType) -> None:
    cursor = connection.cursor()
    try:
        maybe_await(
            cursor.execute_scheme(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        )
        maybe_await(
            cursor.execute_scheme(
                f"""
                CREATE TABLE {TABLE_NAME} (
                id Int64 NOT NULL,
                payload Utf8,
                PRIMARY KEY(id)
                )
                """
            )
        )
        payload = "x" * PAYLOAD_SIZE
        for start in range(0, ROW_COUNT, BATCH_SIZE):
            stop = min(start + BATCH_SIZE, ROW_COUNT)
            values = ",".join(
                f"({row_id}, '{payload}')" for row_id in range(start, stop)
            )
            maybe_await(
                cursor.execute(
                    f"UPSERT INTO {TABLE_NAME} (id, payload) "
                    f"VALUES {values}"
                )
            )
    finally:
        maybe_await(cursor.close())


def _drop_table(connection: ConnectionType) -> None:
    maybe_await(
        connection.cursor().execute_scheme(
            f"DROP TABLE IF EXISTS {TABLE_NAME}"
        )
    )


def _measure_buffered(connection: ConnectionType) -> tuple[int, int]:
    tracemalloc.start()
    try:
        cursor = connection.cursor()
        maybe_await(cursor.execute(f"SELECT id, payload FROM {TABLE_NAME}"))
        rows = maybe_await(cursor.fetchall())
        row_count = len(rows)
        del rows
        maybe_await(cursor.close())
        _, peak = tracemalloc.get_traced_memory()
        return row_count, peak
    finally:
        tracemalloc.stop()


def _measure_stream(connection: ConnectionType) -> tuple[int, int]:
    tracemalloc.start()
    try:
        cursor = connection.cursor(stream_results=True)
        maybe_await(cursor.execute(f"SELECT id, payload FROM {TABLE_NAME}"))
        row_count = 0
        while maybe_await(cursor.fetchone()) is not None:
            row_count += 1
        assert cursor.rowcount == ROW_COUNT
        maybe_await(cursor.close())
        _, peak = tracemalloc.get_traced_memory()
        return row_count, peak
    finally:
        tracemalloc.stop()


def _run_stream_memory_test(connection: ConnectionType) -> None:
    _create_table(connection)
    try:
        buffered_rows, buffered_peak = _measure_buffered(connection)
        stream_rows, stream_peak = _measure_stream(connection)

        print(  # noqa: T201
            f"\nBUFFERED peak: {buffered_peak / 1024 / 1024:.1f} MiB "
            f"({buffered_rows} rows) | STREAM peak: "
            f"{stream_peak / 1024 / 1024:.1f} MiB ({stream_rows} rows)"
        )

        assert buffered_rows == ROW_COUNT
        assert stream_rows == ROW_COUNT
        assert buffered_peak > 10 * 1024 * 1024
        assert stream_peak * 3 < buffered_peak
    finally:
        _drop_table(connection)


@pytest.fixture
def connection(
    connection_kwargs: dict,
) -> Generator[dbapi.Connection]:
    conn = dbapi.connect(**connection_kwargs)
    try:
        yield conn
    finally:
        conn.close()


@pytest_asyncio.fixture
async def async_connection(
    connection_kwargs: dict,
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


def test_stream_cursor_keeps_memory_bounded(
    connection: dbapi.Connection,
) -> None:
    _run_stream_memory_test(connection)


@pytest.mark.asyncio
async def test_async_stream_cursor_keeps_memory_bounded(
    async_connection: dbapi.AsyncConnection,
) -> None:
    await greenlet_spawn(_run_stream_memory_test, async_connection)
