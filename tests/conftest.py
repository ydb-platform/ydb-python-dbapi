from __future__ import annotations

from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator
from collections.abc import Generator

import pytest
import ydb


@pytest.fixture
def connection_string() -> str:
    return "grpc://localhost:2136/?database=/local"


@pytest.fixture
def connection_kwargs() -> dict:
    return {
        "host": "localhost",
        "port": "2136",
        "database": "/local",
    }


@pytest.fixture
async def driver(
    connection_string: str,
    event_loop: AbstractEventLoop,
) -> AsyncGenerator[ydb.aio.Driver]:
    driver = ydb.aio.Driver(connection_string=connection_string)
    await driver.wait(timeout=10)

    yield driver

    await driver.stop(timeout=10)
    del driver


@pytest.fixture
def driver_sync(
    connection_string: str,
) -> Generator[ydb.Driver]:
    driver = ydb.Driver(connection_string=connection_string)
    driver.wait(timeout=10)

    yield driver

    driver.stop(timeout=10)
    del driver


@pytest.fixture
async def session_pool(
    driver: ydb.aio.Driver,
) -> AsyncGenerator[ydb.aio.QuerySessionPool]:
    session_pool = ydb.aio.QuerySessionPool(driver)
    async with session_pool:
        for name in ["table", "table1", "table2"]:
            await session_pool.execute_with_retries(
                f"""
                DROP TABLE IF EXISTS {name};
                CREATE TABLE {name} (
                id Int64 NOT NULL,
                val Int64,
                PRIMARY KEY(id)
                )
                """
            )

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

        yield session_pool

        for name in ["table", "table1", "table2"]:
            await session_pool.execute_with_retries(
                f"""
                DROP TABLE {name};
                """
            )


@pytest.fixture
def session_pool_sync(
    driver_sync: ydb.Driver,
) -> Generator[ydb.QuerySessionPool]:
    session_pool = ydb.QuerySessionPool(driver_sync)
    with session_pool:
        for name in ["table", "table1", "table2"]:
            session_pool.execute_with_retries(
                f"""
                DROP TABLE IF EXISTS {name};
                CREATE TABLE {name} (
                id Int64 NOT NULL,
                val Int64,
                PRIMARY KEY(id)
                )
                """
            )

            session_pool.execute_with_retries(
                f"""
                DELETE FROM {name};
                INSERT INTO {name} (id, val) VALUES
                (0, 0),
                (1, 1),
                (2, 2),
                (3, 3)
                """
            )

        yield session_pool

        for name in ["table", "table1", "table2"]:
            session_pool.execute_with_retries(
                f"""
                DROP TABLE {name};
                """
            )
