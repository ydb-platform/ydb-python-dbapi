from __future__ import annotations

from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator
from collections.abc import Generator
from typing import Any
from typing import Callable

import pytest
import ydb
from testcontainers.core.generic import DbContainer
from testcontainers.core.generic import wait_container_is_ready
from testcontainers.core.utils import setup_logger
from typing_extensions import Self

logger = setup_logger(__name__)


class YDBContainer(DbContainer):
    def __init__(
        self,
        name: str | None = None,
        port: str = "2135",
        image: str = "ydbplatform/local-ydb:trunk",
        **kwargs: Any,
    ) -> None:
        docker_client_kw: dict[str, Any] = kwargs.pop("docker_client_kw", {})
        docker_client_kw["timeout"] = docker_client_kw.get("timeout") or 300
        super().__init__(
            image=image,
            hostname="localhost",
            docker_client_kw=docker_client_kw,
            **kwargs,
        )
        self.port_to_expose = port
        self._name = name
        self._database_name = "local"

    def start(self) -> Self:
        self._maybe_stop_old_container()
        super().start()
        return self

    def get_connection_url(self, driver: str = "ydb") -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port_to_expose)
        return f"yql+{driver}://{host}:{port}/local"

    def get_connection_string(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port_to_expose)
        return f"grpc://{host}:{port}/?database=/local"

    def get_ydb_database_name(self) -> str:
        return self._database_name

    def get_ydb_host(self) -> str:
        return self.get_container_host_ip()

    def get_ydb_port(self) -> str:
        return self.get_exposed_port(self.port_to_expose)

    @wait_container_is_ready(ydb.ConnectionError)
    def _connect(self) -> None:
        with ydb.Driver(
            connection_string=self.get_connection_string()
        ) as driver:
            driver.wait(fail_fast=True)
            try:
                driver.scheme_client.describe_path("/local/.sys_health/test")
            except ydb.SchemeError as e:
                msg = "Database is not ready"
                raise ydb.ConnectionError(msg) from e

    def _configure(self) -> None:
        self.with_bind_ports(self.port_to_expose, self.port_to_expose)
        if self._name:
            self.with_name(self._name)
        self.with_env("YDB_USE_IN_MEMORY_PDISKS", "true")
        self.with_env("YDB_DEFAULT_LOG_LEVEL", "DEBUG")
        self.with_env("GRPC_PORT", self.port_to_expose)
        self.with_env("GRPC_TLS_PORT", self.port_to_expose)

    def _maybe_stop_old_container(self) -> None:
        if not self._name:
            return
        docker_client = self.get_docker_client()
        running_container = docker_client.client.api.containers(
            filters={"name": self._name}
        )
        if running_container:
            logger.info("Stop existing container")
            docker_client.client.api.remove_container(
                running_container[0], force=True, v=True
            )


@pytest.fixture(scope="session")
def ydb_container(
    unused_tcp_port_factory: Callable[[], int],
) -> Generator[YDBContainer, None, None]:
    with YDBContainer(port=str(unused_tcp_port_factory())) as ydb_container:
        yield ydb_container


@pytest.fixture
def connection_string(ydb_container: YDBContainer) -> str:
    return ydb_container.get_connection_string()


@pytest.fixture
def connection_kwargs(ydb_container: YDBContainer) -> dict:
    return {
        "host": ydb_container.get_ydb_host(),
        "port": ydb_container.get_ydb_port(),
        "database": ydb_container.get_ydb_database_name(),
    }


@pytest.fixture
async def driver(
    ydb_container: YDBContainer, event_loop: AbstractEventLoop
) -> AsyncGenerator[ydb.aio.Driver]:
    driver = ydb.aio.Driver(
        connection_string=ydb_container.get_connection_string()
    )
    await driver.wait(timeout=15, fail_fast=True)

    yield driver

    await driver.stop(timeout=10)
    del driver


@pytest.fixture
def driver_sync(
    ydb_container: YDBContainer,
) -> Generator[ydb.Driver]:
    driver = ydb.Driver(
        connection_string=ydb_container.get_connection_string()
    )
    driver.wait(timeout=15, fail_fast=True)

    yield driver

    driver.stop(timeout=10)
    del driver


@pytest.fixture
async def session_pool(
    driver: ydb.aio.Driver,
) -> AsyncGenerator[ydb.aio.QuerySessionPool]:
    session_pool = ydb.aio.QuerySessionPool(driver)
    async with session_pool:
        await session_pool.execute_with_retries(
            """DROP TABLE IF EXISTS table"""
        )
        await session_pool.execute_with_retries(
            """
            CREATE TABLE table (
            id Int64 NOT NULL,
            val Int64,
            PRIMARY KEY(id)
            )
            """
        )

        yield session_pool


@pytest.fixture
def session_pool_sync(
    driver_sync: ydb.Driver,
) -> Generator[ydb.QuerySessionPool]:
    session_pool = ydb.QuerySessionPool(driver_sync)
    with session_pool:
        session_pool.execute_with_retries("""DROP TABLE IF EXISTS table""")
        session_pool.execute_with_retries(
            """
            CREATE TABLE table (
            id Int64 NOT NULL,
            val Int64,
            PRIMARY KEY(id)
            )
            """
        )
        yield session_pool


@pytest.fixture
async def session(
    session_pool: ydb.aio.QuerySessionPool,
) -> AsyncGenerator[ydb.aio.QuerySession]:
    session = await session_pool.acquire()

    yield session

    await session_pool.release(session)


@pytest.fixture
def session_sync(
    session_pool_sync: ydb.QuerySessionPool,
) -> Generator[ydb.QuerySession]:
    session = session_pool_sync.acquire()

    yield session

    session_pool_sync.release(session)
