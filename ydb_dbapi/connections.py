from __future__ import annotations

import posixpath
from typing import Any
from typing import NamedTuple

import ydb
from ydb.retries import retry_operation_async
from ydb.retries import retry_operation_sync

from .cursors import AsyncCursor
from .cursors import Cursor
from .errors import InterfaceError
from .errors import InternalError
from .errors import NotSupportedError
from .utils import handle_ydb_errors


class IsolationLevel:
    SERIALIZABLE = "SERIALIZABLE"
    ONLINE_READONLY = "ONLINE READONLY"
    ONLINE_READONLY_INCONSISTENT = "ONLINE READONLY INCONSISTENT"
    STALE_READONLY = "STALE READONLY"
    SNAPSHOT_READONLY = "SNAPSHOT READONLY"
    AUTOCOMMIT = "AUTOCOMMIT"


class Connection:
    def __init__(
        self,
        host: str = "",
        port: str = "",
        database: str = "",
        **conn_kwargs: Any,
    ) -> None:
        self.endpoint = f"grpc://{host}:{port}"
        self.database = database
        self.conn_kwargs = conn_kwargs
        self.credentials = self.conn_kwargs.pop("credentials", None)
        self.table_path_prefix = self.conn_kwargs.pop(
            "ydb_table_path_prefix", ""
        )

        if (
            "ydb_session_pool" in self.conn_kwargs
        ):  # Use session pool managed manually
            self._shared_session_pool = True
            self._session_pool = self.conn_kwargs.pop("ydb_session_pool")
            self._driver: ydb.Driver = self._session_pool._driver
        else:
            self._shared_session_pool = False
            driver_config = ydb.DriverConfig(
                endpoint=self.endpoint,
                database=self.database,
                credentials=self.credentials,
            )
            self._driver = ydb.Driver(driver_config)
            self._session_pool = ydb.QuerySessionPool(self._driver, size=5)

        self._tx_mode: ydb.BaseQueryTxMode = ydb.QuerySerializableReadWrite()

        self._current_cursor: Cursor | None = None
        self.interactive_transaction: bool = False

        self._session: ydb.QuerySession | None = None
        self._tx_context: ydb.QueryTxContext | None = None

    def wait_ready(self, timeout: int = 10) -> None:
        try:
            self._driver.wait(timeout, fail_fast=True)
        except ydb.Error as e:
            raise InterfaceError(e.message, original_error=e) from e
        except Exception as e:
            self._driver.stop()
            msg = (
                "Failed to connect to YDB, details "
                f"{self._driver.discovery_debug_details()}"
            )
            raise InterfaceError(msg) from e

        self._session = self._session_pool.acquire()

    def set_isolation_level(self, isolation_level: str) -> None:
        class IsolationSettings(NamedTuple):
            ydb_mode: ydb.BaseQueryTxMode
            interactive: bool

        ydb_isolation_settings_map = {
            IsolationLevel.AUTOCOMMIT: IsolationSettings(
                ydb.QuerySerializableReadWrite(), interactive=False
            ),
            IsolationLevel.SERIALIZABLE: IsolationSettings(
                ydb.QuerySerializableReadWrite(), interactive=True
            ),
            IsolationLevel.ONLINE_READONLY: IsolationSettings(
                ydb.QueryOnlineReadOnly(), interactive=True
            ),
            IsolationLevel.ONLINE_READONLY_INCONSISTENT: IsolationSettings(
                ydb.QueryOnlineReadOnly().with_allow_inconsistent_reads(),
                interactive=True,
            ),
            IsolationLevel.STALE_READONLY: IsolationSettings(
                ydb.QueryStaleReadOnly(), interactive=True
            ),
            IsolationLevel.SNAPSHOT_READONLY: IsolationSettings(
                ydb.QuerySnapshotReadOnly(), interactive=True
            ),
        }
        ydb_isolation_settings = ydb_isolation_settings_map[isolation_level]
        if self._tx_context and self._tx_context.tx_id:
            raise InternalError(
                "Failed to set transaction mode: transaction is already began"
            )
        self._tx_mode = ydb_isolation_settings.ydb_mode
        self.interactive_transaction = ydb_isolation_settings.interactive

    def get_isolation_level(self) -> str:
        if self._tx_mode.name == ydb.QuerySerializableReadWrite().name:
            if self.interactive_transaction:
                return IsolationLevel.SERIALIZABLE
            return IsolationLevel.AUTOCOMMIT
        if self._tx_mode.name == ydb.QueryOnlineReadOnly().name:
            if self._tx_mode.settings.allow_inconsistent_reads:
                return IsolationLevel.ONLINE_READONLY_INCONSISTENT
            return IsolationLevel.ONLINE_READONLY
        if self._tx_mode.name == ydb.QueryStaleReadOnly().name:
            return IsolationLevel.STALE_READONLY
        if self._tx_mode.name == ydb.QuerySnapshotReadOnly().name:
            return IsolationLevel.SNAPSHOT_READONLY
        msg = f"{self._tx_mode.name} is not supported"
        raise NotSupportedError(msg)

    def cursor(self) -> Cursor:
        if self._session is None:
            raise RuntimeError("Connection is not ready, use wait_ready.")
        if self._current_cursor and not self._current_cursor.is_closed:
            raise RuntimeError(
                "Unable to create new Cursor before closing existing one."
            )

        if self.interactive_transaction:
            self._tx_context = self._session.transaction(self._tx_mode)
        else:
            self._tx_context = None

        self._current_cursor = Cursor(
            session=self._session,
            tx_context=self._tx_context,
            autocommit=(not self.interactive_transaction),
        )
        return self._current_cursor

    def commit(self) -> None:
        if self._tx_context and self._tx_context.tx_id:
            self._tx_context.commit()
            self._tx_context = None

    def rollback(self) -> None:
        if self._tx_context and self._tx_context.tx_id:
            self._tx_context.rollback()
            self._tx_context = None

    def close(self) -> None:
        self.rollback()

        if self._current_cursor:
            self._current_cursor.close()

        if self._session:
            self._session_pool.release(self._session)

        if not self._shared_session_pool:
            self._session_pool.stop()
            self._driver.stop()

    @handle_ydb_errors
    def describe(self, table_path: str) -> ydb.TableSchemeEntry:
        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_path
        )
        return self._driver.table_client.describe_table(abs_table_path)

    @handle_ydb_errors
    def check_exists(self, table_path: str) -> bool:
        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_path
        )
        return self._check_path_exists(abs_table_path)

    @handle_ydb_errors
    def get_table_names(self) -> list[str]:
        abs_dir_path = posixpath.join(self.database, self.table_path_prefix)
        names = self._get_table_names(abs_dir_path)
        return [posixpath.relpath(path, abs_dir_path) for path in names]

    def _check_path_exists(self, table_path: str) -> bool:
        try:

            def callee() -> None:
                self._driver.scheme_client.describe_path(table_path)

            retry_operation_sync(callee)
        except ydb.SchemeError:
            return False
        else:
            return True

    def _get_table_names(self, abs_dir_path: str) -> list[str]:
        def callee() -> ydb.Directory:
            return self._driver.scheme_client.list_directory(abs_dir_path)

        directory = retry_operation_sync(callee)
        result = []
        for child in directory.children:
            child_abs_path = posixpath.join(abs_dir_path, child.name)
            if child.is_table():
                result.append(child_abs_path)
            elif child.is_directory() and not child.name.startswith("."):
                result.extend(self.get_table_names(child_abs_path))
        return result


class AsyncConnection:
    def __init__(
        self,
        host: str = "",
        port: str = "",
        database: str = "",
        **conn_kwargs: Any,
    ) -> None:
        self.endpoint = f"grpc://{host}:{port}"
        self.database = database
        self.conn_kwargs = conn_kwargs
        self.credentials = self.conn_kwargs.pop("credentials", None)
        self.table_path_prefix = self.conn_kwargs.pop(
            "ydb_table_path_prefix", ""
        )

        if (
            "ydb_session_pool" in self.conn_kwargs
        ):  # Use session pool managed manually
            self._shared_session_pool = True
            self._session_pool = self.conn_kwargs.pop("ydb_session_pool")
            self._driver: ydb.aio.Driver = self._session_pool._driver
        else:
            self._shared_session_pool = False
            driver_config = ydb.DriverConfig(
                endpoint=self.endpoint,
                database=self.database,
                credentials=self.credentials,
            )
            self._driver = ydb.aio.Driver(driver_config)
            self._session_pool = ydb.aio.QuerySessionPool(self._driver, size=5)

        self._tx_mode: ydb.BaseQueryTxMode = ydb.QuerySerializableReadWrite()

        self._current_cursor: AsyncCursor | None = None
        self.interactive_transaction: bool = False

        self._session: ydb.aio.QuerySession | None = None
        self._tx_context: ydb.aio.QueryTxContext | None = None

    async def wait_ready(self, timeout: int = 10) -> None:
        try:
            await self._driver.wait(timeout, fail_fast=True)
        except ydb.Error as e:
            raise InterfaceError(e.message, original_error=e) from e
        except Exception as e:
            await self._driver.stop()
            msg = (
                "Failed to connect to YDB, details "
                f"{self._driver.discovery_debug_details()}"
            )
            raise InterfaceError(msg) from e

        self._session = await self._session_pool.acquire()

    def set_isolation_level(self, isolation_level: str) -> None:
        class IsolationSettings(NamedTuple):
            ydb_mode: ydb.BaseQueryTxMode
            interactive: bool

        ydb_isolation_settings_map = {
            IsolationLevel.AUTOCOMMIT: IsolationSettings(
                ydb.QuerySerializableReadWrite(), interactive=False
            ),
            IsolationLevel.SERIALIZABLE: IsolationSettings(
                ydb.QuerySerializableReadWrite(), interactive=True
            ),
            IsolationLevel.ONLINE_READONLY: IsolationSettings(
                ydb.QueryOnlineReadOnly(), interactive=True
            ),
            IsolationLevel.ONLINE_READONLY_INCONSISTENT: IsolationSettings(
                ydb.QueryOnlineReadOnly().with_allow_inconsistent_reads(),
                interactive=True,
            ),
            IsolationLevel.STALE_READONLY: IsolationSettings(
                ydb.QueryStaleReadOnly(), interactive=True
            ),
            IsolationLevel.SNAPSHOT_READONLY: IsolationSettings(
                ydb.QuerySnapshotReadOnly(), interactive=True
            ),
        }
        ydb_isolation_settings = ydb_isolation_settings_map[isolation_level]
        if self._tx_context and self._tx_context.tx_id:
            raise InternalError(
                "Failed to set transaction mode: transaction is already began"
            )
        self._tx_mode = ydb_isolation_settings.ydb_mode
        self.interactive_transaction = ydb_isolation_settings.interactive

    def get_isolation_level(self) -> str:
        if self._tx_mode.name == ydb.QuerySerializableReadWrite().name:
            if self.interactive_transaction:
                return IsolationLevel.SERIALIZABLE
            return IsolationLevel.AUTOCOMMIT
        if self._tx_mode.name == ydb.QueryOnlineReadOnly().name:
            if self._tx_mode.settings.allow_inconsistent_reads:
                return IsolationLevel.ONLINE_READONLY_INCONSISTENT
            return IsolationLevel.ONLINE_READONLY
        if self._tx_mode.name == ydb.QueryStaleReadOnly().name:
            return IsolationLevel.STALE_READONLY
        if self._tx_mode.name == ydb.QuerySnapshotReadOnly().name:
            return IsolationLevel.SNAPSHOT_READONLY
        msg = f"{self._tx_mode.name} is not supported"
        raise NotSupportedError(msg)

    def cursor(self) -> AsyncCursor:
        if self._session is None:
            raise RuntimeError("Connection is not ready, use wait_ready.")
        if self._current_cursor and not self._current_cursor.is_closed:
            raise RuntimeError(
                "Unable to create new Cursor before closing existing one."
            )

        if self.interactive_transaction:
            self._tx_context = self._session.transaction(self._tx_mode)
        else:
            self._tx_context = None

        self._current_cursor = AsyncCursor(
            session=self._session,
            tx_context=self._tx_context,
            autocommit=(not self.interactive_transaction),
        )
        return self._current_cursor

    async def commit(self) -> None:
        if self._tx_context and self._tx_context.tx_id:
            await self._tx_context.commit()
            self._tx_context = None

    async def rollback(self) -> None:
        if self._tx_context and self._tx_context.tx_id:
            await self._tx_context.rollback()
            self._tx_context = None

    async def close(self) -> None:
        await self.rollback()

        if self._current_cursor:
            await self._current_cursor.close()

        if self._session:
            await self._session_pool.release(self._session)

        if not self._shared_session_pool:
            await self._session_pool.stop()
            await self._driver.stop()

    @handle_ydb_errors
    async def describe(self, table_path: str) -> ydb.TableSchemeEntry:
        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_path
        )
        return await self._driver.table_client.describe_table(abs_table_path)

    @handle_ydb_errors
    async def check_exists(self, table_path: str) -> bool:
        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_path
        )
        return await self._check_path_exists(abs_table_path)

    @handle_ydb_errors
    async def get_table_names(self) -> list[str]:
        abs_dir_path = posixpath.join(self.database, self.table_path_prefix)
        names = await self._get_table_names(abs_dir_path)
        return [posixpath.relpath(path, abs_dir_path) for path in names]

    async def _check_path_exists(self, table_path: str) -> bool:
        try:

            async def callee() -> None:
                await self._driver.scheme_client.describe_path(table_path)

            await retry_operation_async(callee)
        except ydb.SchemeError:
            return False
        else:
            return True

    async def _get_table_names(self, abs_dir_path: str) -> list[str]:
        async def callee() -> ydb.Directory:
            return await self._driver.scheme_client.list_directory(
                abs_dir_path
            )

        directory = await retry_operation_async(callee)
        result = []
        for child in directory.children:
            child_abs_path = posixpath.join(abs_dir_path, child.name)
            if child.is_table():
                result.append(child_abs_path)
            elif child.is_directory() and not child.name.startswith("."):
                result.extend(self.get_table_names(child_abs_path))
        return result


def connect(*args: tuple, **kwargs: dict) -> Connection:
    conn = Connection(*args, **kwargs)  # type: ignore
    conn.wait_ready()
    return conn


async def async_connect(*args: tuple, **kwargs: dict) -> AsyncConnection:
    conn = AsyncConnection(*args, **kwargs)  # type: ignore
    await conn.wait_ready()
    return conn
