from typing import (
    Any,
    List,
    NamedTuple,
    Optional,
)
import posixpath

import ydb
from ydb.retries import retry_operation_async

from .cursors import Cursor

from .utils import (
    handle_ydb_errors,
)

from .errors import (
    InterfaceError,
    InternalError,
    NotSupportedError,
)


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
    ):
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
            self._session_pool: ydb.aio.SessionPool = self.conn_kwargs.pop(
                "ydb_session_pool"
            )
            self._driver = self._session_pool._driver
        else:
            self._shared_session_pool = False
            driver_config = ydb.DriverConfig(
                endpoint=self.endpoint,
                database=self.database,
                credentials=self.credentials,
            )
            self._driver = ydb.aio.Driver(driver_config)
            self._session_pool = ydb.aio.QuerySessionPool(self._driver, size=5)

        self._tx_context: Optional[ydb.QueryTxContext] = None
        self._tx_mode: ydb.BaseQueryTxMode = ydb.QuerySerializableReadWrite()

        self._current_cursor: Optional[Cursor] = None
        self.interactive_transaction: bool = False

    async def _wait(self, timeout: int = 5):
        try:
            await self._driver.wait(timeout, fail_fast=True)
        except ydb.Error as e:
            raise InterfaceError(e.message, original_error=e) from e
        except Exception as e:
            await self._driver.stop()
            raise InterfaceError(
                "Failed to connect to YDB, details "
                f"{self._driver.discovery_debug_details()}"
            ) from e

    def cursor(self):
        if self._current_cursor and not self._current_cursor._closed:
            raise RuntimeError(
                "Unable to create new Cursor before closing existing one."
            )
        self._current_cursor = Cursor(
            session_pool=self._session_pool,
            tx_context=self._tx_context,
            autocommit=(not self.interactive_transaction),
        )
        return self._current_cursor

    async def begin(self):
        self._tx_context = None
        self._session = await self._session_pool.acquire()
        self._tx_context = self._session.transaction(self._tx_mode)
        # await self._tx_context.begin()

    async def commit(self):
        if self._tx_context and self._tx_context.tx_id:
            await self._tx_context.commit()
            await self._session_pool.release(self._session)
            self._session = None
            self._tx_context = None

    async def rollback(self):
        if self._tx_context and self._tx_context.tx_id:
            await self._tx_context.rollback()
            await self._session_pool.release(self._session)
            self._session = None
            self._tx_context = None

    async def close(self):
        await self.rollback()

        if self._current_cursor:
            await self._current_cursor.close()

        if not self._shared_session_pool:
            await self._session_pool.stop()
            await self._driver.stop()

    def set_isolation_level(self, isolation_level: str):
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
                ydb.QueryOnlineReadOnly(), interactive=False
            ),
            IsolationLevel.ONLINE_READONLY_INCONSISTENT: IsolationSettings(
                ydb.QueryOnlineReadOnly().with_allow_inconsistent_reads(),
                interactive=False,
            ),
            IsolationLevel.STALE_READONLY: IsolationSettings(
                ydb.QueryStaleReadOnly(), interactive=False
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
        if self._tx_mode.name == ydb.SerializableReadWrite().name:
            if self.interactive_transaction:
                return IsolationLevel.SERIALIZABLE
            else:
                return IsolationLevel.AUTOCOMMIT
        elif self._tx_mode.name == ydb.OnlineReadOnly().name:
            if self._tx_mode.settings.allow_inconsistent_reads:
                return IsolationLevel.ONLINE_READONLY_INCONSISTENT
            else:
                return IsolationLevel.ONLINE_READONLY
        elif self._tx_mode.name == ydb.StaleReadOnly().name:
            return IsolationLevel.STALE_READONLY
        elif self._tx_mode.name == ydb.SnapshotReadOnly().name:
            return IsolationLevel.SNAPSHOT_READONLY
        else:
            raise NotSupportedError(f"{self._tx_mode.name} is not supported")

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
    async def get_table_names(self) -> List[str]:
        abs_dir_path = posixpath.join(self.database, self.table_path_prefix)
        names = await self._get_table_names(abs_dir_path)
        return [posixpath.relpath(path, abs_dir_path) for path in names]

    async def _check_path_exists(self, table_path: str) -> bool:
        try:

            async def callee():
                await self._driver.scheme_client.describe_path(table_path)

            await retry_operation_async(callee)
            return True
        except ydb.SchemeError:
            return False

    async def _get_table_names(self, abs_dir_path: str) -> List[str]:
        async def callee():
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


async def connect(*args, **kwargs) -> Connection:
    conn = Connection(*args, **kwargs)
    await conn._wait()
    return conn
