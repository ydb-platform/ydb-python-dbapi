from typing import (
    Any,
    List,
    NamedTuple,
    Optional,
)
import posixpath

import ydb
from ydb.retries import retry_operation_async

from .utils import (
    handle_ydb_errors,
)

from .errors import (
    # InterfaceError,
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

        self.driver: ydb.aio.Driver = self.conn_kwargs.pop("ydb_driver", None)

        self.session_pool: ydb.aio.QuerySessionPool = self.conn_kwargs.pop(
            "ydb_session_pool", None
        )
        self.session: ydb.aio.QuerySession = None
        self.tx_context: Optional[ydb.QueryTxContext] = None
        self.tx_mode: ydb.BaseQueryTxMode = ydb.QuerySerializableReadWrite()

        self.interactive_transaction: bool = False  # AUTOCOMMIT

    def cursor(self):
        pass

    async def begin(self):
        self.tx_context = None
        self.session = await self.session_pool.acquire()
        self.tx_context = self.session.transaction(self.tx_mode)
        await self.tx_context.begin()

    async def commit(self):
        if self.tx_context and self.tx_context.tx_id:
            await self.tx_context.commit()
            await self.session_pool.release(self.session)
            self.session = None
            self.tx_context = None

    async def rollback(self):
        if self.tx_context and self.tx_context.tx_id:
            await self.tx_context.rollback()
            await self.session_pool.release(self.session)
            self.session = None
            self.tx_context = None

    async def close(self):
        await self.rollback()

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
        if self.tx_context and self.tx_context.tx_id:
            raise InternalError(
                "Failed to set transaction mode: transaction is already began"
            )
        self.tx_mode = ydb_isolation_settings.ydb_mode
        self.interactive_transaction = ydb_isolation_settings.interactive

    def get_isolation_level(self) -> str:
        if self.tx_mode.name == ydb.SerializableReadWrite().name:
            if self.interactive_transaction:
                return IsolationLevel.SERIALIZABLE
            else:
                return IsolationLevel.AUTOCOMMIT
        elif self.tx_mode.name == ydb.OnlineReadOnly().name:
            if self.tx_mode.settings.allow_inconsistent_reads:
                return IsolationLevel.ONLINE_READONLY_INCONSISTENT
            else:
                return IsolationLevel.ONLINE_READONLY
        elif self.tx_mode.name == ydb.StaleReadOnly().name:
            return IsolationLevel.STALE_READONLY
        elif self.tx_mode.name == ydb.SnapshotReadOnly().name:
            return IsolationLevel.SNAPSHOT_READONLY
        else:
            raise NotSupportedError(f"{self.tx_mode.name} is not supported")

    @handle_ydb_errors
    async def describe(self, table_path: str) -> ydb.TableSchemeEntry:
        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_path
        )
        return self.driver.table_client.describe_table(abs_table_path)

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
            await retry_operation_async(
                self.driver.scheme_client.describe_path, table_path
            )
            return True
        except ydb.SchemeError:
            return False

    async def _get_table_names(self, abs_dir_path: str) -> List[str]:
        directory = await retry_operation_async(
            self.driver.scheme_client.list_directory, abs_dir_path
        )
        result = []
        for child in directory.children:
            child_abs_path = posixpath.join(abs_dir_path, child.name)
            if child.is_table():
                result.append(child_abs_path)
            elif child.is_directory() and not child.name.startswith("."):
                result.extend(self.get_table_names(child_abs_path))
        return result


async def connect() -> Connection:
    return Connection()
