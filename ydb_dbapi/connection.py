from typing import (
    Any,
    Optional,
)

import ydb


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

        self.session_pool: ydb.aio.QuerySessionPool = None
        self.session: ydb.aio.QuerySession = None
        self.tx_context: Optional[ydb.QueryTxContext] = None
        self.tx_mode: ydb.BaseQueryTxMode = ydb.QuerySerializableReadWrite()

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
        pass


async def connect() -> Connection:
    return Connection()
