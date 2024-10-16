import dataclasses
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import ydb


ParametersType = Dict[
    str,
    Union[
        Any,
        Tuple[Any, Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]],
        ydb.TypedValue,
    ],
]


@dataclasses.dataclass
class YdbQuery:
    yql_text: str
    is_ddl: bool = False


class Cursor:
    def __init__(
        self,
        session_pool: ydb.aio.QuerySessionPool,
        session: ydb.aio.QuerySession,
        tx_mode: ydb.BaseQueryTxMode,
        tx_context: Optional[ydb.QueryTxContext] = None,
        table_path_prefix: str = "",
    ):
        self.arraysize = 1
        self._description = None

        self._pool = session_pool
        self._session = session
        self._tx_mode = tx_mode
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix

    async def _execute_ddl_query(
        self, query: YdbQuery, parameters: Optional[ParametersType] = None
    ) -> List[ydb.convert.ResultSet]:
        return await self._pool.execute_with_retries(
            query=query, parameters=parameters
        )

    async def execute(
        self, operation: YdbQuery, parameters: Optional[ParametersType] = None
    ):
        pass

    async def executemany(self):
        pass

    async def fetchone(self):
        pass

    async def fetchmany(self):
        pass

    async def fetchall(self):
        pass

    async def nextset(self):
        pass

    def setinputsizes(self):
        pass

    def setoutputsize(self):
        pass

    async def close(self):
        pass

    @property
    def description(self):
        pass

    @property
    def rowcount(self):
        pass
