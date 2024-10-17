import dataclasses
import typing
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import ydb

if typing.TYPE_CHECKING:
    from ydb.aio.query.base import AsyncResponseContextIterator


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
        tx_mode: ydb.aio.QueryTxContext,
        tx_context: ydb.aio.QueryTxContext,
        table_path_prefix: str = "",
        autocommit: bool = True,
    ):
        self.arraysize = 1
        self._description = None

        self._pool = session_pool
        self._session = session
        self._tx_mode = tx_mode
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix
        self._autocommit = autocommit

    async def _execute_ddl_query(
        self, query: str, parameters: Optional[ParametersType] = None
    ) -> List[ydb.convert.ResultSet]:
        return await self._pool.execute_with_retries(
            query=query, parameters=parameters
        )

    async def _execute_dml_query(
        self, query: str, parameters: Optional[ParametersType] = None
    ) -> AsyncResponseContextIterator:
        return await self._tx_context.execute(
            query=query,
            parameters=parameters,
            commit_tx=self._autocommit,
        )

    async def execute(
        self, operation: YdbQuery, parameters: Optional[ParametersType] = None
    ):
        if operation.is_ddl:
            result_sets = await self._execute_ddl_query(
                query=operation.yql_text, parameters=parameters
            )
        else:
            result_sets_stream = await self._execute_dml_query(
                query=operation.yql_text, parameters=parameters
            )

        return result_sets or result_sets_stream

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
