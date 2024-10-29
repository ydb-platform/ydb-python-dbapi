import dataclasses
import itertools
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import ydb
from .errors import DatabaseError
from .utils import handle_ydb_errors, AsyncFromSyncIterator


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


def _get_column_type(type_obj: Any) -> str:
    return str(ydb.convert.type_to_native(type_obj))


class Cursor:
    def __init__(
        self,
        session_pool: ydb.aio.QuerySessionPool,
        session: ydb.aio.QuerySession,
        tx_mode: Optional[ydb.aio.QueryTxContext] = None,
        tx_context: Optional[ydb.aio.QueryTxContext] = None,
        table_path_prefix: str = "",
        autocommit: bool = True,
    ):
        self.arraysize: int = 1
        self._description: Optional[List[Tuple]] = None

        self._pool = session_pool
        self._session = session
        self._tx_mode = tx_mode
        self._tx_context: ydb.aio.QueryTxContext = tx_context
        self._table_path_prefix = table_path_prefix
        self._autocommit = autocommit

        self._stream: Optional[AsyncIterator] = None
        self._rows: Optional[Iterator[Dict]] = None

    @handle_ydb_errors
    async def _execute_ddl_query(
        self, query: str, parameters: Optional[ParametersType] = None
    ) -> List[ydb.convert.ResultSet]:
        return await self._pool.execute_with_retries(
            query=query, parameters=parameters
        )

    @handle_ydb_errors
    async def _execute_dml_query(
        self, query: str, parameters: Optional[ParametersType] = None
    ) -> AsyncIterator:
        return await self._tx_context.execute(
            query=query,
            parameters=parameters,
            commit_tx=self._autocommit,
        )

    @handle_ydb_errors
    async def execute(
        self, operation: YdbQuery, parameters: Optional[ParametersType] = None
    ):
        if operation.is_ddl:
            result_sets = await self._execute_ddl_query(
                query=operation.yql_text, parameters=parameters
            )
            self._stream = AsyncFromSyncIterator(iter(result_sets))
        else:
            self._stream = await self._execute_dml_query(
                query=operation.yql_text, parameters=parameters
            )

        if self._stream is None:
            return

        result_set = await self._stream.__anext__()
        self._update_result_set(result_set)

    def _update_result_set(self, result_set: ydb.convert.ResultSet):
        # self._result_set = result_set
        self._update_description(result_set)
        self._rows = self._rows_iterable(result_set)

    def _update_description(self, result_set: ydb.convert.ResultSet):
        self._description = [
            (
                col.name,
                _get_column_type(col.type),
                None,
                None,
                None,
                None,
                None,
            )
            for col in result_set.columns
        ]

    def _rows_iterable(self, result_set):
        try:
            for row in result_set.rows:
                # returns tuple to be compatible with SqlAlchemy and because
                #  of this PEP to return a sequence:
                # https://www.python.org/dev/peps/pep-0249/#fetchmany
                yield row[::]
        except ydb.Error as e:
            raise DatabaseError(e.message, original_error=e) from e

    async def executemany(self):
        pass

    async def fetchone(self):
        return next(self._rows or iter([]), None)

    async def fetchmany(self, size: Optional[int] = None):
        return list(
            itertools.islice(self._rows or iter([]), size or self.arraysize)
        )

    async def fetchall(self):
        return list(self._rows or iter([]))

    async def nextset(self):
        if self._stream is None:
            return False
        try:
            result_set = await self._stream.__anext__()
            self._update_result_set(result_set)
        except StopIteration:
            return False
        return True

    def setinputsizes(self):
        pass

    def setoutputsize(self):
        pass

    async def close(self):
        next_set_available = True
        while next_set_available:
            next_set_available = await self.nextset()

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self):
        pass
