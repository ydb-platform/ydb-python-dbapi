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
from .errors import Error, DatabaseError, InterfaceError, ProgrammingError
from .utils import handle_ydb_errors, AsyncFromSyncIterator, CursorStatus


ParametersType = Dict[
    str,
    Union[
        Any,
        Tuple[Any, Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]],
        ydb.TypedValue,
    ],
]


def _get_column_type(type_obj: Any) -> str:
    return str(ydb.convert.type_to_native(type_obj))


class Cursor:
    def __init__(
        self,
        session_pool: ydb.aio.QuerySessionPool,
        tx_context: Optional[ydb.aio.QueryTxContext] = None,
        table_path_prefix: str = "",
        autocommit: bool = True,
    ):
        self.arraysize: int = 1
        self._description: Optional[List[Tuple]] = None

        self._session_pool = session_pool
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix
        self._autocommit = autocommit

        self._stream: Optional[AsyncIterator] = None
        self._rows: Optional[Iterator[Dict]] = None
        self._rows_count: int = -1

        self._closed: bool = False
        self._state = CursorStatus.ready

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self):
        return self._rows_count

    @handle_ydb_errors
    async def _execute_generic_query(
        self, query: str, parameters: Optional[ParametersType] = None
    ) -> List[ydb.convert.ResultSet]:
        return await self._session_pool.execute_with_retries(
            query=query, parameters=parameters
        )

    @handle_ydb_errors
    async def _execute_transactional_query(
        self, query: str, parameters: Optional[ParametersType] = None
    ) -> AsyncIterator:
        if self._tx_context is None:
            raise Error(
                "Unable to execute tx based queries without transaction."
            )
        return await self._tx_context.execute(
            query=query,
            parameters=parameters,
            commit_tx=self._autocommit,
        )

    async def execute(
        self,
        query: str,
        parameters: Optional[ParametersType] = None,
        prefetch_first_set: bool = True,
    ):
        self._check_cursor_closed()
        self._check_pending_query()
        if self._tx_context is not None:
            self._stream = await self._execute_transactional_query(
                query=query, parameters=parameters
            )
        else:
            result_sets = await self._execute_generic_query(
                query=query, parameters=parameters
            )
            self._stream = AsyncFromSyncIterator(iter(result_sets))

        if self._stream is None:
            return

        self._begin_query()

        if prefetch_first_set:
            await self.nextset()

    def _update_result_set(self, result_set: ydb.convert.ResultSet):
        self._update_description(result_set)
        self._rows = self._rows_iterable(result_set)
        self._rows_count = len(result_set.rows) or -1

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
        return (
            list(
                itertools.islice(
                    self._rows or iter([]), size or self.arraysize
                )
            )
            or None
        )

    async def fetchall(self):
        return list(self._rows or iter([])) or None

    @handle_ydb_errors
    async def nextset(self):
        if self._stream is None:
            return False
        try:
            result_set = await self._stream.__anext__()
            self._update_result_set(result_set)
        except (StopIteration, StopAsyncIteration, RuntimeError):
            self._state = CursorStatus.finished
            return False
        except ydb.Error as e:
            self._state = CursorStatus.finished
            raise e
        return True

    async def finish_query(self):
        self._check_cursor_closed()

        if not self._state == CursorStatus.running:
            return

        next_set_available = True
        while next_set_available:
            next_set_available = await self.nextset()

        self._state = CursorStatus.finished

    def setinputsizes(self):
        pass

    def setoutputsize(self):
        pass

    async def close(self):
        if self._closed:
            return

        await self.finish_query()
        self._state = CursorStatus.closed
        self._closed = True

    def _begin_query(self):
        self._state = CursorStatus.running

    def _check_pending_query(self):
        if self._state == CursorStatus.running:
            raise ProgrammingError(
                "Some records have not been fetched. "
                "Fetch the remaining records before executing the next query."
            )

    def _check_cursor_closed(self):
        if self._state == CursorStatus.closed:
            raise InterfaceError(
                "Could not perform operation: Cursor is closed."
            )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
