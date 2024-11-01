from __future__ import annotations

import itertools
from collections.abc import AsyncIterator
from collections.abc import Generator
from collections.abc import Iterator
from collections.abc import Sequence
from typing import Any
from typing import Union

import ydb
from typing_extensions import Self

from .errors import DatabaseError
from .errors import InterfaceError
from .errors import ProgrammingError
from .utils import CursorStatus
from .utils import handle_ydb_errors

ParametersType = dict[
    str,
    Union[
        Any,
        tuple[Any, Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]],
        ydb.TypedValue,
    ],
]


def _get_column_type(type_obj: Any) -> str:
    return str(ydb.convert.type_to_native(type_obj))


class BufferedCursor:
    def __init__(self) -> None:
        self.arraysize: int = 1
        self._rows: Iterator | None = None
        self._rows_count: int = -1
        self._description: list[tuple] | None = None
        self._state: CursorStatus = CursorStatus.ready

    @property
    def description(self) -> list[tuple] | None:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rows_count

    def setinputsizes(self) -> None:
        pass

    def setoutputsize(self) -> None:
        pass

    def _rows_iterable(
        self, result_set: ydb.convert.ResultSet
    ) -> Generator[tuple]:
        try:
            for row in result_set.rows:
                # returns tuple to be compatible with SqlAlchemy and because
                #  of this PEP to return a sequence:
                # https://www.python.org/dev/peps/pep-0249/#fetchmany
                yield row[::]
        except ydb.Error as e:
            raise DatabaseError(e.message, original_error=e) from e

    def _update_result_set(
        self,
        result_set: ydb.convert.ResultSet,
        replace_current: bool = True,
    ) -> None:
        self._update_description(result_set)

        new_rows_iter = self._rows_iterable(result_set)
        new_rows_count = len(result_set.rows) or -1

        if self._rows is None or replace_current:
            self._rows = new_rows_iter
            self._rows_count = new_rows_count
        else:
            self._rows = itertools.chain(self._rows, new_rows_iter)
            if new_rows_count != -1:
                if self._rows_count != -1:
                    self._rows_count += new_rows_count
                else:
                    self._rows_count = new_rows_count

    def _update_description(self, result_set: ydb.convert.ResultSet) -> None:
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

    def _raise_if_running(self) -> None:
        if self._state == CursorStatus.running:
            raise ProgrammingError(
                "Some records have not been fetched. "
                "Fetch the remaining records before executing the next query."
            )

    def _raise_if_closed(self) -> None:
        if self.is_closed:
            raise InterfaceError(
                "Could not perform operation: Cursor is closed."
            )

    @property
    def is_closed(self) -> bool:
        return self._state == CursorStatus.closed

    def _begin_query(self) -> None:
        self._state = CursorStatus.running

    def _fetchone_from_buffer(self) -> tuple | None:
        self._raise_if_closed()
        return next(self._rows or iter([]), None)

    def _fetchmany_from_buffer(self, size: int | None = None) -> list:
        self._raise_if_closed()
        return list(
            itertools.islice(self._rows or iter([]), size or self.arraysize)
        )

    def _fetchall_from_buffer(self) -> list:
        self._raise_if_closed()
        return list(self._rows or iter([]))


class Cursor(BufferedCursor):
    def __init__(
        self,
        session_pool: ydb.QuerySessionPool,
        tx_mode: ydb.BaseQueryTxMode,
        tx_context: ydb.QueryTxContext | None = None,
        table_path_prefix: str = "",
    ) -> None:
        super().__init__()
        self._session_pool = session_pool
        self._tx_mode = tx_mode
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix

        self._stream: Iterator | None = None

    def fetchone(self) -> tuple | None:
        return self._fetchone_from_buffer()

    def fetchmany(self, size: int | None = None) -> list:
        size = size or self.arraysize
        return self._fetchmany_from_buffer(size)

    def fetchall(self) -> list:
        return self._fetchall_from_buffer()

    @handle_ydb_errors
    def _execute_generic_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> Iterator[ydb.convert.ResultSet]:
        def callee(
            session: ydb.QuerySession,
        ) -> Iterator[ydb.convert.ResultSet]:
            return session.execute(
                query=query,
                parameters=parameters,
            )

        return self._session_pool.retry_operation_sync(callee)

    @handle_ydb_errors
    def _execute_session_query(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> Iterator[ydb.convert.ResultSet]:
        def callee(
            session: ydb.QuerySession,
        ) -> Iterator[ydb.convert.ResultSet]:
            return session.transaction(self._tx_mode).execute(
                query=query,
                parameters=parameters,
                commit_tx=True,
            )

        return self._session_pool.retry_operation_sync(callee)

    @handle_ydb_errors
    def _execute_transactional_query(
        self,
        tx_context: ydb.QueryTxContext,
        query: str,
        parameters: ParametersType | None = None,
    ) -> Iterator[ydb.convert.ResultSet]:
        return tx_context.execute(
            query=query,
            parameters=parameters,
            commit_tx=False,
        )

    def execute_scheme(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()

        self._stream = self._execute_generic_query(
            query=query, parameters=parameters
        )
        self._begin_query()
        self._scroll_stream(replace_current=False)

    def execute(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()
        if self._tx_context is not None:
            self._stream = self._execute_transactional_query(
                tx_context=self._tx_context, query=query, parameters=parameters
            )
        else:
            self._stream = self._execute_session_query(
                query=query, parameters=parameters
            )

        self._begin_query()
        self._scroll_stream(replace_current=False)

    def executemany(
        self, query: str, seq_of_parameters: Sequence[ParametersType]
    ) -> None:
        for parameters in seq_of_parameters:
            self.execute(query, parameters)

    @handle_ydb_errors
    def nextset(self, replace_current: bool = True) -> bool:
        if self._stream is None:
            return False
        try:
            result_set = self._stream.__next__()
            self._update_result_set(result_set, replace_current)
        except (StopIteration, StopAsyncIteration, RuntimeError):
            self._state = CursorStatus.finished
            return False
        except ydb.Error:
            self._state = CursorStatus.finished
            raise
        return True

    def _scroll_stream(self, replace_current: bool = True) -> None:
        self._raise_if_closed()

        next_set_available = True
        while next_set_available:
            next_set_available = self.nextset(replace_current)

        self._state = CursorStatus.finished

    def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

        self._scroll_stream()
        self._state = CursorStatus.closed

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        self.close()


class AsyncCursor(BufferedCursor):
    def __init__(
        self,
        session_pool: ydb.aio.QuerySessionPool,
        tx_mode: ydb.BaseQueryTxMode,
        tx_context: ydb.aio.QueryTxContext | None = None,
        table_path_prefix: str = "",
    ) -> None:
        super().__init__()
        self._session_pool = session_pool
        self._tx_mode = tx_mode
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix

        self._stream: AsyncIterator | None = None

    async def fetchone(self) -> tuple | None:
        return self._fetchone_from_buffer()

    async def fetchmany(self, size: int | None = None) -> list:
        size = size or self.arraysize
        return self._fetchmany_from_buffer(size)

    async def fetchall(self) -> list:
        return self._fetchall_from_buffer()

    @handle_ydb_errors
    async def _execute_generic_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> AsyncIterator[ydb.convert.ResultSet]:
        async def callee(
            session: ydb.aio.QuerySession,
        ) -> AsyncIterator[ydb.convert.ResultSet]:
            return await session.execute(
                query=query,
                parameters=parameters,
            )

        return await self._session_pool.retry_operation_async(callee)

    @handle_ydb_errors
    async def _execute_session_query(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> AsyncIterator[ydb.convert.ResultSet]:
        async def callee(
            session: ydb.aio.QuerySession,
        ) -> AsyncIterator[ydb.convert.ResultSet]:
            return await session.transaction(self._tx_mode).execute(
                query=query,
                parameters=parameters,
                commit_tx=True,
            )

        return await self._session_pool.retry_operation_async(callee)

    @handle_ydb_errors
    async def _execute_transactional_query(
        self,
        tx_context: ydb.aio.QueryTxContext,
        query: str,
        parameters: ParametersType | None = None,
    ) -> AsyncIterator[ydb.convert.ResultSet]:
        return await tx_context.execute(
            query=query,
            parameters=parameters,
            commit_tx=False,
        )

    async def execute_scheme(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()

        self._stream = await self._execute_generic_query(
            query=query, parameters=parameters
        )
        self._begin_query()
        await self._scroll_stream(replace_current=False)

    async def execute(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()
        if self._tx_context is not None:
            self._stream = await self._execute_transactional_query(
                tx_context=self._tx_context, query=query, parameters=parameters
            )
        else:
            self._stream = await self._execute_session_query(
                query=query, parameters=parameters
            )

        self._begin_query()
        await self._scroll_stream(replace_current=False)

    async def executemany(
        self, query: str, seq_of_parameters: Sequence[ParametersType]
    ) -> None:
        for parameters in seq_of_parameters:
            await self.execute(query, parameters)

    @handle_ydb_errors
    async def nextset(self, replace_current: bool = True) -> bool:
        if self._stream is None:
            return False
        try:
            result_set = await self._stream.__anext__()
            self._update_result_set(result_set, replace_current)
        except (StopIteration, StopAsyncIteration, RuntimeError):
            self._stream = None
            self._state = CursorStatus.finished
            return False
        except ydb.Error:
            self._state = CursorStatus.finished
            raise
        return True

    async def _scroll_stream(self, replace_current: bool = True) -> None:
        self._raise_if_closed()

        next_set_available = True
        while next_set_available:
            next_set_available = await self.nextset(replace_current)

        self._state = CursorStatus.finished

    async def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

        await self._scroll_stream()
        self._state = CursorStatus.closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        await self.close()
