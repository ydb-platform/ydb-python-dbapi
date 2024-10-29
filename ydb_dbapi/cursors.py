from __future__ import annotations

import itertools
from collections.abc import AsyncIterator
from collections.abc import Generator
from collections.abc import Iterator
from typing import Any
from typing import Union

import ydb
from typing_extensions import Self

from .errors import DatabaseError
from .errors import Error
from .errors import InterfaceError
from .errors import ProgrammingError
from .utils import CursorStatus
from .utils import handle_ydb_errors
from .utils import handle_ydb_errors_async

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


class Cursor:
    def __init__(
        self,
        session: ydb.QuerySession,
        tx_context: ydb.QueryTxContext | None = None,
        table_path_prefix: str = "",
        autocommit: bool = True,
    ) -> None:
        self.arraysize: int = 1
        self._description: list[tuple] | None = None

        self._session = session
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix
        self._autocommit = autocommit

        self._stream: Iterator | None = None
        self._rows: Iterator[tuple] | None = None
        self._rows_count: int = -1

        self._closed: bool = False
        self._state = CursorStatus.ready

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

    def _update_result_set(self, result_set: ydb.convert.ResultSet) -> None:
        self._update_description(result_set)
        self._rows = self._rows_iterable(result_set)
        self._rows_count = len(result_set.rows) or -1

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

    def _check_pending_query(self) -> None:
        if self._state == CursorStatus.running:
            raise ProgrammingError(
                "Some records have not been fetched. "
                "Fetch the remaining records before executing the next query."
            )

    def _check_cursor_closed(self) -> None:
        if self._state == CursorStatus.closed:
            raise InterfaceError(
                "Could not perform operation: Cursor is closed."
            )

    def _begin_query(self) -> None:
        self._state = CursorStatus.running

    def fetchone(self) -> tuple | None:
        return next(self._rows or iter([]), None)

    def fetchmany(self, size: int | None = None) -> list | None:
        return (
            list(
                itertools.islice(
                    self._rows or iter([]), size or self.arraysize
                )
            )
            or None
        )

    def fetchall(self) -> list | None:
        return list(self._rows or iter([])) or None

    @handle_ydb_errors
    def _execute_generic_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> Iterator[ydb.convert.ResultSet]:
        return self._session.execute(query=query, parameters=parameters)

    @handle_ydb_errors
    def _execute_transactional_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> Iterator[ydb.convert.ResultSet]:
        if self._tx_context is None:
            raise Error(
                "Unable to execute tx based queries without transaction."
            )
        return self._tx_context.execute(
            query=query,
            parameters=parameters,
            commit_tx=self._autocommit,
        )

    def execute(
        self,
        query: str,
        parameters: ParametersType | None = None,
        prefetch_first_set: bool = True,
    ) -> None:
        self._check_cursor_closed()
        self._check_pending_query()
        if self._tx_context is not None:
            self._stream = self._execute_transactional_query(
                query=query, parameters=parameters
            )
        else:
            self._stream = self._execute_generic_query(
                query=query, parameters=parameters
            )

        if self._stream is None:
            return

        self._begin_query()

        if prefetch_first_set:
            self.nextset()

    async def executemany(self) -> None:
        pass

    @handle_ydb_errors
    def nextset(self) -> bool:
        if self._stream is None:
            return False
        try:
            result_set = self._stream.__next__()
            self._update_result_set(result_set)
        except (StopIteration, StopAsyncIteration, RuntimeError):
            self._state = CursorStatus.finished
            return False
        except ydb.Error:
            self._state = CursorStatus.finished
            raise
        return True

    def finish_query(self) -> None:
        self._check_cursor_closed()

        if self._state != CursorStatus.running:
            return

        next_set_available = True
        while next_set_available:
            next_set_available = self.nextset()

        self._state = CursorStatus.finished

    def close(self) -> None:
        if self._closed:
            return

        self.finish_query()
        self._state = CursorStatus.closed
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        self.close()


class AsyncCursor:
    def __init__(
        self,
        session: ydb.aio.QuerySession,
        tx_context: ydb.aio.QueryTxContext | None = None,
        table_path_prefix: str = "",
        autocommit: bool = True,
    ) -> None:
        self.arraysize: int = 1
        self._description: list[tuple] | None = None

        self._session = session
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix
        self._autocommit = autocommit

        self._stream: AsyncIterator | None = None
        self._rows: Iterator[tuple] | None = None
        self._rows_count: int = -1

        self._closed: bool = False
        self._state = CursorStatus.ready

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

    def _update_result_set(self, result_set: ydb.convert.ResultSet) -> None:
        self._update_description(result_set)
        self._rows = self._rows_iterable(result_set)
        self._rows_count = len(result_set.rows) or -1

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

    def _check_pending_query(self) -> None:
        if self._state == CursorStatus.running:
            raise ProgrammingError(
                "Some records have not been fetched. "
                "Fetch the remaining records before executing the next query."
            )

    def _check_cursor_closed(self) -> None:
        if self._state == CursorStatus.closed:
            raise InterfaceError(
                "Could not perform operation: Cursor is closed."
            )

    def _begin_query(self) -> None:
        self._state = CursorStatus.running

    def fetchone(self) -> tuple | None:
        return next(self._rows or iter([]), None)

    def fetchmany(self, size: int | None = None) -> list | None:
        return (
            list(
                itertools.islice(
                    self._rows or iter([]), size or self.arraysize
                )
            )
            or None
        )

    def fetchall(self) -> list | None:
        return list(self._rows or iter([])) or None

    @handle_ydb_errors_async
    async def _execute_generic_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> AsyncIterator[ydb.convert.ResultSet]:
        return await self._session.execute(query=query, parameters=parameters)

    @handle_ydb_errors_async
    async def _execute_transactional_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> AsyncIterator[ydb.convert.ResultSet]:
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
        parameters: ParametersType | None = None,
        prefetch_first_set: bool = True,
    ) -> None:
        self._check_cursor_closed()
        self._check_pending_query()
        if self._tx_context is not None:
            self._stream = await self._execute_transactional_query(
                query=query, parameters=parameters
            )
        else:
            self._stream = await self._execute_generic_query(
                query=query, parameters=parameters
            )

        if self._stream is None:
            return

        self._begin_query()

        if prefetch_first_set:
            await self.nextset()

    async def executemany(self) -> None:
        pass

    @handle_ydb_errors_async
    async def nextset(self) -> bool:
        if self._stream is None:
            return False
        try:
            result_set = await self._stream.__anext__()
            self._update_result_set(result_set)
        except (StopIteration, StopAsyncIteration, RuntimeError):
            self._state = CursorStatus.finished
            return False
        except ydb.Error:
            self._state = CursorStatus.finished
            raise
        return True

    async def finish_query(self) -> None:
        self._check_cursor_closed()

        if self._state != CursorStatus.running:
            return

        next_set_available = True
        while next_set_available:
            next_set_available = await self.nextset()

        self._state = CursorStatus.finished

    async def close(self) -> None:
        if self._closed:
            return

        await self.finish_query()
        self._state = CursorStatus.closed
        self._closed = True

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        await self.close()
