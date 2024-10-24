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


class BaseCursor:
    arraysize: int = 1
    _rows: Iterator | None = None
    _rows_count: int = -1
    _description: list[tuple] | None = None
    _state: CursorStatus = CursorStatus.ready

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
        return next(self._rows or iter([]), None)

    def _fetchmany_from_buffer(self, size: int | None = None) -> list | None:
        return (
            list(
                itertools.islice(
                    self._rows or iter([]), size or self.arraysize
                )
            )
            or None
        )

    def _fetchall_from_buffer(self) -> list | None:
        return list(self._rows or iter([])) or None


class Cursor(BaseCursor):
    def __init__(
        self,
        session: ydb.QuerySession,
        tx_context: ydb.QueryTxContext | None = None,
        table_path_prefix: str = "",
        autocommit: bool = True,
        auto_scroll_result_sets: bool = False,
    ) -> None:
        self._session = session
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix
        self._autocommit = autocommit
        self._auto_scroll = auto_scroll_result_sets

        self._stream: Iterator | None = None

    def fetchone(self) -> tuple | None:
        row = self._fetchone_from_buffer()
        if not self._auto_scroll:
            return row

        if row is None:
            while self.nextset():
                # We should skip empty result sets
                row = self._fetchone_from_buffer()
                if row is not None:
                    return row

        return row

    def fetchmany(self, size: int | None = None) -> list | None:
        return self._fetchmany_from_buffer(size)

    def fetchall(self) -> list | None:
        return self._fetchall_from_buffer()

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
        self._raise_if_closed()
        self._raise_if_running()
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
        self._raise_if_closed()

        next_set_available = True
        while next_set_available:
            next_set_available = self.nextset()

        self._state = CursorStatus.finished

    def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

        self.finish_query()
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


class AsyncCursor(BaseCursor):
    def __init__(
        self,
        session: ydb.aio.QuerySession,
        tx_context: ydb.aio.QueryTxContext | None = None,
        table_path_prefix: str = "",
        autocommit: bool = True,
    ) -> None:
        self._session = session
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix
        self._autocommit = autocommit

        self._stream: AsyncIterator | None = None

    async def fetchone(self) -> tuple | None:
        return self._fetchone_from_buffer()

    async def fetchmany(self, size: int | None = None) -> list | None:
        return self._fetchmany_from_buffer(size)

    async def fetchall(self) -> list | None:
        return self._fetchall_from_buffer()

    @handle_ydb_errors
    async def _execute_generic_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> AsyncIterator[ydb.convert.ResultSet]:
        return await self._session.execute(query=query, parameters=parameters)

    @handle_ydb_errors
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
        self._raise_if_closed()
        self._raise_if_running()
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

    @handle_ydb_errors
    async def nextset(self) -> bool:
        if self._stream is None:
            return False
        try:
            result_set = await self._stream.__anext__()
            self._update_result_set(result_set)
        except (StopIteration, StopAsyncIteration, RuntimeError):
            self._stream = None
            self._state = CursorStatus.finished
            return False
        except ydb.Error:
            self._state = CursorStatus.finished
            raise
        return True

    async def finish_query(self) -> None:
        self._raise_if_closed()

        next_set_available = True
        while next_set_available:
            next_set_available = await self.nextset()

        self._state = CursorStatus.finished

    async def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

        await self.finish_query()
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
