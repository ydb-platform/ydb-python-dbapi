from __future__ import annotations

import functools
import itertools
from collections.abc import AsyncIterator
from collections.abc import Generator
from collections.abc import Iterator
from collections.abc import Sequence
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Union

import ydb
from typing_extensions import Self
from ydb.aio.query.base import AsyncResponseContextIterator
from ydb.query.base import SyncResponseContextIterator
from ydb.retries import retry_operation_async
from ydb.retries import retry_operation_sync

from .errors import DatabaseError
from .errors import InterfaceError
from .errors import ProgrammingError
from .utils import CursorStatus
from .utils import handle_ydb_errors
from .utils import maybe_get_current_trace_id

if TYPE_CHECKING:
    from .connections import AsyncConnection
    from .connections import Connection

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


def invalidate_cursor_on_ydb_error(func: Callable) -> Callable:
    if iscoroutinefunction(func):

        @functools.wraps(func)
        async def awrapper(
            self: AsyncCursor, *args: tuple, **kwargs: dict
        ) -> Any:
            try:
                return await func(self, *args, **kwargs)
            except ydb.Error:
                self._state = CursorStatus.finished
                await self._invalidate_active_session()
                raise

        return awrapper

    @functools.wraps(func)
    def wrapper(self: Cursor, *args: tuple, **kwargs: dict) -> Any:
        try:
            return func(self, *args, **kwargs)
        except ydb.Error:
            self._state = CursorStatus.finished
            self._invalidate_active_session()
            raise

    return wrapper


class BaseCursor:
    def __init__(self) -> None:
        self.arraysize: int = 1
        self._rows: Iterator | None = None
        self._rows_count: int = -1
        self._description: list[tuple] | None = None
        self._state: CursorStatus = CursorStatus.ready
        self._rowcount_accumulator: int = 0

        self._table_path_prefix: str = ""

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

    def _reset_result_state(self) -> None:
        self._rows = None
        self._rows_count = -1
        self._description = None
        self._rowcount_accumulator = 0

    def _rows_iterable(
        self, result_set: ydb.convert.ResultSet
    ) -> Generator[tuple]:
        try:
            for row in result_set.rows:
                # returns tuple to be compatible with SqlAlchemy and because
                # of this PEP to return a sequence:
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
        self._rowcount_accumulator += len(result_set.rows)

        new_rows_iter = self._rows_iterable(result_set)

        if self._rows is None or replace_current:
            self._rows = new_rows_iter
        else:
            self._rows = itertools.chain(self._rows, new_rows_iter)

    def _update_description(self, result_set: ydb.convert.ResultSet) -> None:
        if not result_set.columns:
            # We should not rely on 'empty' result sets,
            # because they can appear at any moment
            return

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

    def _fill_buffer(self, result_set_list: list) -> None:
        for result_set in result_set_list:
            self._update_result_set(result_set, replace_current=False)

    def _finalize_rowcount(self) -> None:
        self._rows_count = self._rowcount_accumulator

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
        self._reset_result_state()
        self._state = CursorStatus.running

    def _finish_query(self) -> None:
        self._finalize_rowcount()
        self._state = CursorStatus.finished

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

    def _append_table_path_prefix(self, query: str) -> str:
        if self._table_path_prefix:
            prgm = f'PRAGMA TablePathPrefix = "{self._table_path_prefix}";\n'
            return prgm + query
        return query


class Cursor(BaseCursor):
    def __init__(
        self,
        connection: Connection,
        session_pool: ydb.QuerySessionPool,
        tx_mode: ydb.BaseQueryTxMode,
        request_settings: ydb.BaseRequestSettings,
        retry_settings: ydb.RetrySettings,
        tx_context: ydb.QueryTxContext | None = None,
        table_path_prefix: str = "",
    ) -> None:
        super().__init__()
        self._connection = connection
        self._session_pool = session_pool
        self._tx_mode = tx_mode
        self._request_settings = request_settings
        self._retry_settings = retry_settings
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix

    def fetchone(self) -> tuple | None:
        return self._fetchone_from_buffer()

    def fetchmany(self, size: int | None = None) -> list:
        size = size or self.arraysize
        return self._fetchmany_from_buffer(size)

    def fetchall(self) -> list:
        return self._fetchall_from_buffer()

    def _invalidate_active_session(self) -> None:
        self._connection._invalidate_session()

    def _get_request_settings(self) -> ydb.BaseRequestSettings:
        settings = self._request_settings.make_copy()

        if self._request_settings.trace_id is None:
            settings = settings.with_trace_id(maybe_get_current_trace_id())

        return settings

    def _materialize(
        self, stream: Iterator[ydb.convert.ResultSet]
    ) -> list[ydb.convert.ResultSet]:
        return list(stream)

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    def _execute_generic_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()

        def callee(
            session: ydb.QuerySession,
        ) -> list[ydb.convert.ResultSet]:
            return self._materialize(
                session.execute(
                    query=query,
                    parameters=parameters,
                    settings=settings,
                )
            )

        return self._session_pool.retry_operation_sync(
            callee,
            retry_settings=self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    def _execute_session_query(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()

        def callee(
            session: ydb.QuerySession,
        ) -> list[ydb.convert.ResultSet]:
            return self._materialize(
                session.transaction(self._tx_mode).execute(
                    query=query,
                    parameters=parameters,
                    commit_tx=True,
                    settings=settings,
                )
            )

        return self._session_pool.retry_operation_sync(
            callee,
            retry_settings=self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    def _execute_transactional_query(
        self,
        tx_context: ydb.QueryTxContext,
        query: str,
        parameters: ParametersType | None = None,
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()
        return self._materialize(
            tx_context.execute(
                query=query,
                parameters=parameters,
                commit_tx=False,
                settings=settings,
            )
        )

    def execute_scheme(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()

        query = self._append_table_path_prefix(query)
        self._begin_query()

        result_list = self._execute_generic_query(
            query=query, parameters=parameters
        )
        self._fill_buffer(result_list)
        self._finish_query()

    def execute(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()
        if self._tx_context is not None:
            self._connection._raise_if_current_cursor_running()

        query = self._append_table_path_prefix(query)
        self._begin_query()

        if self._tx_context is not None:
            result_list = self._execute_transactional_query(
                tx_context=self._tx_context, query=query, parameters=parameters
            )
        else:
            result_list = self._execute_session_query(
                query=query, parameters=parameters
            )

        self._fill_buffer(result_list)
        self._finish_query()

    def executemany(
        self, query: str, seq_of_parameters: Sequence[ParametersType]
    ) -> None:
        for parameters in seq_of_parameters:
            self.execute(query, parameters)

    def nextset(self) -> bool:
        return False

    def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

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


class StreamCursor(Cursor):
    def __init__(
        self,
        connection: Connection,
        session_pool: ydb.QuerySessionPool,
        tx_mode: ydb.BaseQueryTxMode,
        request_settings: ydb.BaseRequestSettings,
        retry_settings: ydb.RetrySettings,
        tx_context: ydb.QueryTxContext | None = None,
        table_path_prefix: str = "",
    ) -> None:
        super().__init__(
            connection=connection,
            session_pool=session_pool,
            tx_mode=tx_mode,
            request_settings=request_settings,
            retry_settings=retry_settings,
            tx_context=tx_context,
            table_path_prefix=table_path_prefix,
        )
        self._stream: SyncResponseContextIterator | None = None
        self._session_owner: ydb.QuerySession | None = None

    def _invalidate_active_session(self) -> None:
        self._clear_current_cursor()
        if self._session_owner is None:
            self._connection._invalidate_session()
            return

        session = self._session_owner
        self._session_owner = None
        self._stream = None
        try:
            session.delete()
        finally:
            self._session_pool.release(session)

    def _clear_current_cursor(self) -> None:
        self._connection._clear_current_cursor(self)

    def _register_current_cursor(self) -> None:
        if self._tx_context is not None:
            self._connection._set_current_cursor(self)

    def _release_owned_session(self) -> None:
        if self._session_owner is None:
            return

        session = self._session_owner
        self._session_owner = None
        self._session_pool.release(session)

    def _discard_owned_session(self) -> None:
        if self._session_owner is None:
            return

        session = self._session_owner
        self._session_owner = None
        try:
            session.delete()
        finally:
            self._session_pool.release(session)

    def _finish_stream(self) -> None:
        self._stream = None
        self._release_owned_session()
        self._clear_current_cursor()
        self._finish_query()

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    def _load_next_result_set(self) -> bool:
        if self._stream is None:
            return False

        try:
            result_set = next(self._stream)
        except StopIteration:
            self._finish_stream()
            return False

        self._update_result_set(result_set, replace_current=False)
        return True

    def _prime_stream(self) -> None:
        if (
            not self._load_next_result_set()
            and self._state == CursorStatus.running
        ):
            self._finish_stream()

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    def _execute_session_query_stream(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> SyncResponseContextIterator:
        settings = self._get_request_settings()

        def callee() -> SyncResponseContextIterator:
            acquire_timeout = getattr(
                self._retry_settings,
                "max_session_acquire_timeout",
                None,
            )
            session = self._session_pool.acquire(timeout=acquire_timeout)
            try:
                stream = session.transaction(self._tx_mode).execute(
                    query=query,
                    parameters=parameters,
                    commit_tx=True,
                    settings=settings,
                )
            except Exception:
                self._session_pool.release(session)
                raise

            self._session_owner = session
            return stream

        return retry_operation_sync(
            callee,
            self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    def _execute_transactional_query_stream(
        self,
        tx_context: ydb.QueryTxContext,
        query: str,
        parameters: ParametersType | None = None,
    ) -> SyncResponseContextIterator:
        settings = self._get_request_settings()
        return tx_context.execute(
            query=query,
            parameters=parameters,
            commit_tx=False,
            settings=settings,
        )

    def execute(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()
        if self._tx_context is not None:
            self._connection._raise_if_current_cursor_running(self)

        query = self._append_table_path_prefix(query)
        self._begin_query()

        if self._tx_context is not None:
            self._stream = self._execute_transactional_query_stream(
                tx_context=self._tx_context,
                query=query,
                parameters=parameters,
            )
            self._register_current_cursor()
        else:
            self._stream = self._execute_session_query_stream(
                query=query,
                parameters=parameters,
            )

        self._prime_stream()

    def fetchone(self) -> tuple | None:
        self._raise_if_closed()

        while True:
            row = self._fetchone_from_buffer()
            if row is not None:
                return row
            if not self._load_next_result_set():
                return None

    def fetchmany(self, size: int | None = None) -> list:
        self._raise_if_closed()
        result: list[tuple] = []
        target_size = size or self.arraysize

        while len(result) < target_size:
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def fetchall(self) -> list:
        self._raise_if_closed()
        result = list(self._fetchall_from_buffer())

        while self._load_next_result_set():
            result.extend(self._fetchall_from_buffer())

        return result

    def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

        if self._state == CursorStatus.running:
            if self._session_owner is not None and self._stream is not None:
                self._stream.cancel()
                self._stream = None
                self._discard_owned_session()
            elif self._stream is not None:
                with self._stream:
                    pass
                self._finish_stream()
            else:
                self._release_owned_session()
                self._clear_current_cursor()

        self._stream = None
        self._session_owner = None
        self._clear_current_cursor()
        self._state = CursorStatus.closed


class AsyncCursor(BaseCursor):
    def __init__(
        self,
        connection: AsyncConnection,
        session_pool: ydb.aio.QuerySessionPool,
        tx_mode: ydb.BaseQueryTxMode,
        request_settings: ydb.BaseRequestSettings,
        retry_settings: ydb.RetrySettings,
        tx_context: ydb.aio.QueryTxContext | None = None,
        table_path_prefix: str = "",
    ) -> None:
        super().__init__()
        self._connection = connection
        self._session_pool = session_pool
        self._tx_mode = tx_mode
        self._request_settings = request_settings
        self._retry_settings = retry_settings
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix

    def fetchone(self) -> tuple | None:
        return self._fetchone_from_buffer()

    def fetchmany(self, size: int | None = None) -> list:
        size = size or self.arraysize
        return self._fetchmany_from_buffer(size)

    def fetchall(self) -> list:
        return self._fetchall_from_buffer()

    async def _invalidate_active_session(self) -> None:
        await self._connection._invalidate_session()

    def _get_request_settings(self) -> ydb.BaseRequestSettings:
        settings = self._request_settings.make_copy()

        if self._request_settings.trace_id is None:
            settings = settings.with_trace_id(maybe_get_current_trace_id())

        return settings

    async def _materialize(
        self, stream: AsyncIterator[ydb.convert.ResultSet]
    ) -> list[ydb.convert.ResultSet]:
        return [result_set async for result_set in stream]

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    async def _execute_generic_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()

        async def callee(
            session: ydb.aio.QuerySession,
        ) -> list[ydb.convert.ResultSet]:
            return await self._materialize(
                await session.execute(
                    query=query,
                    parameters=parameters,
                    settings=settings,
                )
            )

        return await self._session_pool.retry_operation_async(
            callee,
            retry_settings=self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    async def _execute_session_query(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()

        async def callee(
            session: ydb.aio.QuerySession,
        ) -> list[ydb.convert.ResultSet]:
            return await self._materialize(
                await session.transaction(self._tx_mode).execute(
                    query=query,
                    parameters=parameters,
                    commit_tx=True,
                    settings=settings,
                )
            )

        return await self._session_pool.retry_operation_async(
            callee,
            retry_settings=self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    async def _execute_transactional_query(
        self,
        tx_context: ydb.aio.QueryTxContext,
        query: str,
        parameters: ParametersType | None = None,
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()
        return await self._materialize(
            await tx_context.execute(
                query=query,
                parameters=parameters,
                commit_tx=False,
                settings=settings,
            )
        )

    async def execute_scheme(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()

        query = self._append_table_path_prefix(query)
        self._begin_query()

        result_list = await self._execute_generic_query(
            query=query, parameters=parameters
        )
        self._fill_buffer(result_list)
        self._finish_query()

    async def execute(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()
        if self._tx_context is not None:
            self._connection._raise_if_current_cursor_running()

        query = self._append_table_path_prefix(query)

        self._begin_query()

        if self._tx_context is not None:
            result_list = await self._execute_transactional_query(
                tx_context=self._tx_context, query=query, parameters=parameters
            )
        else:
            result_list = await self._execute_session_query(
                query=query, parameters=parameters
            )

        self._fill_buffer(result_list)
        self._finish_query()

    async def executemany(
        self, query: str, seq_of_parameters: Sequence[ParametersType]
    ) -> None:
        for parameters in seq_of_parameters:
            await self.execute(query, parameters)

    async def nextset(self) -> bool:
        return False

    def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

        self._state = CursorStatus.closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        self.close()


class AsyncStreamCursor(BaseCursor):
    def __init__(
        self,
        connection: AsyncConnection,
        session_pool: ydb.aio.QuerySessionPool,
        tx_mode: ydb.BaseQueryTxMode,
        request_settings: ydb.BaseRequestSettings,
        retry_settings: ydb.RetrySettings,
        tx_context: ydb.aio.QueryTxContext | None = None,
        table_path_prefix: str = "",
    ) -> None:
        super().__init__()
        self._connection = connection
        self._session_pool = session_pool
        self._tx_mode = tx_mode
        self._request_settings = request_settings
        self._retry_settings = retry_settings
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix
        self._stream: AsyncResponseContextIterator | None = None
        self._session_owner: ydb.aio.QuerySession | None = None

    async def _invalidate_active_session(self) -> None:
        self._clear_current_cursor()
        self._stream = None
        if self._session_owner is None:
            await self._connection._invalidate_session()
            return

        session = self._session_owner
        self._session_owner = None
        try:
            await session.delete()
        finally:
            await self._session_pool.release(session)

    def _clear_current_cursor(self) -> None:
        self._connection._clear_current_cursor(self)

    def _register_current_cursor(self) -> None:
        if self._tx_context is not None:
            self._connection._set_current_cursor(self)

    def _get_request_settings(self) -> ydb.BaseRequestSettings:
        settings = self._request_settings.make_copy()

        if self._request_settings.trace_id is None:
            settings = settings.with_trace_id(maybe_get_current_trace_id())

        return settings

    async def _release_owned_session(self) -> None:
        if self._session_owner is None:
            return

        session = self._session_owner
        self._session_owner = None
        await self._session_pool.release(session)

    async def _discard_owned_session(self) -> None:
        if self._session_owner is None:
            return

        session = self._session_owner
        self._session_owner = None
        try:
            await session.delete()
        finally:
            await self._session_pool.release(session)

    async def _finish_stream(self) -> None:
        self._stream = None
        await self._release_owned_session()
        self._clear_current_cursor()
        self._finish_query()

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    async def _load_next_result_set(self) -> bool:
        if self._stream is None:
            return False

        try:
            result_set = await self._stream.__anext__()
        except StopAsyncIteration:
            await self._finish_stream()
            return False

        self._update_result_set(result_set, replace_current=False)
        return True

    async def _prime_stream(self) -> None:
        if (
            not await self._load_next_result_set()
            and self._state == CursorStatus.running
        ):
            await self._finish_stream()

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    async def _execute_session_query_stream(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> AsyncResponseContextIterator:
        settings = self._get_request_settings()

        async def callee() -> AsyncResponseContextIterator:
            acquire_timeout = getattr(
                self._retry_settings,
                "max_session_acquire_timeout",
                None,
            )
            session = await self._session_pool.acquire(timeout=acquire_timeout)
            try:
                stream = await session.transaction(self._tx_mode).execute(
                    query=query,
                    parameters=parameters,
                    commit_tx=True,
                    settings=settings,
                )
            except Exception:
                await self._session_pool.release(session)
                raise

            self._session_owner = session
            return stream

        return await retry_operation_async(
            callee,
            self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    async def _execute_transactional_query_stream(
        self,
        tx_context: ydb.aio.QueryTxContext,
        query: str,
        parameters: ParametersType | None = None,
    ) -> AsyncResponseContextIterator:
        settings = self._get_request_settings()
        return await tx_context.execute(
            query=query,
            parameters=parameters,
            commit_tx=False,
            settings=settings,
        )

    async def execute(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()
        if self._tx_context is not None:
            self._connection._raise_if_current_cursor_running(self)

        query = self._append_table_path_prefix(query)
        self._begin_query()

        if self._tx_context is not None:
            self._stream = await self._execute_transactional_query_stream(
                tx_context=self._tx_context,
                query=query,
                parameters=parameters,
            )
            self._register_current_cursor()
        else:
            self._stream = await self._execute_session_query_stream(
                query=query,
                parameters=parameters,
            )

        await self._prime_stream()

    async def execute_scheme(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()

        query = self._append_table_path_prefix(query)
        self._begin_query()

        settings = self._get_request_settings()

        @handle_ydb_errors
        @invalidate_cursor_on_ydb_error
        async def execute_generic_query() -> list[ydb.convert.ResultSet]:
            async def callee(
                session: ydb.aio.QuerySession,
            ) -> list[ydb.convert.ResultSet]:
                stream = await session.execute(
                    query=query,
                    parameters=parameters,
                    settings=settings,
                )
                return [result_set async for result_set in stream]

            return await self._session_pool.retry_operation_async(
                callee,
                retry_settings=self._retry_settings,
            )

        result_list = await execute_generic_query()
        self._fill_buffer(result_list)
        self._finish_query()

    async def fetchone(self) -> tuple | None:
        self._raise_if_closed()

        while True:
            row = self._fetchone_from_buffer()
            if row is not None:
                return row
            if not await self._load_next_result_set():
                return None

    async def fetchmany(self, size: int | None = None) -> list:
        self._raise_if_closed()
        result: list[tuple] = []
        target_size = size or self.arraysize

        while len(result) < target_size:
            row = await self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    async def fetchall(self) -> list:
        self._raise_if_closed()
        result = list(self._fetchall_from_buffer())

        while await self._load_next_result_set():
            result.extend(self._fetchall_from_buffer())

        return result

    async def nextset(self) -> bool:
        return False

    async def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

        if self._state == CursorStatus.running:
            if self._session_owner is not None and self._stream is not None:
                self._stream.cancel()
                self._stream = None
                await self._discard_owned_session()
            elif self._stream is not None:
                async with self._stream:
                    pass
                await self._finish_stream()
            else:
                await self._release_owned_session()
                self._clear_current_cursor()

        self._stream = None
        self._session_owner = None
        self._clear_current_cursor()
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
