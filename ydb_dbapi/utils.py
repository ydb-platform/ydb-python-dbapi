from __future__ import annotations

import datetime
import decimal
import functools
import importlib.util
import json
import re
from enum import Enum
from inspect import iscoroutinefunction
from typing import Any
from typing import Callable

import ydb

from .errors import DatabaseError
from .errors import DataError
from .errors import IntegrityError
from .errors import InternalError
from .errors import NotSupportedError
from .errors import OperationalError
from .errors import ProgrammingError


def handle_ydb_errors(func: Callable) -> Callable:  # noqa: C901
    if iscoroutinefunction(func):

        @functools.wraps(func)
        async def awrapper(*args: tuple, **kwargs: dict) -> Any:
            try:
                return await func(*args, **kwargs)
            except (
                ydb.issues.AlreadyExists,
                ydb.issues.PreconditionFailed,
            ) as e:
                raise IntegrityError(e.message, original_error=e) from e
            except (ydb.issues.Unsupported, ydb.issues.Unimplemented) as e:
                raise NotSupportedError(e.message, original_error=e) from e
            except (ydb.issues.BadRequest, ydb.issues.SchemeError) as e:
                raise ProgrammingError(e.message, original_error=e) from e
            except (
                ydb.issues.TruncatedResponseError,
                ydb.issues.ConnectionError,
                ydb.issues.Aborted,
                ydb.issues.Unavailable,
                ydb.issues.Overloaded,
                ydb.issues.Undetermined,
                ydb.issues.Timeout,
                ydb.issues.Cancelled,
                ydb.issues.SessionBusy,
                ydb.issues.SessionExpired,
                ydb.issues.SessionPoolEmpty,
                ydb.issues.DeadlineExceed,
            ) as e:
                raise OperationalError(e.message, original_error=e) from e
            except ydb.issues.GenericError as e:
                raise DataError(e.message, original_error=e) from e
            except ydb.issues.InternalError as e:
                raise InternalError(e.message, original_error=e) from e
            except ydb.Error as e:
                raise DatabaseError(e.message, original_error=e) from e
            except Exception as e:
                raise DatabaseError("Failed to execute query") from e

        return awrapper

    @functools.wraps(func)
    def wrapper(*args: tuple, **kwargs: dict) -> Any:
        try:
            return func(*args, **kwargs)
        except (
            ydb.issues.AlreadyExists,
            ydb.issues.PreconditionFailed,
        ) as e:
            raise IntegrityError(e.message, original_error=e) from e
        except (ydb.issues.Unsupported, ydb.issues.Unimplemented) as e:
            raise NotSupportedError(e.message, original_error=e) from e
        except (ydb.issues.BadRequest, ydb.issues.SchemeError) as e:
            raise ProgrammingError(e.message, original_error=e) from e
        except (
            ydb.issues.TruncatedResponseError,
            ydb.issues.ConnectionError,
            ydb.issues.Aborted,
            ydb.issues.Unavailable,
            ydb.issues.Overloaded,
            ydb.issues.Undetermined,
            ydb.issues.Timeout,
            ydb.issues.Cancelled,
            ydb.issues.SessionBusy,
            ydb.issues.SessionExpired,
            ydb.issues.SessionPoolEmpty,
            ydb.issues.DeadlineExceed,
        ) as e:
            raise OperationalError(e.message, original_error=e) from e
        except ydb.issues.GenericError as e:
            raise DataError(e.message, original_error=e) from e
        except ydb.issues.InternalError as e:
            raise InternalError(e.message, original_error=e) from e
        except ydb.Error as e:
            raise DatabaseError(e.message, original_error=e) from e
        except Exception as e:
            raise DatabaseError("Failed to execute query") from e

    return wrapper


class CursorStatus(str, Enum):
    ready = "ready"
    running = "running"
    finished = "finished"
    closed = "closed"


def maybe_get_current_trace_id() -> str | None:
    # Check if OpenTelemetry is available
    if importlib.util.find_spec("opentelemetry"):
        from opentelemetry import trace  # type: ignore

        current_span = trace.get_current_span()

        if current_span.get_span_context().is_valid:
            return format(current_span.get_span_context().trace_id, "032x")

    # Return None if OpenTelemetry is not available or trace ID is invalid
    return None


def prepare_credentials(
    credentials: ydb.Credentials | dict | str | None,
) -> ydb.Credentials | None:
    if not credentials:
        return None

    if isinstance(credentials, ydb.Credentials):
        return credentials

    if isinstance(credentials, str):
        credentials = json.loads(credentials)

    if isinstance(credentials, dict):
        credentials = credentials or {}

        username = credentials.get("username")
        if username:
            password = credentials.get("password")
            return ydb.StaticCredentials.from_user_password(
                username,
                password,
            )

        token = credentials.get("token")
        if token:
            return ydb.AccessTokenCredentials(token)

        service_account_json = credentials.get("service_account_json")
        if service_account_json:
            return ydb.iam.ServiceAccountCredentials.from_content(
                json.dumps(service_account_json),
            )

    return ydb.AnonymousCredentials()


# Order matters: bool before int, datetime before date (subclass checks).
_PYTHON_TO_YDB_TYPE: list[tuple[type, Any]] = [
    (bool, ydb.PrimitiveType.Bool),
    (int, ydb.PrimitiveType.Int64),
    (float, ydb.PrimitiveType.Double),
    (str, ydb.PrimitiveType.Utf8),
    (bytes, ydb.PrimitiveType.String),
    (datetime.datetime, ydb.PrimitiveType.Timestamp),
    (datetime.date, ydb.PrimitiveType.Date),
    (datetime.timedelta, ydb.PrimitiveType.Interval),
    (decimal.Decimal, ydb.DecimalType(22, 9)),
]
_PYFORMAT_TOKEN_RE = re.compile(r"%%|%\((\w+)\)s|%s|%")


def _infer_ydb_type(value: Any) -> Any:
    """Infer a YDB type from a Python value."""
    for python_type, ydb_type in _PYTHON_TO_YDB_TYPE:
        if isinstance(value, python_type):
            return ydb_type
    return None


def _wrap_value(value: Any) -> Any:
    """Wrap a Python value in ydb.TypedValue if a type can be inferred.

    ``ydb.TypedValue`` instances are returned as-is so callers can supply
    an explicit type for values whose type cannot be inferred automatically.
    """
    if isinstance(value, ydb.TypedValue):
        return value
    if value is None:
        return value
    ydb_type = _infer_ydb_type(value)
    if ydb_type is not None:
        return ydb.TypedValue(value, ydb_type)
    message = (
        "Could not infer YDB type for parameter of type "
        f"{type(value).__name__}. "
        "Wrap it in ydb.TypedValue to pass an explicit type."
    )
    raise TypeError(message)


def _extract_pyformat_placeholders(query: str) -> tuple[list[str], int]:
    named_placeholders: list[str] = []
    positional_count = 0

    for match in _PYFORMAT_TOKEN_RE.finditer(query):
        placeholder = match.group(0)
        if placeholder == "%":
            raise ProgrammingError(
                "Invalid pyformat placeholder in query. "
                "Use %% for a literal percent, %(name)s for named parameters, "
                "or %s for positional parameters."
            )
        if placeholder == "%%":
            continue
        if placeholder == "%s":
            positional_count += 1
            continue
        named_placeholders.append(match.group(1))

    if named_placeholders and positional_count:
        raise ProgrammingError(
            "Mixing named (%(name)s) and positional (%s) placeholders "
            "is not supported."
        )

    return named_placeholders, positional_count


def _convert_pyformat_query(query: str) -> str:
    positional_index = 0

    def replace(match: re.Match) -> str:
        nonlocal positional_index
        placeholder = match.group(0)
        if placeholder == "%%":
            return "%"
        if placeholder == "%s":
            positional_index += 1
            return f"$p{positional_index}"
        return f"${match.group(1)}"

    return _PYFORMAT_TOKEN_RE.sub(replace, query)


def _validate_positional_parameters(
    parameters: dict | list | tuple,
    positional_count: int,
) -> None:
    if not isinstance(parameters, (list, tuple)):
        raise ProgrammingError(
            "Positional placeholders (%s) require parameters "
            "to be a list or tuple."
        )
    if len(parameters) == positional_count:
        return

    message = (
        "Positional placeholder count does not match the number "
        f"of parameters: expected {positional_count}, got {len(parameters)}."
    )
    raise ProgrammingError(message)


def _validate_named_parameters(
    parameters: dict | list | tuple,
    named_placeholders: list[str],
) -> dict:
    if not isinstance(parameters, dict):
        raise ProgrammingError(
            "Named placeholders (%(name)s) require parameters "
            "to be a dict."
        )

    expected_keys = set(named_placeholders)
    actual_keys = set(parameters)
    invalid_prefixed_keys = [
        name for name in actual_keys
        if isinstance(name, str) and name.startswith("$")
    ]
    if invalid_prefixed_keys:
        invalid_key = invalid_prefixed_keys[0]
        message = (
            "Mapping parameter names must not start with '$' when using "
            "pyformat style; "
            f"got key {invalid_key!r}. Use bare names instead."
        )
        raise ProgrammingError(message)

    missing_keys = expected_keys - actual_keys
    extra_keys = actual_keys - expected_keys
    if not (missing_keys or extra_keys):
        return parameters

    details: list[str] = []
    if missing_keys:
        details.append(
            "missing keys: "
            + ", ".join(sorted(repr(key) for key in missing_keys))
        )
    if extra_keys:
        details.append(
            "unexpected keys: "
            + ", ".join(sorted(repr(key) for key in extra_keys))
        )
    raise ProgrammingError(
        "Named placeholders do not match the provided parameters: "
        + "; ".join(details)
        + "."
    )


def _validate_pyformat_parameters(
    parameters: dict | list | tuple,
    named_placeholders: list[str],
    positional_count: int,
) -> dict | list | tuple:
    if positional_count:
        _validate_positional_parameters(parameters, positional_count)
        return parameters
    if named_placeholders:
        return _validate_named_parameters(parameters, named_placeholders)
    if parameters:
        raise ProgrammingError(
            "Query does not contain pyformat placeholders, "
            "but parameters were provided."
        )
    return parameters


def convert_query_parameters(
    query: str,
    parameters: dict | list | tuple,
) -> tuple[str, dict]:
    """Convert pyformat-style query and parameters to YDB format.

    Supports two parameter styles:

    Named (``%(name)s``) with a mapping::

        convert_query_parameters(
            "SELECT %(id)s", {"id": 42}
        )
        # -> ("SELECT $id", {"$id": TypedValue(42, Int64)})

    Positional (``%s``) with a sequence::

        convert_query_parameters(
            "SELECT %s, %s", [42, "hi"]
        )
        # -> ("SELECT $p1, $p2", {"$p1": TypedValue(42, Int64),
        #                         "$p2": TypedValue("hi", Utf8)})

    ``%%`` is converted to a literal ``%`` in both modes.

    Python-to-YDB type mapping:
        bool        -> Bool
        int         -> Int64
        float       -> Double
        str         -> Utf8
        bytes       -> String
        datetime    -> Timestamp
        date        -> Date
        timedelta   -> Interval
        Decimal     -> Decimal(22, 9)
        None        -> passed as-is (NULL)
    """
    named_placeholders, positional_count = _extract_pyformat_placeholders(
        query
    )
    parameters = _validate_pyformat_parameters(
        parameters, named_placeholders, positional_count
    )
    converted_query = _convert_pyformat_query(query)

    converted_params: dict = {}

    if isinstance(parameters, (list, tuple)):
        for i, value in enumerate(parameters, start=1):
            converted_params[f"$p{i}"] = _wrap_value(value)
    else:
        for name, value in parameters.items():
            converted_params[f"${name}"] = _wrap_value(value)

    return converted_query, converted_params
