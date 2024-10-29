import functools
from enum import Enum
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


def handle_ydb_errors_async(func: Callable) -> Callable:
    @functools.wraps(func)
    async def wrapper(*args: tuple, **kwargs: dict) -> Any:
        try:
            return await func(*args, **kwargs)
        except (ydb.issues.AlreadyExists, ydb.issues.PreconditionFailed) as e:
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


def handle_ydb_errors(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args: tuple, **kwargs: dict) -> Any:
        try:
            return func(*args, **kwargs)
        except (ydb.issues.AlreadyExists, ydb.issues.PreconditionFailed) as e:
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
