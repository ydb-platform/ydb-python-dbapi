import functools
import ydb

from .errors import (
    DatabaseError,
    DataError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)


def handle_ydb_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
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
