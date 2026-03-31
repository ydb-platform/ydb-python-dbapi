# YDB Python DBAPI

## Introduction

Python DBAPI to `YDB`, which provides both sync and async drivers and complies with [PEP249](https://www.python.org/dev/peps/pep-0249/).

## Installation

```shell
pip install ydb-dbapi
```

## Usage

To establish a new DBAPI connection you should provide `host`, `port` and `database`:

```python
import ydb_dbapi

connection = ydb_dbapi.connect(
    host="localhost", port="2136", database="/local"
) # sync connection

async_connection = await ydb_dbapi.async_connect(
    host="localhost", port="2136", database="/local"
) # async connection
```

Usage of connection:

```python
with connection.cursor() as cursor:
    cursor.execute("SELECT id, val FROM table")

    row = cursor.fetchone()
    rows = cursor.fetchmany(size=5)
    rows = cursor.fetchall()
```

Usage of async connection:

```python
async with async_connection.cursor() as cursor:
    await cursor.execute("SELECT id, val FROM table")

    row = await cursor.fetchone()
    rows = await cursor.fetchmany(size=5)
    rows = await cursor.fetchall()
```

## Query parameters

### Standard mode (`pyformat=True`)

Pass `pyformat=True` to `connect()` to use familiar Python DB-API
parameter syntax. The driver will convert placeholders and infer YDB types
from Python values automatically.

**Named parameters** — `%(name)s` with a `dict`:

```python
connection = ydb_dbapi.connect(
    host="localhost", port="2136", database="/local",
    pyformat=True,
)

with connection.cursor() as cursor:
    cursor.execute(
        "SELECT * FROM users WHERE id = %(id)s AND active = %(active)s",
        {"id": 42, "active": True},
    )
```

**Positional parameters** — `%s` with a `list` or `tuple`:

```python
with connection.cursor() as cursor:
    cursor.execute(
        "INSERT INTO users (id, name, score) VALUES (%s, %s, %s)",
        [1, "Alice", 9.8],
    )
```

Use `%%` to insert a literal `%` character in the query.

**Automatic type mapping:**

| Python type        | YDB type    |
|--------------------|-------------|
| `bool`             | `Bool`      |
| `int`              | `Int64`     |
| `float`            | `Double`    |
| `str`              | `Utf8`      |
| `bytes`            | `String`    |
| `datetime.datetime`| `Timestamp` |
| `datetime.date`    | `Date`      |
| `datetime.timedelta`| `Interval` |
| `decimal.Decimal`  | `Decimal(22, 9)` |
| `None`             | `NULL` (passed as-is) |

**Explicit types with `ydb.TypedValue`:**

When automatic inference is not suitable (e.g. you need `Int32` instead of
`Int64`, or `Json`), wrap the value in `ydb.TypedValue` — it will be passed
through unchanged:

```python
import ydb

with connection.cursor() as cursor:
    cursor.execute(
        "INSERT INTO events (id, payload) VALUES (%(id)s, %(payload)s)",
        {
            "id": ydb.TypedValue(99, ydb.PrimitiveType.Int32),
            "payload": ydb.TypedValue('{"key": "value"}', ydb.PrimitiveType.Json),
        },
    )
```

### Native YDB mode (default, deprecated)

> **Deprecated.** Native YDB mode is the current default for backwards
> compatibility, but it will be removed in a future release. Migrate to
> `pyformat=True` at your earliest convenience.

By default (`pyformat=False`) the driver passes the query and parameters
directly to the YDB SDK without any transformation. Use `$name` placeholders
in the query and supply a `dict` with `$`-prefixed keys:

```python
connection = ydb_dbapi.connect(
    host="localhost", port="2136", database="/local",
)

with connection.cursor() as cursor:
    cursor.execute(
        "SELECT * FROM users WHERE id = $id",
        {"$id": ydb.TypedValue(42, ydb.PrimitiveType.Int64)},
    )
```
