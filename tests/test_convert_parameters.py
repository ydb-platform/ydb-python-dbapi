from __future__ import annotations

import datetime
import decimal

import pytest
import ydb
from ydb_dbapi.errors import ProgrammingError
from ydb_dbapi.utils import convert_query_parameters


class TestNamedStyle:
    """%(name)s placeholders with a dict."""

    def test_basic_query_transformation(self):
        q, _ = convert_query_parameters(
            "SELECT %(id)s FROM t WHERE name = %(name)s",
            {"id": 1, "name": "alice"},
        )
        assert q == "SELECT $id FROM t WHERE name = $name"

    def test_keys_prefixed_with_dollar(self):
        _, p = convert_query_parameters("SELECT %(x)s", {"x": 1})
        assert "$x" in p
        assert "x" not in p

    def test_int(self):
        _, p = convert_query_parameters("SELECT %(x)s", {"x": 42})
        assert p["$x"] == ydb.TypedValue(42, ydb.PrimitiveType.Int64)

    def test_float(self):
        _, p = convert_query_parameters("SELECT %(x)s", {"x": 3.14})
        assert p["$x"] == ydb.TypedValue(3.14, ydb.PrimitiveType.Double)

    def test_str(self):
        _, p = convert_query_parameters("SELECT %(x)s", {"x": "hello"})
        assert p["$x"] == ydb.TypedValue("hello", ydb.PrimitiveType.Utf8)

    def test_bytes(self):
        _, p = convert_query_parameters("SELECT %(x)s", {"x": b"data"})
        assert p["$x"] == ydb.TypedValue(b"data", ydb.PrimitiveType.String)

    def test_bool(self):
        _, p = convert_query_parameters("SELECT %(x)s", {"x": True})
        assert p["$x"] == ydb.TypedValue(True, ydb.PrimitiveType.Bool)

    def test_bool_not_confused_with_int(self):
        # bool is subclass of int — must map to Bool, not Int64
        _, p = convert_query_parameters("SELECT %(x)s", {"x": False})
        assert p["$x"].value_type == ydb.PrimitiveType.Bool

    def test_date(self):
        d = datetime.date(2024, 1, 15)
        _, p = convert_query_parameters("SELECT %(x)s", {"x": d})
        assert p["$x"] == ydb.TypedValue(d, ydb.PrimitiveType.Date)

    def test_datetime(self):
        tz = datetime.timezone.utc
        dt = datetime.datetime(2024, 1, 15, 12, 0, 0, tzinfo=tz)
        _, p = convert_query_parameters("SELECT %(x)s", {"x": dt})
        assert p["$x"] == ydb.TypedValue(dt, ydb.PrimitiveType.Timestamp)

    def test_datetime_not_confused_with_date(self):
        # datetime is subclass of date — must map to Timestamp, not Date
        tz = datetime.timezone.utc
        dt = datetime.datetime(2024, 6, 1, 0, 0, 0, tzinfo=tz)
        _, p = convert_query_parameters("SELECT %(x)s", {"x": dt})
        assert p["$x"].value_type == ydb.PrimitiveType.Timestamp

    def test_timedelta(self):
        td = datetime.timedelta(seconds=60)
        _, p = convert_query_parameters("SELECT %(x)s", {"x": td})
        assert p["$x"] == ydb.TypedValue(td, ydb.PrimitiveType.Interval)

    def test_decimal(self):
        d = decimal.Decimal("3.14")
        _, p = convert_query_parameters("SELECT %(x)s", {"x": d})
        assert isinstance(p["$x"], ydb.TypedValue)
        assert p["$x"].value == d

    def test_none_passed_as_is(self):
        _, p = convert_query_parameters("SELECT %(x)s", {"x": None})
        assert p["$x"] is None

    def test_unknown_type_raises_type_error(self):
        obj = object()
        with pytest.raises(TypeError, match="Could not infer YDB type"):
            convert_query_parameters("SELECT %(x)s", {"x": obj})

    def test_unknown_type_error_does_not_leak_value_repr(self):
        class SecretValue:
            def __repr__(self) -> str:
                return "secret-token"

        with pytest.raises(TypeError) as exc_info:
            convert_query_parameters("SELECT %(x)s", {"x": SecretValue()})

        assert "secret-token" not in str(exc_info.value)
        assert "SecretValue" in str(exc_info.value)

    def test_multiple_params(self):
        q, p = convert_query_parameters(
            "INSERT INTO t VALUES (%(a)s, %(b)s, %(c)s)",
            {"a": 1, "b": "hi", "c": True},
        )
        assert q == "INSERT INTO t VALUES ($a, $b, $c)"
        assert "$a" in p
        assert "$b" in p
        assert "$c" in p

    def test_percent_percent_escape(self):
        q, _ = convert_query_parameters(
            "SELECT %% as pct, %(x)s", {"x": 1}
        )
        assert q == "SELECT % as pct, $x"

    def test_empty_params(self):
        q, p = convert_query_parameters("SELECT 1", {})
        assert q == "SELECT 1"
        assert p == {}

    def test_named_requires_dict(self):
        with pytest.raises(
            ProgrammingError,
            match="require parameters to be a dict",
        ):
            convert_query_parameters("SELECT %(x)s", [1])

    def test_named_missing_key_raises(self):
        with pytest.raises(ProgrammingError, match="missing keys: 'y'"):
            convert_query_parameters("SELECT %(x)s, %(y)s", {"x": 1})

    def test_named_extra_key_raises(self):
        with pytest.raises(ProgrammingError, match="unexpected keys: 'y'"):
            convert_query_parameters("SELECT %(x)s", {"x": 1, "y": 2})

    def test_named_dollar_prefixed_key_raises(self):
        with pytest.raises(
            ProgrammingError,
            match="must not start with '\\$'",
        ):
            convert_query_parameters("SELECT %(x)s", {"$x": 1})

    def test_mixing_named_and_positional_raises(self):
        with pytest.raises(ProgrammingError, match="Mixing named"):
            convert_query_parameters("SELECT %(x)s, %s", {"x": 1})

    def test_invalid_percent_sequence_raises(self):
        with pytest.raises(
            ProgrammingError,
            match="Invalid pyformat placeholder",
        ):
            convert_query_parameters("SELECT %%%", [])


class TestCustomTypes:
    """Pass-through for ydb.TypedValue (explicit type hint)."""

    def test_typed_value_passed_through(self):
        tv = ydb.TypedValue(42, ydb.PrimitiveType.Int32)
        _, p = convert_query_parameters("SELECT %(x)s", {"x": tv})
        assert p["$x"] is tv

    def test_typed_value_not_double_wrapped(self):
        tv = ydb.TypedValue("hello", ydb.PrimitiveType.Utf8)
        _, p = convert_query_parameters("SELECT %(x)s", {"x": tv})
        assert isinstance(p["$x"], ydb.TypedValue)
        assert p["$x"].value_type == ydb.PrimitiveType.Utf8

    def test_typed_value_positional(self):
        tv = ydb.TypedValue(99, ydb.PrimitiveType.Int32)
        _, p = convert_query_parameters("SELECT %s", [tv])
        assert p["$p1"] is tv

    def test_unknown_type_raises_type_error(self):
        val = object()
        with pytest.raises(TypeError, match="Could not infer YDB type"):
            convert_query_parameters("SELECT %(x)s", {"x": val})


class TestPositionalStyle:
    """Positional %s placeholders with a list or tuple."""

    def test_basic_list(self):
        q, p = convert_query_parameters("SELECT %s", [42])
        assert q == "SELECT $p1"
        assert p["$p1"] == ydb.TypedValue(42, ydb.PrimitiveType.Int64)

    def test_basic_tuple(self):
        q, p = convert_query_parameters("SELECT %s", (42,))
        assert q == "SELECT $p1"
        assert p["$p1"] == ydb.TypedValue(42, ydb.PrimitiveType.Int64)

    def test_multiple_params_numbered_sequentially(self):
        q, p = convert_query_parameters(
            "INSERT INTO t VALUES (%s, %s, %s)", [1, "hi", 3.14]
        )
        assert q == "INSERT INTO t VALUES ($p1, $p2, $p3)"
        assert p["$p1"] == ydb.TypedValue(1, ydb.PrimitiveType.Int64)
        assert p["$p2"] == ydb.TypedValue("hi", ydb.PrimitiveType.Utf8)
        assert p["$p3"] == ydb.TypedValue(3.14, ydb.PrimitiveType.Double)

    def test_none_passed_as_is(self):
        _, p = convert_query_parameters("SELECT %s", [None])
        assert p["$p1"] is None

    def test_percent_percent_escape(self):
        q, p = convert_query_parameters("SELECT %%, %s", [7])
        assert q == "SELECT %, $p1"
        assert p["$p1"] == ydb.TypedValue(7, ydb.PrimitiveType.Int64)

    def test_empty_list(self):
        q, p = convert_query_parameters("SELECT 1", [])
        assert q == "SELECT 1"
        assert p == {}

    def test_positional_requires_sequence(self):
        with pytest.raises(
            ProgrammingError, match="require parameters to be a list or tuple"
        ):
            convert_query_parameters("SELECT %s", {"x": 1})

    def test_positional_count_mismatch_raises(self):
        with pytest.raises(ProgrammingError, match="expected 2, got 1"):
            convert_query_parameters("SELECT %s, %s", [1])

    def test_positional_extra_args_raises(self):
        with pytest.raises(ProgrammingError, match="expected 1, got 2"):
            convert_query_parameters("SELECT %s", [1, 2])
