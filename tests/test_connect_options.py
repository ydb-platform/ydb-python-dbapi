from __future__ import annotations

import pytest
import ydb_dbapi as dbapi
from ydb_dbapi.errors import ProgrammingError
from ydb_dbapi.utils import prepare_driver_config_kwargs


class TestPrepareDriverConfigKwargs:
    """Leftover connect() keywords are routed, not dropped."""

    def test_known_driver_option_is_routed(self):
        assert prepare_driver_config_kwargs(
            None, {"disable_discovery": True}
        ) == {"disable_discovery": True}

    def test_explicit_driver_config_kwargs_are_kept(self):
        result = prepare_driver_config_kwargs(
            {"grpc_keep_alive_timeout": 777}, {"disable_discovery": True}
        )
        assert result == {
            "grpc_keep_alive_timeout": 777,
            "disable_discovery": True,
        }

    def test_no_leftovers_returns_driver_config_kwargs(self):
        assert prepare_driver_config_kwargs({"use_all_nodes": False}, {}) == {
            "use_all_nodes": False
        }

    def test_unknown_option_raises(self):
        with pytest.raises(ProgrammingError, match="disable_discovry"):
            prepare_driver_config_kwargs(None, {"disable_discovry": True})

    def test_unknown_option_lists_supported_ones(self):
        with pytest.raises(ProgrammingError, match="disable_discovery"):
            prepare_driver_config_kwargs(None, {"nonsense": 1})

    def test_all_unknown_options_are_reported(self):
        with pytest.raises(ProgrammingError, match="first, second"):
            prepare_driver_config_kwargs(None, {"second": 1, "first": 2})

    def test_option_reserved_by_the_connection_is_unknown(self):
        # The connection computes these itself, so they must not be
        # overridable through connect() keywords.
        with pytest.raises(ProgrammingError, match="endpoint"):
            prepare_driver_config_kwargs(None, {"endpoint": "grpc://host:1"})

    def test_option_passed_twice_raises(self):
        with pytest.raises(ProgrammingError, match="disable_discovery"):
            prepare_driver_config_kwargs(
                {"disable_discovery": True}, {"disable_discovery": False}
            )


class TestDriverOptionCoercion:
    """URL query parameters arrive as strings and must not stay strings."""

    @pytest.mark.parametrize("value", ["true", "True", "yes", "on", "1"])
    def test_truthy_strings(self, value: str):
        result = prepare_driver_config_kwargs(
            None, {"disable_discovery": value}
        )
        assert result["disable_discovery"] is True

    @pytest.mark.parametrize("value", ["false", "False", "no", "off", "0"])
    def test_falsy_strings(self, value: str):
        # The whole point of coercion: "false" is a non-empty string and
        # would otherwise disable discovery.
        result = prepare_driver_config_kwargs(
            None, {"disable_discovery": value}
        )
        assert result["disable_discovery"] is False

    def test_invalid_boolean_raises(self):
        with pytest.raises(ProgrammingError, match="expects a boolean"):
            prepare_driver_config_kwargs(None, {"disable_discovery": "maybe"})

    def test_integer_string(self):
        result = prepare_driver_config_kwargs(
            None, {"discovery_request_timeout": "42"}
        )
        assert result["discovery_request_timeout"] == 42

    def test_optional_integer_string(self):
        result = prepare_driver_config_kwargs(
            None, {"grpc_keep_alive_timeout": "777"}
        )
        assert result["grpc_keep_alive_timeout"] == 777

    def test_invalid_integer_raises(self):
        with pytest.raises(ProgrammingError, match="expects an integer"):
            prepare_driver_config_kwargs(
                None, {"discovery_request_timeout": "soon"}
            )

    def test_string_option_is_left_alone(self):
        result = prepare_driver_config_kwargs(
            None, {"grpc_lb_policy_name": "pick_first"}
        )
        assert result["grpc_lb_policy_name"] == "pick_first"

    def test_non_string_values_are_passed_through(self):
        result = prepare_driver_config_kwargs(
            None, {"discovery_request_timeout": 42}
        )
        assert result["discovery_request_timeout"] == 42

    def test_option_not_expressible_as_string_raises(self):
        with pytest.raises(ProgrammingError, match="driver_config_kwargs"):
            prepare_driver_config_kwargs(None, {"channel_options": "a=b"})


class TestConnectRejectsUnusableOptions:
    """connect() reports bad options before touching the network."""

    def test_unknown_keyword(self, connection_kwargs: dict):
        with pytest.raises(ProgrammingError, match="disable_discovry"):
            dbapi.connect(**connection_kwargs, disable_discovry=True)

    def test_auth_token_together_with_credentials(
        self, connection_kwargs: dict
    ):
        # auth_token silently wins inside DriverConfig, so refuse the
        # combination instead of dropping the credentials.
        with pytest.raises(ProgrammingError, match="auth_token"):
            dbapi.connect(
                **connection_kwargs,
                credentials={"token": "some-token"},
                auth_token="another-token",
            )

    def test_driver_option_with_shared_session_pool(
        self, connection_kwargs: dict
    ):
        with pytest.raises(ProgrammingError, match="ydb_session_pool"):
            dbapi.connect(
                **connection_kwargs,
                ydb_session_pool=object(),
                disable_discovery=True,
            )

    def test_driver_config_kwargs_with_shared_session_pool(
        self, connection_kwargs: dict
    ):
        with pytest.raises(ProgrammingError, match="ydb_session_pool"):
            dbapi.connect(
                **connection_kwargs,
                ydb_session_pool=object(),
                driver_config_kwargs={"disable_discovery": True},
            )

    def test_additional_sdk_headers_are_still_accepted(
        self, connection_kwargs: dict
    ):
        # Private channel used by ydb-sqlalchemy: it must never be
        # treated as a driver option.
        conn = dbapi.Connection(
            **connection_kwargs,
            _additional_sdk_headers=("ydb-sqlalchemy/0.0.0",),
        )
        try:
            headers = conn._driver._driver_config._additional_sdk_headers
            assert "ydb-sqlalchemy/0.0.0" in headers
        finally:
            conn.close()

    def test_additional_sdk_headers_with_shared_session_pool(
        self, connection_kwargs: dict
    ):
        # It is not a driver option, so it must not trip the shared pool
        # check either.
        with pytest.raises(AttributeError):
            dbapi.Connection(
                **connection_kwargs,
                ydb_session_pool=object(),
                _additional_sdk_headers=("ydb-sqlalchemy/0.0.0",),
            )
