from __future__ import annotations
import pytest
from python_sf.snowflake_session import SnowflakeSession
import snowflake.snowpark as sp


class DummySession:
    """Dummy session class for testing purposes."""

    def __init__(self):
        self.closed = False

    def close(self) -> None:
        """Simulate closing the session."""
        self.closed = True


class DummyBuilder:
    """Dummy builder to simulate sp.Session.builder."""

    def __init__(self):
        self.configs_value = None

    def configs(self, configs: dict) -> DummyBuilder:
        self.configs_value = configs
        return self

    def create(self) -> DummySession:
        return DummySession()


def test_snowflake_session_context_manager(monkeypatch):
    """Test that the SnowflakeSession context manager creates a session and closes it after exiting."""
    dummy_session_instance = DummySession()

    class DummyBuilderCapture:
        def __init__(self):
            self.configs_value = None

        def configs(self, configs: dict) -> DummyBuilderCapture:
            self.configs_value = configs
            return self

        def create(self) -> DummySession:
            return dummy_session_instance

    dummy_builder = DummyBuilderCapture()
    monkeypatch.setattr(sp.Session, "builder", dummy_builder)

    sfs = SnowflakeSession()
    with sfs as session:
        assert session is dummy_session_instance, (
            f"Expected: {dummy_session_instance}, Actual: {session}"
        )
        assert not session.closed, f"Expected: False, Actual: {session.closed}"
    
    assert dummy_session_instance.closed, (
        f"Expected dummy_session_instance to be closed, but dummy_session_instance.closed is {dummy_session_instance.closed}"
    )
    assert dummy_builder.configs_value is not None, (
        f"Expected dummy_builder.configs_value to be set, but dummy_builder.configs_value is {dummy_builder.configs_value}"
    )
    assert dummy_builder.configs_value == sfs.config.to_dict(), (
        f"Expected: {sfs.config.to_dict()}, Actual: {dummy_builder.configs_value}"
    )


def test_snowflake_session_default_config(monkeypatch):
    """Test that the SnowflakeSession uses a default SnowflakeConnectionConfig if none is provided."""
    dummy_session_instance = DummySession()

    class DummyBuilderDefault:
        """Dummy builder to simulate sp.Session.builder."""

        def __init__(self):
            self.configs_value = None

        def configs(self, configs: dict) -> DummyBuilderDefault:
            """Ensure that the configs contain the expected keys."""
            expected_keys = {
                "account",
                "user",
                "role",
                "warehouse",
                "database",
                "schema",
                "password",
            }
            assert expected_keys.issubset(set(configs.keys())), (
                f"Expected keys: {expected_keys}, Actual keys: {configs.keys()}"
            )
            self.configs_value = configs
            return self

        def create(self) -> DummySession:
            """Return the dummy session instance."""
            return dummy_session_instance

    dummy_builder_default = DummyBuilderDefault()
    monkeypatch.setattr(sp.Session, "builder", dummy_builder_default)

    sfs = SnowflakeSession()
    with sfs as session:
        assert session is dummy_session_instance
