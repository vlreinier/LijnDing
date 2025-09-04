import pytest
import yaml

from lijnding.config import Config, load_config


# --- Tests for the Config class ---


def test_config_get_top_level():
    """Tests retrieving a simple top-level value."""
    config = Config({"a": 1, "b": "hello"})
    assert config.get("a") == 1
    assert config.get("b") == "hello"


def test_config_get_nested():
    """Tests retrieving a nested value using dot notation."""
    config = Config({"a": {"b": {"c": 3}}})
    assert config.get("a.b.c") == 3


def test_config_get_missing_key_returns_default_none():
    """Tests that a missing key returns None by default."""
    config = Config({"a": 1})
    assert config.get("b") is None


def test_config_get_missing_key_returns_custom_default():
    """Tests that a missing key returns the specified default value."""
    config = Config({"a": 1})
    assert config.get("b", "default_value") == "default_value"


def test_config_get_partially_correct_nested_key():
    """Tests that a partially correct nested key returns the default."""
    config = Config({"a": {"b": 1}})
    assert config.get("a.c", "default") == "default"


def test_config_get_from_empty_config():
    """Tests that getting any key from an empty config returns the default."""
    config = Config({})
    assert config.get("a.b.c") is None
    assert config.get("a", "default") == "default"


# --- Tests for the load_config function ---


def test_load_config_with_none_path():
    """Tests that passing None as a path returns an empty Config object."""
    config = load_config(None)
    assert isinstance(config, Config)
    assert config.get("any.key", "default") == "default"


def test_load_config_with_non_existent_path():
    """Tests that a non-existent path returns an empty Config object."""
    config = load_config("path/that/does/not/exist.yml")
    assert isinstance(config, Config)
    assert config.get("any.key", "default") == "default"


def test_load_config_from_valid_file(tmp_path):
    """Tests loading a valid YAML file."""
    content = """
    app:
      host: "localhost"
      port: 8000
    database:
      user: "admin"
    """
    config_file = tmp_path / "config.yml"
    config_file.write_text(content)

    config = load_config(str(config_file))
    assert config.get("app.host") == "localhost"
    assert config.get("database.user") == "admin"
    assert config.get("app.port") == 8000


def test_load_config_from_empty_file(tmp_path):
    """Tests that loading an empty file results in an empty Config."""
    config_file = tmp_path / "config.yml"
    config_file.write_text("")

    config = load_config(str(config_file))
    assert config.get("any.key", "default") == "default"


def test_load_config_raises_error_for_malformed_file(tmp_path):
    """Tests that a malformed YAML file raises a YAMLError."""
    # This YAML is invalid because a mapping value is on the same line as the key
    # without proper syntax.
    invalid_content = "key: value: oops"
    config_file = tmp_path / "config.yml"
    config_file.write_text(invalid_content)

    with pytest.raises(yaml.YAMLError):
        load_config(str(config_file))
