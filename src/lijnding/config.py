"""
Configuration loading and management.

This module provides tools for loading YAML configuration files and accessing
their contents in a structured way.
"""

from typing import Any, Dict, Optional
import yaml
import os


class Config:
    """
    A wrapper around a dictionary for managing configuration.

    It provides a `get` method that allows accessing nested values using
    dot-notation (e.g., 'database.host').
    """

    def __init__(self, config_data: Dict[str, Any]):
        self._config = config_data or {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        Access a config value using dot notation.

        Example:
            >>> config = Config({'a': {'b': 1}})
            >>> config.get('a.b')
            1
            >>> config.get('a.c', 'default_value')
            'default_value'

        :param key: The dot-separated key for the desired value.
        :param default: The value to return if the key is not found.
        :return: The configuration value or the default.
        """
        keys = key.split(".")
        value = self._config
        for k in keys:
            if not isinstance(value, dict) or k not in value:
                return default
            value = value[k]
        return value

    def __repr__(self) -> str:
        return f"Config(config_data={self._config})"


def load_config(path: Optional[str]) -> Config:
    """
    Loads a YAML configuration file from the given path.

    If the path is None or does not exist, it returns an empty Config object.

    :param path: The path to the YAML configuration file.
    :return: A Config object with the loaded data.
    """
    if not path or not os.path.exists(path):
        return Config({})

    with open(path, "r") as f:
        # Use safe_load to avoid arbitrary code execution
        config_data = yaml.safe_load(f)

    return Config(config_data)
