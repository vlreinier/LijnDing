from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Dict, Type

if TYPE_CHECKING:
    from .base import BaseRunner

# This registry now maps backend names to the fully qualified class names.
# The actual class will be imported lazily.
_runner_registry: Dict[str, str] = {
    "serial": "lijnding.backends.serial.SerialRunner",
    "thread": "lijnding.backends.threading.ThreadingRunner",
    "process": "lijnding.backends.processing.ProcessingRunner",
    "async": "lijnding.backends.asyncio.AsyncioRunner",
}

# A cache for instantiated runner classes to avoid repeated imports.
_runner_class_cache: Dict[str, Type[BaseRunner]] = {}


def register_backend(name: str, runner_class_path: str):
    """
    Registers a new backend runner by its fully qualified class path.

    This allows extending LijnDing with custom execution backends without
    requiring them to be imported at startup.

    Args:
        name: The name for the new backend (e.g., 'my_custom_runner').
        runner_class_path: The fully qualified path to the runner class
                           (e.g., 'my_package.runners.MyRunner').

    Raises:
        ValueError: If a backend with the same name is already registered.
    """
    if name in _runner_registry:
        raise ValueError(f"Backend '{name}' is already registered.")

    _runner_registry[name] = runner_class_path


class MissingBackendError(Exception):
    """Raised when a requested backend is not registered."""

    pass


def get_runner(name: str) -> "BaseRunner":
    """
    Retrieves a backend runner instance by name using lazy loading.

    This function acts as a factory for runner objects. It looks up the
    backend name in the registry, dynamically imports the module and gets
    the class, and then returns a new instance of the runner class.

    Args:
        name: The name of the backend (e.g., 'serial', 'thread').

    Returns:
        An instance of a BaseRunner subclass.

    Raises:
        MissingBackendError: If the requested name is not in the registry.
        ImportError: If the runner class cannot be imported.
    """
    # Check cache first
    if name in _runner_class_cache:
        return _runner_class_cache[name]()

    class_path = _runner_registry.get(name)
    if class_path is None:
        raise MissingBackendError(f"No backend registered with the name: '{name}'")

    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        runner_class = getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Could not import runner for backend '{name}': {e}") from e

    # Cache the class for future calls
    _runner_class_cache[name] = runner_class

    return runner_class()
