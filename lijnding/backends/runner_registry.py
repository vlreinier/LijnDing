from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Type

if TYPE_CHECKING:
    from .base import BaseRunner

# Import runner classes
from .serial import SerialRunner
from .threading import ThreadingRunner
from .processing import ProcessingRunner
from .asyncio import AsyncioRunner

# A registry to hold the mapping from backend names to runner classes
_runner_registry: Dict[str, Type[BaseRunner]] = {
    "serial": SerialRunner,
    "thread": ThreadingRunner,
    "process": ProcessingRunner,
    "async": AsyncioRunner,
}


def register_backend(name: str, runner_class: Type[BaseRunner]):
    """
    Registers a new backend runner.

    This allows extending LijnDing with custom execution backends. The provided
    class must be a subclass of `BaseRunner`.

    Args:
        name: The name for the new backend (e.g., 'my_custom_runner').
        runner_class: The runner class to associate with the name.

    Raises:
        TypeError: If the provided class is not a subclass of BaseRunner.
        ValueError: If a backend with the same name is already registered.
    """
    from .base import BaseRunner
    if not issubclass(runner_class, BaseRunner):
        raise TypeError("The provided runner must be a subclass of BaseRunner.")

    if name in _runner_registry:
        # For now, we don't allow overwriting built-in backends.
        # This could be changed to include a `force=True` parameter if needed.
        raise ValueError(f"Backend '{name}' is already registered.")

    _runner_registry[name] = runner_class


class MissingBackendError(Exception):
    """Raised when a requested backend is not registered."""
    pass


def get_runner(name: str) -> "BaseRunner":
    """
    Retrieves a backend runner instance by name.

    This function acts as a factory for runner objects. It looks up the
    backend name in the registry and returns a new instance of the
    corresponding runner class.

    Args:
        name: The name of the backend (e.g., 'serial', 'thread').

    Returns:
        An instance of a BaseRunner subclass.

    Raises:
        MissingBackendError: If the requested name is not in the registry.
    """
    runner_class = _runner_registry.get(name)
    if runner_class is None:
        raise MissingBackendError(f"No backend registered with the name: '{name}'")

    return runner_class()
