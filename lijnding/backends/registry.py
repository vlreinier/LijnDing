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
