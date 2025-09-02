# LijnDing Pipeline Framework
# __init__.py for the main package

# Import key components to the top-level namespace for easier access
from .core.pipeline import Pipeline
from .core.stage import stage, aggregator_stage
from .core.context import Context
from .core.errors import ErrorPolicy
from .core.hooks import Hooks
from .backends.runner_registry import register_backend
from .components import (
    branch,
    batch,
    reduce,
    filter_,
    read_from_file,
    write_to_file,
    save_progress,
    http_request
)


__all__ = [
    # Core API
    "Pipeline",
    "stage",
    "aggregator_stage",
    "Context",
    "ErrorPolicy",
    "Hooks",

    # Components
    "branch",
    "batch",
    "reduce",
    "filter_",
    "read_from_file",
    "write_to_file",
    "save_progress",
    "http_request",

    # Extensibility
    "register_backend",
]

__version__ = "0.1.1"
