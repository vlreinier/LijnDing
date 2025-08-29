# LijnDing Pipeline Framework
# __init__.py for the main package

# Import key components to the top-level namespace for easier access
from .core.pipeline import Pipeline
from .core.stage import stage
from .core.context import Context
from .core.errors import ErrorPolicy
from .core.hooks import Hooks
from .components.branch import branch

__all__ = [
    "Pipeline",
    "stage",
    "Context",
    "ErrorPolicy",
    "Hooks",
    "Branch",
]

__version__ = "0.1.0"
