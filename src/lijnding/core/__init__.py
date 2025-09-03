# lijnding.core
# This package contains the core classes of the LijnDing framework,
# such as Pipeline, Stage, and Context.

from .pipeline import Pipeline
from .stage import stage, Stage, aggregator_stage
from .context import Context
from .errors import ErrorPolicy
from .hooks import Hooks

__all__ = [
    "Pipeline",
    "stage",
    "aggregator_stage",
    "Stage",
    "Context",
    "ErrorPolicy",
    "Hooks",
]
