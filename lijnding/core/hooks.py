from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from .stage import Stage
    from .context import Context

@dataclass
class Hooks:
    """
    A collection of hook functions to monitor and trace pipeline execution.

    These hooks can be passed to a Stage to gain insights into its behavior,
    log metrics, or implement custom tracing.

    Attributes:
        before_stage: Called just before a stage processes an item.
        after_stage: Called just after a stage successfully processes an item.
        on_error: Called when a stage encounters an exception while processing.
    """
    before_stage: Optional[Callable[["Stage", "Context", Any], None]] = None
    after_stage: Optional[Callable[["Stage", "Context", Any, Any, float], None]] = None
    on_error: Optional[Callable[["Stage", "Context", Any, BaseException, int], None]] = None
