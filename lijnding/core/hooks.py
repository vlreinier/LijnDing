from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional, Dict

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
        on_worker_init: Called once per worker before it starts processing items.
                        It can return a dictionary to be stored in `context.worker_state`.
        on_worker_exit: Called once per worker when it is about to terminate.
                        Useful for cleaning up resources created in `on_worker_init`.
        on_stream_end: For aggregator stages, called after the input stream is
                       exhausted, but before the main stage logic is called.
                       Ideal for accessing final context values.
    """
    before_stage: Optional[Callable[["Stage", "Context", Any], None]] = None
    after_stage: Optional[Callable[["Stage", "Context", Any, Any, float], None]] = None
    on_error: Optional[Callable[["Stage", "Context", Any, BaseException, int], None]] = None
    on_worker_init: Optional[Callable[["Context"], Dict[str, Any]]] = None
    on_worker_exit: Optional[Callable[["Context"], None]] = None
    on_stream_end: Optional[Callable[["Context"], None]] = None
