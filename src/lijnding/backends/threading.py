from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Iterable, Iterator

from .base import BaseRunner

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


class ThreadingRunner(BaseRunner):
    """
    A runner that executes stages in a pool of threads.
    This is a simplified version for debugging purposes.
    """

    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """Processes items concurrently in a thread pool."""
        # Note: The original implementation had complex queue-based logic.
        # This version uses the simpler ThreadPoolExecutor to isolate the deadlock.
        with ThreadPoolExecutor(max_workers=stage.workers) as executor:
            # The `map` function is lazy, it returns an iterator that
            # yields results as they are completed.
            yield from executor.map(lambda item: stage._invoke(context, item), iterable)

    def run(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """Synchronous entry point for the threading runner."""
        if stage.stage_type == "aggregator":
            # The original implementation delegated this to the SerialRunner.
            # For this debug version, we'll just raise an error.
            raise NotImplementedError("The simplified ThreadingRunner does not support aggregators.")

        yield from self._run_itemwise(stage, context, iterable)
