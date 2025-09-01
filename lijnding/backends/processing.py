from __future__ import annotations

import multiprocessing as mp
from typing import TYPE_CHECKING, Any, Iterable, Iterator

from .base import BaseRunner

if TYPE_CHECKING:
    from ..core.stage import Stage
    from ..core.context import Context

class ProcessingRunner(BaseRunner):
    """
    A runner that executes itemwise stages in separate processes.

    .. warning::
        This backend is currently unstable in some environments and has been
        disabled. It is left as an experimental feature for future development.
    """
    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        raise NotImplementedError("The 'process' backend is currently unstable and disabled.")

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        raise NotImplementedError("The 'process' backend is currently unstable and disabled.")
