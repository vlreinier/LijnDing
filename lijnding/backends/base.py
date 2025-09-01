from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Iterable, Iterator

from ..core.utils import ensure_iterable

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


class BaseRunner(ABC):
    """
    Abstract base class for all stage runners.

    A runner is responsible for executing the logic of a single Stage,
    handling the flow of data, and managing the execution environment
    (e.g., serial, threaded, or multiprocessing).
    """

    def run(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """
        Executes the stage. This is the main entry point for a runner.
        It delegates to the appropriate method based on the stage type.
        """
        if stage.stage_type == "source":
            # Source stages ignore the input iterable and generate their own data.
            return ensure_iterable(stage._invoke(context))
        if stage.stage_type == "aggregator":
            return self._run_aggregator(stage, context, iterable)

        # Default to itemwise processing
        return self._run_itemwise(stage, context, iterable)

    @abstractmethod
    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """
        Processes an iterable item by item.
        Each runner must implement this method.
        """
        raise NotImplementedError

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """
        Processes an entire iterable at once.
        This is a concrete implementation that most runners can use directly,
        as aggregation is typically a serial operation within the runner's context.
        """
        stage.metrics["items_in"] += 1  # The whole iterable is one item
        start_time = time.perf_counter()

        try:
            # The iterable itself is the single item for an aggregator
            results = stage._invoke(context, iterable)

            # Ensure the result is iterable for downstream stages
            output_stream = ensure_iterable(results)

            for res in output_stream:
                stage.metrics["items_out"] += 1
                yield res

        except Exception as e:
            stage.metrics["errors"] += 1
            # Here we might want to add hook calls or error policy handling
            # For now, we just re-raise.
            raise e
        finally:
            elapsed = time.perf_counter() - start_time
            stage.metrics["time_total"] += elapsed
