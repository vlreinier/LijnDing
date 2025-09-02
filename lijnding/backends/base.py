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
        Processes an entire iterable at once with structured logging.
        This implementation fully consumes the input stream, logs metrics,
        and then calls the aggregator function.
        """
        stage.logger.info("aggregator_started")
        start_time = time.perf_counter()
        items_in = 0
        items_out = 0

        try:
            # Materialize the iterable to know the count and allow hooks.
            materialized_items = list(iterable)
            items_in = len(materialized_items)
            stage.metrics["items_in"] += items_in

            stage.logger.debug("aggregation_input_materialized", items_in=items_in)

            if stage.hooks and stage.hooks.on_stream_end:
                stage.hooks.on_stream_end(context)

            results = stage._invoke(context, materialized_items)
            output_stream = ensure_iterable(results)

            for res in output_stream:
                stage.metrics["items_out"] += 1
                items_out += 1
                yield res

        except Exception as e:
            stage.metrics["errors"] += 1
            stage.logger.error("aggregator_error", error=str(e))
            raise e
        finally:
            duration = time.perf_counter() - start_time
            stage.metrics["time_total"] += duration
            stage.logger.info(
                "aggregator_finished",
                items_in=items_in,
                items_out=items_out,
                errors=stage.metrics["errors"],
                duration=round(duration, 4),
            )


def _handle_error_routing(stage: "Stage", context: "Context", item: Any):
    """Helper function to route a failed item to a dead-letter stage."""
    # Local imports to avoid circular dependencies
    from .runner_registry import get_runner
    from ..core.pipeline import Pipeline

    if not stage.error_policy.route_to:
        return

    dead_letter_stage = stage.error_policy.route_to
    if isinstance(dead_letter_stage, Pipeline):
        dead_letter_stage = dead_letter_stage.to_stage()

    runner = get_runner(dead_letter_stage.backend)
    dead_letter_stream = runner.run(dead_letter_stage, context, [item])

    # Consume the stream to ensure it executes
    for _ in dead_letter_stream:
        pass


async def _handle_error_routing_async(stage: "Stage", context: "Context", item: Any):
    """Async helper function to route a failed item to a dead-letter stage."""
    # Local imports to avoid circular dependencies
    import asyncio
    from .runner_registry import get_runner
    from ..core.pipeline import Pipeline

    if not stage.error_policy.route_to:
        return

    dead_letter_stage = stage.error_policy.route_to
    if isinstance(dead_letter_stage, Pipeline):
        dead_letter_stage = dead_letter_stage.to_stage()

    runner = get_runner(dead_letter_stage.backend)

    # Helper to convert sync iterable to async
    async def _to_async(it):
        for i in it:
            yield i

    if hasattr(runner, 'run_async'):
        stream = await runner.run_async(dead_letter_stage, context, _to_async([item]))
        async for _ in stream:
            pass
    else:
        # For a sync dead-letter stage, we can run its .collect() method in a
        # thread to ensure it executes without blocking the event loop.
        await asyncio.to_thread(dead_letter_stage.collect, [item], context=context)
