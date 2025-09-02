from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable, Iterator

from .base import BaseRunner, _handle_error_routing_async
from ..core.utils import ensure_iterable

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


class AsyncioRunner(BaseRunner):
    """
    A runner that executes stages using Python's asyncio for non-blocking I/O.
    """

    # This runner has a different signature for its entry point to support `async`.
    # The Pipeline will need to be aware of this.
    async def run_async(
        self, stage: "Stage", context: "Context", iterable: AsyncIterator[Any]
    ) -> AsyncIterator[Any]:
        """
        Asynchronously executes the stage.
        """
        stage.logger.info("Async stage run started.")
        start_time = asyncio.get_running_loop().time()

        try:
            if stage.stage_type == "source":
                # Source stages ignore the input iterable and generate their own data.
                results = stage._invoke(context)
                output_stream = ensure_iterable(results)
                if hasattr(output_stream, '__aiter__'):
                    async for res in output_stream:
                        yield res
                else:
                    for res in output_stream:
                        yield res
                return

            if stage.stage_type == "aggregator":
                # We need to collect all items from the async iterator first.
                items = [item async for item in iterable]
                # Then run the aggregator logic.
                if stage.is_async:
                    results = await stage._invoke(context, items)
                else:
                    results = await asyncio.to_thread(stage._invoke, context, items)

                output_stream = ensure_iterable(results)
                for res in output_stream:
                    yield res
            else:
                # Itemwise processing
                async for res in self._run_itemwise_async(stage, context, iterable):
                    yield res
        finally:
            end_time = asyncio.get_running_loop().time()
            total_time = end_time - start_time
            stage.logger.info(f"Async stage run finished in {total_time:.4f} seconds. Metrics: {stage.metrics}")

    async def _run_itemwise_async(
        self, stage: "Stage", context: "Context", iterable: AsyncIterator[Any]
    ) -> AsyncIterator[Any]:
        """
        Processes items concurrently using asyncio.
        This method contains the core logic for unwrapping nested components
        like Pipelines and Branches, which may return generators.
        """
        async for item in iterable:
            try:
                count = 0
                if stage.is_async:
                    # result_obj could be a coroutine or an async generator
                    result_obj = stage._invoke(context, item)

                    if inspect.isasyncgen(result_obj):
                        # The stage returned an async generator. We iterate through it.
                        async for res in result_obj:
                            yield res
                            count += 1
                    else:
                        # The stage returned a coroutine. We await it.
                        results = await result_obj

                        # The awaited result could be another generator.
                        if inspect.isasyncgen(results):
                            async for res in results:
                                yield res
                                count += 1
                        elif inspect.isgenerator(results):
                            for res in results:
                                yield res
                                count += 1
                        else:
                            # The result is a simple value or a sync iterable.
                            for res in ensure_iterable(results):
                                yield res
                                count += 1
                else:
                    # Run sync functions in a thread to avoid blocking the event loop
                    results = await asyncio.to_thread(stage._invoke, context, item)

                    # The result of the sync function could be a generator (e.g., nested sync pipeline)
                    if inspect.isgenerator(results):
                        for res in results:
                            yield res
                            count += 1
                    else:
                        for res in ensure_iterable(results):
                            yield res
                            count += 1

                stage.logger.debug(f"Successfully processed item, produced {count} output item(s).")

            except Exception as e:
                stage.logger.warning(f"Error processing item: {e}", exc_info=True)
                stage.metrics["errors"] += 1

                # TODO: Implement full error policies (e.g., retry) for async.
                policy = stage.error_policy
                if policy.mode == "route_to_stage":
                    await _handle_error_routing_async(stage, context, item)
                elif policy.mode == "skip":
                    # Do nothing, just move to the next item
                    pass
                else:
                    # Default 'fail' policy
                    raise e

    def run(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """
        Provides a synchronous entry point for the async runner by running
        a new event loop. This is useful for running an async-powered pipeline
        from a sync context, but it's not the primary use case.

        .. warning::
            This method is NOT lazy and will block until the entire async
            iterator is consumed. It also cannot be called from a running
            event loop. Use `run_async` for true async behavior.
        """
        # We need an async generator to pass to `asyncio.run`.
        async def _async_gen_wrapper():
            # Convert the sync iterable to an async one
            async def _to_async(it):
                for i in it:
                    yield i

            async for res in self.run_async(stage, context, _to_async(iterable)):
                yield res

        # This is a bridge from the async world to the sync world.
        # It collects all results and then returns them.
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If there's already a loop, we can't just call asyncio.run().
            # This is a complex problem. For now, we'll raise an error.
            raise RuntimeError(
                "Cannot run the asyncio backend from a running event loop with the sync `run` method. "
                "You must use `run_async` instead."
            )

        # This will block until the entire async generator is consumed.
        # This implementation is NOT lazy. A truly lazy bridge is more complex.
        results = asyncio.run(_collect_async_gen(_async_gen_wrapper()))
        yield from results

    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """Implementation for the abstract method, uses the sync bridge."""
        yield from self.run(stage, context, iterable)


async def _collect_async_gen(agen):
    return [item async for item in agen]
