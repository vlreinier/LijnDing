from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable, Iterator

from .base import BaseRunner

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
        if stage.stage_type == "aggregator":
            # We need to collect all items from the async iterator first.
            items = [item async for item in iterable]
            # Then run the aggregator logic.
            if stage.is_async:
                results = await stage._invoke(context, items)
            else:
                results = await asyncio.to_thread(stage._invoke, context, items)

            output_stream = stage._ensure_iterable(results)
            for res in output_stream:
                yield res
        else:
            # Itemwise processing
            async for res in self._run_itemwise_async(stage, context, iterable):
                yield res

    async def _run_itemwise_async(
        self, stage: "Stage", context: "Context", iterable: AsyncIterator[Any]
    ) -> AsyncIterator[Any]:
        """
        Processes items concurrently using asyncio.
        """
        async for item in iterable:
            try:
                if stage.is_async:
                    results = await stage._invoke(context, item)
                else:
                    # Run sync functions in a thread to avoid blocking the event loop
                    results = await asyncio.to_thread(stage._invoke, context, item)

                from ..core.utils import ensure_iterable
                output_stream = ensure_iterable(results)
                for res in output_stream:
                    yield res

            except Exception as e:
                # Proper error handling with policies would go here.
                # For now, we re-raise.
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
