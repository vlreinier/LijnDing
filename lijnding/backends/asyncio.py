from __future__ import annotations

import asyncio
import inspect
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
            # The aggregator function itself might be sync or async.
            is_async_func = inspect.iscoroutinefunction(stage.func)

            if is_async_func:
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
        is_async_func = inspect.iscoroutinefunction(stage.func)

        async for item in iterable:
            try:
                if is_async_func:
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
        Provides a synchronous entry point for the async runner.
        This is useful for running an async-powered pipeline from a sync context.

        It automatically detects a running event loop. If one is found, it
        will schedule the async processing on that loop. Otherwise, it will
        create a new event loop.

        .. warning::
            This method is NOT lazy and will block until the entire async
            iterator is consumed. For truly non-blocking, asynchronous
            execution, use `run_async` from an async context.
        """
        # We need an async generator to pass to `asyncio.run` or `loop.run_until_complete`.
        async def _async_gen_wrapper():
            # Convert the sync iterable to an async one
            async def _to_async(it):
                for i in it:
                    yield i

            async for res in self.run_async(stage, context, _to_async(iterable)):
                yield res

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, so we can create one.
            results = asyncio.run(_collect_async_gen(_async_gen_wrapper()))
            yield from results
            return

        # If a loop is running, we can't use asyncio.run().
        # We must schedule the task and wait for it to complete.
        # This is a synchronous bridge.
        # Using loop.run_until_complete in this context is tricky, as it
        # can block other tasks in the loop. A truly robust solution
        # would involve a more complex async-to-sync pattern, but for the
        # sake of this library's scope, we will use a pragmatic approach
        # that works for the common case of running a sync-style pipeline
        # that happens to have an async stage inside a test or a REPL
        # that already has a running loop.
        # Note: This may not be suitable for all production scenarios.
        task = loop.create_task(_collect_async_gen(_async_gen_wrapper()))

        # A simple way to wait for the task to be done from a sync context
        # without blocking the entire loop indefinitely is to yield control
        # periodically. However, this `run` method is expected to be sync.
        # So we must block.
        # `run_until_complete` is the tool for this, despite its caveats.
        results = loop.run_until_complete(task)
        yield from results

    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """Implementation for the abstract method, uses the sync bridge."""
        yield from self.run(stage, context, iterable)


async def _collect_async_gen(agen):
    return [item async for item in agen]
