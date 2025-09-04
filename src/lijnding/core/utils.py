import asyncio
import queue
import types
from typing import Any, AsyncIterator, Iterable


SENTINEL = object()


class AsyncToSyncIterator:
    """
    Wraps an async iterator into a sync iterator, allowing it to be used
    in a synchronous context (like a for loop). This is useful for bridging
    async and sync parts of a pipeline.

    It works by running the async iterator in a background thread's event
    loop and passing items to the main thread through a thread-safe queue.
    """

    def __init__(
        self, async_iterator: AsyncIterator[Any], loop: asyncio.AbstractEventLoop
    ):
        self._async_iterator = async_iterator
        self._loop = loop
        self._queue: queue.Queue = queue.Queue(maxsize=1)
        self._feeder_task = self._loop.create_task(self._feeder())

    async def _feeder(self):
        """
        Asynchronously consumes items from the async iterator and puts them
        in the queue.
        """
        try:
            async for item in self._async_iterator:
                self._queue.put(item)
        except Exception as e:
            self._queue.put(e)
        finally:
            self._queue.put(SENTINEL)

    def __iter__(self):
        return self

    def __next__(self):
        item = self._queue.get()
        if item is SENTINEL:
            raise StopIteration
        if isinstance(item, Exception):
            # We re-raise the exception in the main thread to ensure
            # proper error propagation.
            raise item
        return item


def ensure_iterable(obj: Any) -> Iterable[Any]:
    """
    Ensures that the given object is an iterable.
    If it's a list or a generator, it's returned as is.
    If it's another type (including a tuple), it's wrapped in a list.
    `None` is treated as an empty list.
    """
    if obj is None:
        return []
    # Pass through lists and all kinds of generators
    if isinstance(obj, (list, types.GeneratorType, types.AsyncGeneratorType)):
        return obj
    # Wrap other types (including tuples) in a list to treat them as a single item
    return [obj]
