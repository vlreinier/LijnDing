import pytest
import time
import asyncio
from lijnding import Pipeline, stage

@pytest.mark.asyncio
async def test_asyncio_backend_async_stage():
    @stage(backend="async")
    async def async_add_one(x):
        await asyncio.sleep(0.01)
        return x + 1

    pipeline = Pipeline([async_add_one])

    stream, _ = await pipeline.run_async(range(3))
    results = [i async for i in stream]

    assert results == [1, 2, 3]

@pytest.mark.asyncio
async def test_asyncio_backend_sync_stage():
    @stage(backend="async")
    def sync_add_one(x):
        # A sync function that does a bit of work
        time.sleep(0.01)
        return x + 1

    pipeline = Pipeline([sync_add_one])

    stream, _ = await pipeline.run_async(range(3))
    results = [i async for i in stream]

    assert results == [1, 2, 3]

@pytest.mark.asyncio
async def test_mixed_async_and_sync_pipeline():
    @stage(backend="async")
    async def async_double(x):
        return x * 2

    @stage(backend="serial") # This will run in the sync bridge
    def sync_add_ten(x):
        return x + 10

    pipeline = async_double | sync_add_ten

    stream, _ = await pipeline.run_async(range(3))
    results = [i async for i in stream]

    # Input: 0, 1, 2
    # After async_double: 0, 2, 4
    # After sync_add_ten: 10, 12, 14
    assert results == [10, 12, 14]
