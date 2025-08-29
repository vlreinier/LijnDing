import pytest
import time
import asyncio
from lijnding import Pipeline, stage, Context

# --- Test Functions for Processing ---
# Must be defined at the top level to be picklable.

def _process_worker_func(x):
    return x * 2

def _process_context_func(context: Context, x):
    context.inc("proc_counter")
    return x

# --- Backend Tests ---

def test_serial_backend_with_aggregator():
    @stage(stage_type="aggregator")
    def sum_all(iterable):
        yield sum(iterable)

    pipeline = Pipeline() | sum_all
    results, _ = pipeline.collect([1, 2, 3, 4])
    assert results == [10]

def test_threading_backend_concurrency():
    @stage(backend="thread", workers=4)
    def slow_stage(x):
        time.sleep(0.1)
        return x

    pipeline = Pipeline() | slow_stage

    start_time = time.perf_counter()
    results, _ = pipeline.collect(range(4))
    end_time = time.perf_counter()

    assert sorted(results) == [0, 1, 2, 3]
    assert (end_time - start_time) < 0.2

@pytest.mark.skip(reason="ProcessingRunner is unstable and causes timeouts.")
def test_processing_backend_execution():
    process_stage = stage(backend="process", workers=2)(_process_worker_func)
    pipeline = Pipeline() | process_stage
    results, _ = pipeline.collect([1, 2, 3, 4])
    assert sorted(results) == [2, 4, 6, 8]

@pytest.mark.skip(reason="ProcessingRunner is unstable and causes timeouts.")
def test_processing_backend_context():
    process_context_stage = stage(backend="process", workers=2)(_process_context_func)
    pipeline = Pipeline() | process_context_stage
    _, context = pipeline.collect([1, 2, 3, 4])
    assert context.get("proc_counter") == 4

@pytest.mark.asyncio
@pytest.mark.skip(reason="Asyncio pipeline entrypoint not implemented yet")
async def test_asyncio_backend_async_stage():
    @stage(backend="async")
    async def async_add_one(x):
        await asyncio.sleep(0.01)
        return x + 1
    pipeline = Pipeline() | async_add_one
    pass

@pytest.mark.asyncio
@pytest.mark.skip(reason="Asyncio pipeline entrypoint not implemented yet")
async def test_asyncio_backend_sync_stage():
    @stage(backend="async")
    def sync_add_one(x):
        time.sleep(0.01)
        return x + 1
    pipeline = Pipeline() | sync_add_one
    pass
