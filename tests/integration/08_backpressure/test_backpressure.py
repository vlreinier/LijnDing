import pytest
import time
from lijnding.core import Pipeline, stage
from tests.helpers.test_runner import run_pipeline

# Backpressure is only relevant for concurrent backends.
CONCURRENT_BACKENDS = ["thread", "process", "async"]


def slow_consumer(x):
    time.sleep(0.02)
    return x


@pytest.mark.parametrize("backend", CONCURRENT_BACKENDS)
@pytest.mark.asyncio
async def test_backpressure(backend):
    """
    Tests that backpressure is applied correctly by ensuring that a fast
    producer is slowed down by a slow consumer.
    """
    num_items = 50
    data = range(num_items)

    # With a default queue size of 1, the producer should be forced to wait
    # for the slow consumer, making the total runtime proportional to the
    # consumer's processing time. We use 2 workers to ensure that we are
    # testing the concurrent backends properly.
    pipeline = Pipeline([stage(slow_consumer, backend=backend, workers=2)])

    start_time = time.time()
    results, _ = await run_pipeline(pipeline, data)
    end_time = time.time()

    duration = end_time - start_time

    # The theoretical minimum time is (num_items * sleep_time) / num_workers.
    # We assert that the actual duration is at least 80% of this theoretical
    # time, which proves that backpressure is working.
    min_expected_duration = (num_items * 0.02 / 2) * 0.8
    assert duration > min_expected_duration

    # And, of course, check that the results are correct.
    assert sorted(results) == list(data)
