import pytest
import time
from lijnding.core import stage


class CountingIterator:
    """An iterator that counts how many items have been requested."""

    def __init__(self, n):
        self.n = n
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.count >= self.n:
            raise StopIteration

        self.count += 1
        # A small sleep to ensure the pipeline runner has time to switch threads
        time.sleep(0.01)
        return self.count - 1


@pytest.mark.parametrize("backend", ["thread", "process"])
def test_buffer_size_applies_backpressure(backend):
    """
    Tests that `buffer_size` correctly applies backpressure by checking
    how far ahead the pipeline reads from a source iterator.
    """
    # This iterator will be the source of the pipeline
    data_source = CountingIterator(10)

    # This stage is slow, so it will cause items to build up.
    # With buffer_size=1, the runner should not pull more than a couple
    # of items ahead from the data_source.
    @stage(backend=backend, workers=1, buffer_size=1)
    def slow_consumer(item):
        time.sleep(0.1)
        return item

    pipeline = slow_consumer

    # We don't use .collect() because we want to control the iteration.
    output_stream, _ = pipeline.run(data_source)

    # Pull the first item from the output. This will start the pipeline.
    first_result = next(output_stream)
    assert first_result == 0

    # Give the feeder thread a moment to run ahead if it's able to.
    time.sleep(0.05)

    # Check how many items have been pulled from the source iterator.
    # The feeder thread in the runner will pull items to fill its buffer.
    # With workers=1 and buffer_size=1:
    # - 1 item is being processed by the worker.
    # - 1 item is in the input queue (the buffer).
    # The feeder thread is one item ahead of the queue. So the total
    # buffer is worker (1) + q_in (1) + feeder (1) = 3.
    # The feeder will have pulled the 4th item and be blocked on put().
    assert data_source.count <= 4

    # Consume the rest of the stream to let the pipeline finish cleanly.
    rest_results = list(output_stream)
    assert rest_results == list(range(1, 10))
    # After full consumption, the count should be exactly 10.
    assert data_source.count == 10
