import pytest
import time
import multiprocessing as mp
from lijnding import Pipeline, stage

@pytest.mark.parametrize("backend", ["thread", "process"])
def test_buffer_size_limits_producer(backend):
    """
    Tests that a small `buffer_size` correctly applies backpressure to
    a fast producer when the consumer is slow.
    """
    with mp.Manager() as manager:
        event_queue = manager.Queue()

        @stage(backend=backend, workers=1)
        def producer(item):
            event_queue.put(f"produced_{item}")
            return item

        # The consumer is slow and has a small buffer
        @stage(backend=backend, workers=1, buffer_size=1)
        def consumer(item):
            time.sleep(0.2)
            event_queue.put(f"consumed_{item}")
            return item

        pipeline = producer | consumer

        # Run the pipeline in a separate thread so we can inspect the queue
        # while the pipeline is running.
        result_queue = manager.Queue()
        def run_pipeline_in_thread():
            results, _ = pipeline.collect(range(5))
            result_queue.put(results)

        import threading
        pipeline_thread = threading.Thread(target=run_pipeline_in_thread)
        pipeline_thread.start()

        # Wait long enough for the first item to be consumed, but not the second.
        time.sleep(0.3)

        events = []
        while not event_queue.empty():
            events.append(event_queue.get())

        # At this point, the consumer should have processed the first item (0).
        # The producer should have produced item 0 (consumed) and item 1 (in the buffer).
        # Because the buffer_size is 1, the producer should be blocked and
        # unable to produce item 2 yet.
        assert "consumed_0" in events
        assert "produced_0" in events
        assert "produced_1" in events
        assert "produced_2" not in events

        # Let the pipeline finish and check final results
        pipeline_thread.join(timeout=2)

        final_results = result_queue.get()
        assert sorted(final_results) == [0, 1, 2, 3, 4]
