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

        # Both producer and consumer have small buffers to make the
        # backpressure effect more pronounced and testable.
        @stage(backend=backend, workers=1, buffer_size=1)
        def producer(item):
            event_queue.put(f"produced_{item}")
            time.sleep(0.01) # Simulate a small amount of work
            return item

        @stage(backend=backend, workers=1, buffer_size=1)
        def consumer(item):
            time.sleep(0.2)
            event_queue.put(f"consumed_{item}")
            return item

        pipeline = producer | consumer

        result_queue = manager.Queue()
        def run_pipeline_in_thread():
            try:
                results, _ = pipeline.collect(range(5))
                result_queue.put(results)
            except Exception as e:
                result_queue.put(e)

        import threading
        pipeline_thread = threading.Thread(target=run_pipeline_in_thread)
        pipeline_thread.start()

        # Wait long enough for the first item to be consumed.
        time.sleep(0.3)

        events = []
        while not event_queue.empty():
            events.append(event_queue.get())

        # With buffer_size=1 on both stages, the total buffer between the
        # producer function and the consumer function is small.
        # After 0.3s:
        # - consumer has processed item 0 (0.2s)
        # - producer has produced items 0, 1, 2. Item 2 is in producer's
        #   output queue, item 1 is in consumer's input queue.
        # - producer should be blocked trying to put item 3 into its output queue.
        assert "consumed_0" in events
        assert "produced_0" in events
        assert "produced_1" in events
        assert "produced_2" in events
        assert "produced_3" not in events, f"Events were: {events}"

        # Let the pipeline finish and check final results
        pipeline_thread.join(timeout=2)

        final_result = result_queue.get()
        if isinstance(final_result, Exception):
            raise final_result # Re-raise exception from thread
        assert sorted(final_result) == [0, 1, 2, 3, 4]
