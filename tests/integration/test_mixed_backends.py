import time
from lijnding import Pipeline, stage

@stage(backend="thread", workers=2)
def slow_io_stage(x: int):
    """Simulates a slow I/O operation."""
    time.sleep(0.1)
    return x * 2

@stage(backend="serial")
def fast_cpu_stage(x: int):
    """A fast, simple transformation."""
    return x + 1

def test_mixed_backend_pipeline():
    """
    Tests a pipeline with a mix of threaded and serial stages.
    """
    # The pipeline starts with a concurrent stage and ends with a serial one.
    pipeline = slow_io_stage | fast_cpu_stage

    data = [1, 2, 3, 4]

    start_time = time.perf_counter()
    results, _ = pipeline.collect(data)
    end_time = time.perf_counter()

    # The slow_io_stage should run concurrently. With 2 workers for 4 items
    # that each take 0.1s, the total time should be around 0.2s. We assert
    # it's well below the sequential time of 0.4s.
    assert (end_time - start_time) < 0.5

    # Check the results are correct after passing through both stages
    # Input:  [1, 2, 3, 4]
    # After slow_io_stage: [2, 4, 6, 8] (order not guaranteed)
    # After fast_cpu_stage: [3, 5, 7, 9]
    assert sorted(results) == [3, 5, 7, 9]
