import time
from lijnding import Pipeline, stage

def test_threading_backend_concurrency():
    @stage(backend="thread", workers=4)
    def slow_stage(x):
        time.sleep(0.1)
        return x

    pipeline = Pipeline([slow_stage])

    start_time = time.perf_counter()
    results, _ = pipeline.collect(range(4))
    end_time = time.perf_counter()

    assert sorted(results) == [0, 1, 2, 3]
    assert (end_time - start_time) < 0.2
