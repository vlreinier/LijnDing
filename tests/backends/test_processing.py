import pytest
from lijnding import Pipeline, stage, Context

# --- Test Functions for Processing ---
# Must be registered to be found by the worker processes.

@stage(register_for_processing=True)
def _process_worker_func(x):
    return x * 2

@stage(register_for_processing=True)
def _process_context_func(context: Context, x):
    context.inc("proc_counter")
    return x

def _unregistered_func(x):
    return "should not run"

def test_processing_backend_execution():
    pipeline = Pipeline([_process_worker_func])
    results, _ = pipeline.collect([1, 2, 3, 4])
    assert sorted(results) == [2, 4, 6, 8]

def test_processing_backend_context():
    pipeline = Pipeline([_process_context_func])
    _, context = pipeline.collect([1, 2, 3, 4])
    assert context.get("proc_counter") == 4

def test_unregistered_stage_fails():
    """
    Tests that the pre-flight check correctly raises a TypeError
    for a stage that was not registered.
    """
    unregistered_stage = stage(backend="process")(_unregistered_func)
    pipeline = Pipeline([unregistered_stage])

    with pytest.raises(TypeError, match="is not registered for multiprocessing"):
        pipeline.collect([1, 2, 3])
