import pytest
from lijnding import Pipeline, stage, Context

# --- Processing Backend Tests ---

def test_processing_backend_simple():
    """Tests a simple function with the process backend."""
    # This top-level function is picklable by default
    @stage(backend="process")
    def double(x):
        return x * 2

    pipeline = Pipeline([double])
    results, _ = pipeline.collect([1, 10, 100])
    assert sorted(results) == [2, 20, 200]

def test_processing_backend_lambda():
    """Tests that a lambda function works with the process backend (requires dill)."""
    double_lambda = stage(lambda x: x * 2, backend="process")

    pipeline = Pipeline([double_lambda])
    results, _ = pipeline.collect([5, 6, 7])
    assert sorted(results) == [10, 12, 14]

def test_processing_backend_with_context():
    """Tests that the shared context works with the process backend."""
    @stage(backend="process")
    def context_user(context: Context, item: int):
        context.inc("my_counter")
        return item + context.get("my_counter")

    pipeline = Pipeline([context_user])
    # The context is created by the pipeline and should be mp-safe
    results, context = pipeline.collect([10, 20, 30])

    assert context.get("my_counter") == 3
    # The results will be 10+1, 20+2, 30+3
    assert sorted(results) == [11, 22, 33]
