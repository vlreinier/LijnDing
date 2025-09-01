import pytest
from lijnding import Pipeline, stage, Context

def top_level_double(x):
    return x * 2

def top_level_context_user(context: Context, x: int):
    # This needs to be an atomic operation.
    # The `inc` method returns the new value.
    return x + context.inc("counter")

def test_processing_backend_top_level_func():
    """Tests a named, top-level function."""
    double_stage = stage(top_level_double, backend="process", workers=2)
    pipeline = Pipeline([double_stage])
    results, _ = pipeline.collect(range(10))
    assert sorted(results) == [i * 2 for i in range(10)]

def test_processing_backend_lambda():
    """Tests that a lambda function works with the process backend (requires dill)."""
    lambda_stage = stage(lambda x: x * 2, backend="process", workers=2)
    pipeline = Pipeline([lambda_stage])
    results, _ = pipeline.collect([5, 6, 7])
    assert sorted(results) == [10, 12, 14]

def test_processing_backend_with_context():
    """Tests that the shared context works across multiple processes."""
    context_stage = stage(top_level_context_user, backend="process", workers=2)
    pipeline = Pipeline([context_stage])
    results, context = pipeline.collect(range(5))

    assert context.get("counter") == 5
    assert sorted(results) == [1, 3, 5, 7, 9]
