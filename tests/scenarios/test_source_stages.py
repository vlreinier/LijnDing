import pytest
from lijnding import Pipeline, stage

# --- Source Stage Tests ---

@stage
def simple_source():
    """A simple source stage that yields a few numbers."""
    yield from range(3)

@stage(register_for_processing=True)
def simple_source_proc():
    """A source stage registered for multiprocessing."""
    yield from range(3)

@stage
def add_one(x: int) -> int:
    return x + 1

@stage(register_for_processing=True)
def add_one_proc(x: int) -> int:
    return x + 1

def test_simple_source_pipeline():
    """Tests a pipeline that starts with a source stage."""
    pipeline = simple_source | add_one
    results, _ = pipeline.collect([]) # Input data is ignored
    assert results == [1, 2, 3]

def test_inline_source_pipeline():
    """Tests a source stage that appears in the middle of a pipeline."""
    pipeline = add_one | simple_source | add_one
    # The output of the first add_one (2, 3, 4) is discarded.
    # The source stage generates [0, 1, 2].
    # The final add_one operates on the source's output.
    results, _ = pipeline.collect([1, 2, 3])
    assert results == [1, 2, 3]

def test_source_validation():
    """Tests that chaining two source stages raises a TypeError."""
    with pytest.raises(TypeError, match="Cannot pipe from one 'source' stage to another."):
        _ = simple_source | simple_source

def test_source_with_processing_backend():
    """Tests a source stage with the multiprocessing backend."""
    pipeline = simple_source_proc | add_one_proc
    results, _ = pipeline.collect([])
    # Note: The add_one_proc is not actually run in a separate process
    # because the simple_source_proc is not. This is a limitation of the
    # current runner design, but the pipeline should still work serially.
    # A fully parallel implementation would require more complex runners.
    assert sorted(results) == [1, 2, 3]
