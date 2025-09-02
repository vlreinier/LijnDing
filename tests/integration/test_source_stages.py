import pytest
from lijnding import Pipeline, stage

# --- Source Stage Tests ---

from typing import Generator

@stage
def simple_source() -> Generator[int, None, None]:
    """A simple source stage that yields a few numbers."""
    yield from range(3)

@stage
def add_one(x: int) -> int:
    return x + 1

def test_simple_source_pipeline():
    """Tests a pipeline that starts with a source stage."""
    pipeline = simple_source | add_one
    results, _ = pipeline.collect([]) # Input data is ignored
    assert results == [1, 2, 3]

def test_inline_source_pipeline():
    """Tests a source stage that appears in the middle of a pipeline."""
    pipeline = add_one | simple_source | add_one
    results, _ = pipeline.collect([1, 2, 3])
    assert results == [1, 2, 3]

def test_source_validation():
    """Tests that chaining two source stages raises a TypeError."""
    with pytest.raises(TypeError, match="Cannot pipe from one 'source' stage to another."):
        _ = simple_source | simple_source

