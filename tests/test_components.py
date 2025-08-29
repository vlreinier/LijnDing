import pytest
from lijnding import Pipeline, stage
from lijnding.components.batch import batch
from lijnding.components.reduce import reduce
from lijnding.components.split import split
from lijnding.components.map import map_values
from lijnding.components.branch import branch

# --- Component Tests ---

def test_map_values_component():
    pipeline = Pipeline() | map_values(lambda x: x * 2)
    results, _ = pipeline.collect([1, 2, 3])
    assert results == [2, 4, 6]

def test_split_component():
    pipeline = Pipeline() | split()
    results, _ = pipeline.collect([[1, 2], [3], [4, 5, 6]])
    assert results == [1, 2, 3, 4, 5, 6]

def test_batch_component():
    # Test with a perfect fit
    pipeline = Pipeline() | batch(size=2)
    results, _ = pipeline.collect([1, 2, 3, 4])
    assert results == [[1, 2], [3, 4]]

    # Test with a partial last batch
    pipeline_partial = Pipeline() | batch(size=3)
    results_partial, _ = pipeline_partial.collect([1, 2, 3, 4, 5])
    assert results_partial == [[1, 2, 3], [4, 5]]

def test_reduce_component():
    # With initializer
    pipeline = Pipeline([reduce(lambda a, b: a + b, 100)])
    results, _ = pipeline.collect([1, 2, 3])
    assert results == [106]

    # Without initializer
    pipeline_no_init = Pipeline([reduce(lambda a, b: a + b)])
    results_no_init, _ = pipeline_no_init.collect([1, 2, 3])
    assert results_no_init == [6]

    # Empty input with no initializer
    pipeline_empty, _ = pipeline_no_init.collect([])
    assert pipeline_empty == []

# --- Branch Tests ---

@stage
def to_upper(x: str) -> str:
    return x.upper()

@stage
def to_lower(x: str) -> str:
    return x.lower()

@stage
def exclaim(x: str) -> str:
    return f"{x}!"

def test_branch_concat():
    # A branch must be part of a pipeline, it cannot be the start.
    # We use a simple identity stage to start the pipeline.
    pipeline = stage(lambda x: x) | branch(to_upper, to_lower)
    results, _ = pipeline.collect(["Hello", "World"])
    assert results == ["HELLO", "hello", "WORLD", "world"]

def test_branch_zip():
    pipeline = stage(lambda x: x) | branch(to_upper, exclaim, merge="zip")
    results, _ = pipeline.collect(["a", "b"])
    assert results == [("A", "a!"), ("B", "b!")]

def test_branch_with_uneven_outputs():
    @stage
    def multi_yield(x):
        yield x
        yield x

    # zip should stop when the shorter branch (exclaim) is exhausted
    pipeline_zip = stage(lambda x: x) | branch(multi_yield, exclaim, merge="zip")
    results_zip, _ = pipeline_zip.collect(["a", "b"])
    assert results_zip == [("a", "a!"), ("b", "b!")]

    # zip_longest should continue, filling with None
    pipeline_zip_longest = stage(lambda x: x) | branch(multi_yield, exclaim, merge="zip_longest")
    results_zip_longest, _ = pipeline_zip_longest.collect(["a"])
    assert results_zip_longest == [("a", "a!"), ("a", None)]
