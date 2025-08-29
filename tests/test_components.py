import pytest
from lijnding import Pipeline, stage
from lijnding.components.batch import batch
from lijnding.components.reduce import reduce_values
from lijnding.components.split import split
from lijnding.components.map import map_values
from lijnding.components.branch import Branch

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

def test_reduce_values_component():
    # With initializer
    pipeline = Pipeline() | reduce_values(lambda a, b: a + b, 100)
    results, _ = pipeline.collect([1, 2, 3])
    assert results == [106]

    # Without initializer
    pipeline_no_init = Pipeline() | reduce_values(lambda a, b: a + b)
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
    branch_pipeline = Branch(to_upper, to_lower).to_stage()
    pipeline = Pipeline() | branch_pipeline
    results, _ = pipeline.collect(["Hello", "World"])
    assert results == ["HELLO", "hello", "WORLD", "world"]

def test_branch_zip():
    branch_pipeline = Branch(to_upper, exclaim, merge="zip").to_stage()
    pipeline = Pipeline() | branch_pipeline
    results, _ = pipeline.collect(["a", "b"])
    assert results == [("A", "a!"), ("B", "b!")]

def test_branch_with_uneven_outputs():
    @stage
    def multi_yield(x):
        yield x
        yield x

    # zip should stop when the shorter branch (exclaim) is exhausted
    branch_zip = Branch(multi_yield, exclaim, merge="zip").to_stage()
    pipeline_zip = Pipeline() | branch_zip
    results_zip, _ = pipeline_zip.collect(["a", "b"])
    assert results_zip == [( "a", "a!"), ("b", "b!")]

    # zip_longest should continue, filling with None
    branch_zip_longest = Branch(multi_yield, exclaim, merge="zip_longest").to_stage()
    pipeline_zip_longest = Pipeline() | branch_zip_longest
    results_zip_longest, _ = pipeline_zip_longest.collect(["a"])
    assert results_zip_longest == [("a", "a!"), ("a", None)]
