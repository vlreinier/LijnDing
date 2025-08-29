from lijnding import stage
from lijnding.components.branch import branch

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

    pipeline_zip = stage(lambda x: x) | branch(multi_yield, exclaim, merge="zip")
    results_zip, _ = pipeline_zip.collect(["a", "b"])
    assert results_zip == [("a", "a!"), ("b", "b!")]

    pipeline_zip_longest = stage(lambda x: x) | branch(multi_yield, exclaim, merge="zip_longest")
    results_zip_longest, _ = pipeline_zip_longest.collect(["a"])
    assert results_zip_longest == [("a", "a!"), ("a", None)]
