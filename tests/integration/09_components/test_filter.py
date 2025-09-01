import pytest
from lijnding import Pipeline
from lijnding.components import filter_
from tests.helpers.test_runner import run_pipeline, BACKENDS

def is_even(n):
    return n % 2 == 0

@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_filter_component(backend):
    """
    Tests the filter_ component with different backends.
    """
    # The filter component creates a stage, so we pass the backend to it
    pipeline: Pipeline = filter_(is_even, backend=backend)

    data = list(range(10))
    results, _ = await run_pipeline(pipeline, data)

    expected = [0, 2, 4, 6, 8]
    # The processing backend might not preserve order, so we sort
    assert sorted(results) == expected

def test_filter_empty_input():
    """
    Tests that the filter_ component handles empty input correctly.
    """
    pipeline = filter_(is_even)
    results, _ = pipeline.collect([])
    assert results == []

def test_filter_no_matches():
    """
    Tests that the filter_ component works correctly when no items match.
    """
    pipeline = filter_(lambda x: x > 10)
    results, _ = pipeline.collect(list(range(5)))
    assert results == []
