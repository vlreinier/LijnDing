import pytest
from lijnding.core import Pipeline, stage
from tests.helpers.test_runner import run_pipeline, BACKENDS


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_empty_input(backend):
    """Tests that the pipeline runs correctly with an empty list of input data."""
    s = stage(lambda x: x * 2, backend=backend)
    pipeline = Pipeline([s])
    results, _ = await run_pipeline(pipeline, [])
    assert results == []
