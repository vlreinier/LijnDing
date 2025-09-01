import pytest
from lijnding import Pipeline, stage
from tests.utils.test_runner import run_pipeline, BACKENDS


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_basic_aggregator(backend):
    """Tests a simple aggregator stage."""
    sum_stage = stage(sum, stage_type="aggregator", backend=backend)
    pipeline = Pipeline([sum_stage])
    data = [1, 2, 3, 4, 5]
    results, _ = await run_pipeline(pipeline, data)
    assert results == [15]
