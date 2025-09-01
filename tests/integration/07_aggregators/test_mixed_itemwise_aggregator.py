import pytest
from lijnding import Pipeline, stage
from ...helpers.test_runner import run_pipeline, BACKENDS


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_mixed_pipeline_itemwise_to_aggregator(backend):
    """Tests a pipeline with an itemwise stage followed by an aggregator."""
    double = stage(lambda x: x * 2, backend=backend)
    summer = stage(sum, stage_type="aggregator", backend=backend)

    pipeline = Pipeline([double, summer])
    data = [1, 2, 3]  # -> [2, 4, 6]
    results, _ = await run_pipeline(pipeline, data)
    assert results == [12]
