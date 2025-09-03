import pytest
from lijnding.core import Pipeline, stage, aggregator_stage
from tests.helpers.test_runner import run_pipeline, BACKENDS


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_mixed_pipeline_itemwise_to_aggregator(backend):
    """Tests a pipeline with an itemwise stage followed by an aggregator."""
    double = stage(lambda x: x * 2, backend=backend)
    summer = aggregator_stage(sum, backend=backend)

    pipeline = Pipeline([double, summer])
    data = [1, 2, 3]  # -> [2, 4, 6]
    results, _ = await run_pipeline(pipeline, data)
    assert results == [12]
